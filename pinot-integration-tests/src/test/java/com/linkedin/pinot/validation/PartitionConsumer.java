/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.validation;

import java.io.IOException;
import java.util.Random;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.clearspring.analytics.util.Preconditions;


public class PartitionConsumer implements Runnable {
  public static Logger LOGGER = LoggerFactory.getLogger(PartitionConsumer.class);
  public enum State {
    INIT_CONSUMING,
    AWAITING_RESP,
    CATCHING_UP,
    HOLDING,
    COMMITTING,
    BUILDING_SEGMENT,
    COMMITTED,
    DISCARDED,
    RETAINED;

    public static boolean shouldConsume(State state) {
      if (state.equals(INIT_CONSUMING) || (state.equals(CATCHING_UP))) {
        return true;
      }
      return false;
    }
    public static boolean isFinal(State state) {
      if (state.equals(COMMITTED) || state.equals(RETAINED) || state.equals(DISCARDED)) {
        return true;
      }
      return false;
    }
  }
  private final String _topic;
  private final long _startOffset;
  private final int _partitionId;
  private Thread _consumerThread;
  private final Random _random = new Random();
  private final String _segmentName;
  private long _maxHoldTimeMs = ValidationConstants.MAX_HOLD_TIME_MS; // Wait time while in hold state
  private long _slpTimeCntrlFailMs = 5000L; // Wait time when a controller transaction fails (it takes helix 30s to get a new controller)
  private long _slpTimeCnrlNotLeaderMs = 5000L;
  private long _slpRetrySendMs = 1000L;

  private volatile long _currentOffset = 5688;
  private volatile boolean _isStopped = false;
  private volatile long _targetOffset = -1L;
  private volatile State _state;

  private static final long _avgSleepTimeMsNormal = 10L;

  private long _avgSleepTimeMs = _avgSleepTimeMsNormal;
  private long _startTime = -1;
  private final long _consumeTimeMs;
  // Assume it takes half as much time to commit a segment as it does to build it.
  private final long _segmentBuildTimeMaxMs = ValidationConstants.MAX_SEGMENT_COMMIT_TIME_MS/2;
  private final String _instanceId;

  public PartitionConsumer(RealtimeSegmentNameHolder holder, String topic, String instanceId) {
    LOGGER = LoggerFactory.getLogger(PartitionConsumer.class.getName() + "_" + holder.getName());
    _topic = topic;
    _startOffset = holder.getStartOffset();
    _partitionId = holder.getKafkaPartition();
    _segmentName = holder.getName();
    _consumeTimeMs = ValidationConstants.MIN_CONSUME_TIME_MS + (_random.nextLong() % ValidationConstants.CONSUME_TIME_VARIATION_MS);
    _currentOffset = _startOffset;
    _state = State.INIT_CONSUMING;
    _instanceId = instanceId;
    int r = _random.nextInt(100);
    if (r < 20) {
      // 20% chance of a slow consumer, 33% slower than normal. Sleep more, consume for more time.
      _avgSleepTimeMs = _avgSleepTimeMs/3 + _avgSleepTimeMs;
    } else if (r < 40) {
      // 20% chance that this is a very fast consumer.
      _avgSleepTimeMs = _avgSleepTimeMs - _avgSleepTimeMs/3;
    }
  }

  private void makeThread() {
    _consumerThread = new Thread(this, "consumer" + "_" + _topic + "_" + _partitionId + "_" + _startOffset);
    LOGGER.info("Created consumer thread {} for {}", _consumerThread.getName(), this.toString());
  }

  private boolean endCriteriaReached() {
    if (_state == State.INIT_CONSUMING) {
      return (_targetOffset == _currentOffset || System.currentTimeMillis() >= _startTime + _consumeTimeMs);
    } else if (_state == State.CATCHING_UP) {
      return (_targetOffset == _currentOffset);
    } else {
      throw new RuntimeException("Wrong call");
    }
  }

  // Consume until end criteria
  private void consumeLoop() throws InterruptedException {
    if (State.shouldConsume(_state)) {
      while (!endCriteriaReached() && !_isStopped) {
        consumeRow();
      }
      LOGGER.info("Consumption stopped for {}", this.toString());
    }
  }

  private void consumeRow() throws InterruptedException {
    long sleepTime = _avgSleepTimeMsNormal /2 + _random.nextInt((int) _avgSleepTimeMsNormal /2);
    Thread.sleep(sleepTime);
    _currentOffset++;
  }

  public void run() {
    LOGGER.info("Starting consumer thread for {}", this.toString());
    _startTime = System.currentTimeMillis();
    try {
      while (!State.isFinal(_state)) {
        consumeLoop();
        if (_isStopped) {
          break;
        }
        _state = State.AWAITING_RESP;
        SegmentFinalProtocol.Response response = consumedSegment();
        SegmentFinalProtocol.ControllerResponseStatus status = response.getStatus();
        switch (status) {
          case CATCHUP:
            _state = State.CATCHING_UP;
            assert(response.getOffset() > _currentOffset);
            _targetOffset = response.getOffset();
            LOGGER.info("Catching up to offset {} from {}", _targetOffset, _currentOffset);
            break;
          case COMMIT:
            boolean success = commitSegment();
            if (success) {
              LOGGER.info("Segment Committed {}", this.toString());
            } else {
              LOGGER.info("Commit failed state={}, offset={}", _state, _currentOffset);
            }
            break;
          case KEEP:
            String tmpDir = buildSegment();
            commitSegmentToLocalDir(tmpDir, State.RETAINED);
            break;
          case HOLD:
            hold();
            break;
          case DISCARD:
            discard();
            break;
          default:
            LOGGER.error("Invalid response {} from controller {}", response.toJsonString(), this.toString());
            throw new RuntimeException("Invalid response from controller " + response.toJsonString());
        }
        LOGGER.info("End of loop {}", this.toString());
      }
      LOGGER.info("Exiting loop {}", this.toString());
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted consumer thread {}", this.toString());
      throw new RuntimeException("Consumer thread interrupted", e);
    }
    LOGGER.info("Stopping thread {}", this.toString());
  }

  private void discard() {
    // Keep the segment in memory, until the ONLINE transition comes along, and the replace it.
    LOGGER.info("Discarding segment {}", this.toString());
    _state = State.DISCARDED;
  }

  private void hold() {
    _state = State.HOLDING;
    LOGGER.info("Holding segment {}", this.toString());
    try {
      Thread.sleep(_maxHoldTimeMs);
    } catch (InterruptedException e) {

    }
    _state = State.AWAITING_RESP;
  }

  private String buildSegment() throws InterruptedException {
    // Build and save segment locally in a temporary directory.
    LOGGER.info("Building segment {}", this.toString());
    _state = State.BUILDING_SEGMENT;
    int sleepTime = _random.nextInt((int) _segmentBuildTimeMaxMs);
    Thread.sleep(sleepTime);
    LOGGER.info("Finished building segment {}", this.toString());
    return "tmp";
  }

  private void commitSegmentToLocalDir(String tmpDir, State nextState) {
    // Move segment from tmpDir to final area.
    LOGGER.info("Committing to local directory {}, nextState={}", this.toString(), nextState);
    _state = nextState;
  }

  /*
   * Public invocations from outside the class.
   */

  public String toString() {
    return "{" + _segmentName + "," + _state + "," + _currentOffset + "}";
  }

  public void abort() throws InterruptedException {
    // For now, stop.
    stop();
  }

  public void consume() {
    makeThread();
    _consumerThread.start();
  }

  public long stop() throws InterruptedException {
    Preconditions.checkState(!_isStopped);
    _isStopped = true;
    _consumerThread.join();
    if (_consumerThread.isAlive()) {
      LOGGER.error("Could not stop consumer thread {}", this.toString());
      throw new RuntimeException("Could not stop consumer thread");
    }
    return _currentOffset;
  }

  public long catchup(long targetOffset, long timeoutMs) throws InterruptedException {
    Preconditions.checkState(_isStopped);
    _isStopped = false;
    _targetOffset = targetOffset;
    _state = State.CATCHING_UP;
    makeThread();
    _consumerThread.start();
    _consumerThread.join(timeoutMs);
    if (_consumerThread.isAlive()) {
      // The consumer took too long to get to the catchup point. We already have a segment
      // in the controller, so we will just download and continue from there.
      stop();
    }
    return _currentOffset;
  }

  public boolean waitForCommitted(long millis) {
    if (_state == State.COMMITTED) {
      return true;
    } else if (_state == State.COMMITTING) {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException e) {

      }
      if (_state == State.COMMITTED) {
        return true;
      }
    }
    return false;
  }

  /*
   * Message interaction with the controller.
   */
  private SegmentFinalProtocol.Response consumedSegment() throws InterruptedException {
    return getResponseWithRery(
        new SegmentFinalProtocol.SegmentConsumedRequest(_segmentName, _currentOffset, _instanceId));
  }

  private boolean commitSegment() throws  InterruptedException {
    if (_random.nextInt(100) < 30) {
      // 30% chance that the committer never gets back to the controller
      LOGGER.info("======= Failing to commit segment {}", _segmentName);
      System.exit(1);
    }
    String tmpDir = buildSegment();
    _state = State.COMMITTING;
    LOGGER.info("Attempting to commit segment {}", this.toString());
    SegmentFinalProtocol.Request request = new SegmentFinalProtocol.SegmentCommitRequest(
        _segmentName, _currentOffset, _instanceId);
    // If the commit fails, we don't retry. Instead, we go to HOLDING state, and retry there.
    SegmentFinalProtocol.Response response =  getResponse(request);
    if (!response.getStatus().equals(SegmentFinalProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      LOGGER.warn("Commit did not succeed resp {} for {}", response.toJsonString(), this.toString());
      _state =  State.HOLDING;
      return false;
    }
    commitSegmentToLocalDir(tmpDir, State.COMMITTED);
    return true;
  }

  private SegmentFinalProtocol.Response getResponseWithRery(SegmentFinalProtocol.Request request)
    throws InterruptedException {
    // TODO Add max retry count here.
    while (true) {
      SegmentFinalProtocol.Response response = getResponse(request);
      switch (response.getStatus()) {
        case NOT_LEADER:
          Thread.sleep(_slpTimeCnrlNotLeaderMs);
          ControllerLeaderLocator.getInstance().getControllerLeader(true);
        case FAILED:
          Thread.sleep(_slpTimeCntrlFailMs);
          break;
        case NOT_SENT:
          Thread.sleep(_slpRetrySendMs);
          break;
        default:
          return response;
      }
    }
  }

  private SegmentFinalProtocol.Response getResponse(SegmentFinalProtocol.Request request) {
    SegmentFinalProtocol.Response response =
        new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.NOT_SENT, -1L);
    HttpClient httpClient = new HttpClient();
    ControllerLeaderLocator leaderLocator = ControllerLeaderLocator.getInstance();
    final String leaderAddress = leaderLocator.getControllerLeader(false);
    if (leaderAddress == null) {
      LOGGER.error("No leader found {}", this.toString());
      return new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.NOT_LEADER, -1L);
    }
    final String url = request.getUrl(leaderAddress);
    GetMethod getMethod = new GetMethod(url);
    LOGGER.info("Sending request {} for {}", url, this.toString());
    try {
      int responseCode = httpClient.executeMethod(getMethod);
      if (responseCode >= 300) {
        LOGGER.error("Bad controller response code {} for {}", responseCode, this.toString());
        return response;
      } else {
        response = new SegmentFinalProtocol.Response(getMethod.getResponseBodyAsString());
        LOGGER.info("Controller response {} for {}", response.toJsonString(), this.toString());
        return response;
      }
    } catch (IOException e) {
      LOGGER.error("IOException {}", this.toString(), e);
      ControllerLeaderLocator.getInstance().getControllerLeader(true);
      return response;
    }
  }
}
