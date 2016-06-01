/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.utils.CommonConstants;


// Singleton class
public class SegmentFinalizer {
  // TODO Can we log using the segment name in the log message?
  public static Logger LOGGER = LoggerFactory.getLogger(SegmentFinalizer.class);
  private enum State {
    HOLDING,          // the segment has started finalizing.
    COMMITTER_DECIDED,
    COMMITTER_NOTIFIED, // we notified the committer to commit.
    COMMITTER_UPLOADING,  // committer is uploading.
    COMMITTING, // we are in the process of committing to zk
    COMMITTED,    // We already committed a segment.
    ABORTED,
  }

  private static SegmentFinalizer _instance = null;

  private final HelixManager _helixManager;
  // A map that holds the FSM for each segment.
  private final Map<String, SegmentFinalizerFSM> _fsmMap = new ConcurrentHashMap<>();
  private final LLRealtimeSegmentManager _segmentManager;

  // TODO keep some history of past committed segments so that we can avoid looking up PROPERTYSTORE if some server comes in late.

  private SegmentFinalizer(HelixManager helixManager, LLRealtimeSegmentManager segmentManager) {
    _helixManager = helixManager;
    _segmentManager = segmentManager;
  }

  public static SegmentFinalizer create(HelixManager helixManager, LLRealtimeSegmentManager segmentManager) {
    if (_instance != null) {
      throw new RuntimeException("Cannot create multiple instances");
    }
    _instance = new SegmentFinalizer(helixManager, segmentManager);
    return _instance;
  }

  public static SegmentFinalizer getInstance() {
    if (_instance == null) {
      throw new RuntimeException("Not yet created");
    }
    return _instance;
  }

  // HACK: Returns an FSM to which we can apply the incoming event, or a response that we can return.
  // We need to return both of these to the caller to avoid race conditions where a caller can
  // TODO find a better way to do this.
  private synchronized SegmentFinalizerFSM lookupOrCreateFsm(final RealtimeSegmentNameHolder holder, String msgType,
      long offset) {
    final String segmentName = holder.getName();
    SegmentFinalizerFSM fsm = _fsmMap.get(segmentName);
    if (fsm == null) {
      // Look up propertystore to see if this is a completed segment
      ZNRecord segment;
      try {
        // TODO if we keep a list of last few committed segments, we don't need to go to zk for this.
        LLRealtimeSegmentZKMetadata segmentMetadata = _segmentManager.getRealtimeSegmentZKMetadata(holder.getTableName(), holder.getName());
        if (segmentMetadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.DONE)) {
          // Best to go through the state machine for this case as well, so that all code regarding state handling is in one place
          // Also good for synchronization, because it is possible that multiple threads take this path, and we don't want
          // multiple instances of the FSM to be created for the same commit sequence at the same time.
          final long endOffset = segmentMetadata.getEndOffset();
          fsm = new SegmentFinalizerFSM(_segmentManager, holder, segmentMetadata.getNumReplicas(), endOffset);
        } else {
          // Segment is finalizing, and this is the first one to respond. Create an entry
          fsm = new SegmentFinalizerFSM(_segmentManager, holder, segmentMetadata.getNumReplicas());
        }
        LOGGER.info("Created FSM {}", fsm);
        _fsmMap.put(segmentName, fsm);
      } catch (Exception e) {
        // Server gone wonky. Segment does not exist in propstore
        LOGGER.error("Exception reading segment read from propertystore {}", segmentName, e);
        throw new RuntimeException("Segment read from propertystore " + segmentName, e);
      }
    }
    return fsm;
  }

  public SegmentFinalProtocol.Response segmentConsumed(final String segmentName, final String instanceId, final long offset) {
    if (!_helixManager.isLeader()) {
      return SegmentFinalProtocol.RESP_NOT_LEADER;
    }
    RealtimeSegmentNameHolder holder = new RealtimeSegmentNameHolder(segmentName);
    SegmentFinalizerFSM fsm = lookupOrCreateFsm(holder, SegmentFinalProtocol.MSG_TYPE_CONSUMED, offset);
    SegmentFinalProtocol.Response response = fsm.segmentConsumed(instanceId, offset);
    if (fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentName);
    }
    return response;
  }

  public SegmentFinalProtocol.Response segmentCommit(final String segmentName, final String instanceId, final long offset) {
    if (!_helixManager.isLeader()) {
      return SegmentFinalProtocol.RESP_NOT_LEADER;
    }
    RealtimeSegmentNameHolder holder = new RealtimeSegmentNameHolder(segmentName);
    SegmentFinalizerFSM fsm = lookupOrCreateFsm(holder, SegmentFinalProtocol.MSG_TYPE_CONSUMED, offset);
    SegmentFinalProtocol.Response response = fsm.segmentCommit(instanceId, offset);
    if (fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(instanceId);
    }
    return response;
  }

  private static class SegmentFinalizerFSM {
    // We expect a 10% variation between hosts, so we add 10% to the max hold time to pick a winner.
    public static final long  MAX_TIME_TO_PICK_WINNER_MS =
        ValidationConstants.MAX_HOLD_TIME_MS + (ValidationConstants.MAX_HOLD_TIME_MS / 10);

    // Once we pick a winner, the winner may get notified in the next call, so add one hold time plus some.
    public static final long MAX_TIME_TO_NOTIFY_WINNER_MS = MAX_TIME_TO_PICK_WINNER_MS +
        ValidationConstants.MAX_HOLD_TIME_MS + (ValidationConstants.MAX_HOLD_TIME_MS / 10);

    // Once the winner is notified, the are expected to start right away. At this point, it is the segment commit
    // time that we need to consider.
    // We may need to add some time here to allow for getting the lock? For now 0
    // We may need to add some time for the committer come back to us? For now 0.
    public static final long MAX_TIME_ALLOWED_TO_COMMIT_MS = MAX_TIME_TO_NOTIFY_WINNER_MS + ValidationConstants.MAX_SEGMENT_COMMIT_TIME_MS;

    public final Logger LOGGER;

    State _state = State.HOLDING;   // Always start off in HOLDING state.
    final long _startTime = System.currentTimeMillis();
    private final RealtimeSegmentNameHolder _holder;
    private final int _numReplicas;
    private final Map<String, Long> _commitStateMap;
    private long _winningOffset = -1L;
    private String _winner;
    private final LLRealtimeSegmentManager _segmentManager;

    public SegmentFinalizerFSM(LLRealtimeSegmentManager segmentManager, RealtimeSegmentNameHolder holder, int numReplicas, long winningOffset) {
      // Constructor used when we get an event after a segment is committed.
      this(segmentManager, holder, numReplicas);
      _state = State.COMMITTED;
      _winningOffset = winningOffset;
      _winner = "UNKNOWN";
    }

    @Override
    public String toString() {
      return "{" + _holder.getName() + "," + _state + "," + _startTime + "," + _winner + "," + _winningOffset + "}";
    }

    public SegmentFinalizerFSM(LLRealtimeSegmentManager segmentManager, RealtimeSegmentNameHolder holder, int numReplicas) {
      _holder = holder;
      _numReplicas = numReplicas;
      _segmentManager = segmentManager;
      _commitStateMap = new HashMap<>(_numReplicas);
      LOGGER = LoggerFactory.getLogger("SegmentFinalizerFSM_"  + holder.getName());
    }

    public boolean isDone() {
      return _state.equals(State.COMMITTED) || _state.equals(State.ABORTED);
    }

    public SegmentFinalProtocol.Response segmentConsumed(String instanceId, long offset) {
      final long now = System.currentTimeMillis();
      synchronized (this) {
        LOGGER.info("Processing segmentConsumed({}, {})", instanceId, offset);
        _commitStateMap.put(instanceId, offset);
        SegmentFinalProtocol.Response response = null;
        switch (_state) {
          case HOLDING:
            if (now > _startTime + MAX_TIME_TO_PICK_WINNER_MS || _commitStateMap.size() == _numReplicas) {
              LOGGER.info("{}:Picking winner time={} size={}", _state, now-_startTime, _commitStateMap.size());
              pickWinner(instanceId);
              if (_winner.equals(instanceId)) {
                LOGGER.info("{}:Committer notified winner instance={} offset={}", _state, instanceId, offset);
                _state = State.COMMITTER_NOTIFIED;
                response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.COMMIT, offset);
              } else {
                LOGGER.info("{}:Committer decided winner={} offset={}", _state, _winner, _winningOffset);
                _state = State.COMMITTER_DECIDED;
                response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.CATCHUP, _winningOffset);
              }
            } else {
              response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
              LOGGER.info("{}:Holding for instance={}  offset={}", _state, instanceId, offset);
            }
            return response;
          case COMMITTER_DECIDED: // This must be a retransmit
            assert(offset <= _winningOffset); // TODO Abort instead of Assert
            if (_winner.equals(instanceId)) {
              if (_winningOffset == offset) {
                LOGGER.info("{}:Committer notified winner instance={} offset={}", _state, instanceId, offset);
                _state = State.COMMITTER_NOTIFIED;
                response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.COMMIT, offset);
              } else {
                // Braindead winner
                LOGGER.info("{}:ABORT for instance={}  offset={}", _state, instanceId, offset);
                response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
              }
            } else  if (offset == _winningOffset) {
              // Wait until winner has posted the segment.
              LOGGER.info("{}:Holding for instance={}  offset={}", _state, instanceId, offset);
              response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
            } else {
              LOGGER.info("{}:Cathing up for instance={}  offset={}", _state, instanceId, _winningOffset);
              response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.CATCHUP, _winningOffset);
            }
            if (now > _startTime + MAX_TIME_TO_NOTIFY_WINNER_MS) {
              // Winner never got back to us. Abort the commit protocol and start afresh.
              // We can potentially optimize here to see if this instance has the highest so far, and re-elect them to
              // be winner, but for now, we will abort it and restart
              LOGGER.warn("{}:Abort for instance={}  offset={}", _state, instanceId, offset);
              _state = State.ABORTED;
              response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
            }
            return response;
          case COMMITTER_NOTIFIED:
          case COMMITTER_UPLOADING:
          case COMMITTING:
            if (now > _startTime + MAX_TIME_ALLOWED_TO_COMMIT_MS) {
              // Too late for anything to happen.
              _state = State.ABORTED;
              LOGGER.warn("{}:Aborting FSM because it is too late instance={} offset={} now={}", _state, instanceId, offset, now);
              return new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
            }
            if (instanceId.equals(_winner)) {
              // Winner is coming back to with a HOLD. Perhaps the commit call failed in the winner for some reason
              // Allow them to be winner again.
              if (offset == _winningOffset) {
                LOGGER.info("{}:Commit for instance={}  offset={}", _state, instanceId, offset);
                response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.COMMIT, offset);
              } else {
                // Something seriously wrong. Abort the transaction
                _state = State.ABORTED;
                response = SegmentFinalProtocol.RESP_DISCARD;
                LOGGER.warn("{}:Abort for instance={} offset={}", _state, instanceId, offset);
              }
            } else {
              // A different instance is reporting.
              if (offset == _winningOffset) {
                // Wait until winner has posted the segment before asking this server to KEEP the segment.
                LOGGER.info("{}:HOLD for instance={} offset={}", _state, instanceId, offset);
                response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
              } else if (offset < _winningOffset) {
                LOGGER.info("{}:CATCHUP for instance={} offset={}", _state, instanceId, offset);
                response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.CATCHUP,
                    _winningOffset);
              } else {
                // We have not yet committed, so ask the new responder to hold. They may be the new leader in case the
                // committer fails.
                LOGGER.info("{}:HOLD for instance={} offset={}", _state, instanceId, offset);
                response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
              }
            }
            return response;
          case COMMITTED:
            if (offset == _winningOffset) {
              // Need to return KEEP
              LOGGER.info("{}:KEEP for instance={} offset={}", _state, instanceId, offset);
              response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.KEEP, offset);
            } else {
              // Return DISCARD. It is hard to say how long the server will take to complete things.
              LOGGER.info("{}:DISCARD for instance={} offset={}", _state, instanceId, offset);
              response = SegmentFinalProtocol.RESP_DISCARD;
            }
            return response;
          case ABORTED:
            LOGGER.info("{}:ABORT for instance={} offset={}", _state, instanceId, offset);
            response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
            return response;
          default:
            LOGGER.info("{}:FAILED for instance={} offset={}", _state, instanceId, offset);
            return SegmentFinalProtocol.RESP_FAILED;
        }
      }
    }

    public SegmentFinalProtocol.Response segmentCommit(String instanceId, long offset) {
      long now = System.currentTimeMillis();
      SegmentFinalProtocol.Response response = null;
      synchronized (this) {
        LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
        _commitStateMap.put(instanceId, offset);
        switch (_state) {
          case HOLDING:
          case COMMITTER_DECIDED:
            if (now > _startTime + MAX_TIME_ALLOWED_TO_COMMIT_MS) {
              // Too late for anything to happen.
              _state = State.ABORTED;
              LOGGER.warn("{} Aborting due to time instance={} offset={} now={}", _state, instanceId, offset, now);
              return new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
            }
            // We cannot get a commit if we are in this state, so ask them to hold. Maybe we are starting afresh.
            LOGGER.info("{}:HOLD for instance={} offset={}", _state, instanceId, offset);
            return new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
          case COMMITTER_NOTIFIED:
            if (now > _startTime + MAX_TIME_ALLOWED_TO_COMMIT_MS) {
              // Too late for anything to happen.
              _state = State.ABORTED;
              LOGGER.warn("{} Aborting due to time instance={} offset={} now={}", _state, instanceId, offset, now);
              return new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
            }
            if (instanceId.equals(_winner) && offset == _winningOffset) {
              // Accept commit. Need to release lock here while downloading the segment.
              LOGGER.info("{}:Uploading for instance={} offset={}", _state, instanceId, offset);
              _state = State.COMMITTER_UPLOADING;
            } else {
              // Hmm. Committer has been notified, but either a different one is committing, or offset is different
              LOGGER.info("{}:ABORT for instance={} offset={} winner={} winningOffset={}", _state, instanceId, offset,
                  _winner, _winningOffset);
            }
            break;
          case COMMITTER_UPLOADING:
          case COMMITTING:
            if (now > _startTime + MAX_TIME_ALLOWED_TO_COMMIT_MS) {
              // Too late for anything to happen.
              _state = State.ABORTED;
              LOGGER.warn("{} Aborting due to time instance={} offset={} now={}", _state, instanceId, offset, now);
              return new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
            }
            // Another committer came in while one was uploading. Ask them to hold in case this one fails.
            // We are committing to zk when another one came in. Ask them to hold.
            return new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
          case COMMITTED:
            if (offset == _winningOffset) {
              // Need to return KEEP
              LOGGER.info("{}:KEEP for instance={} offset={}", _state, instanceId, offset);
              response = new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.KEEP, offset);
            } else {
              // Return DISCARD. It is hard to say how long the server will take to complete things.
              LOGGER.info("{}:DISCARD for instance={} offset={}", _state, instanceId, offset);
              response = SegmentFinalProtocol.RESP_DISCARD;
            }
            return response;
          case ABORTED:
            LOGGER.info("{}:ABORT for instance={} offset={}", _state, instanceId, offset);
            return new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.HOLD, offset);
          default:
            LOGGER.info("{}:FAILED for instance={} offset={}", _state, instanceId, offset);
            return SegmentFinalProtocol.RESP_FAILED;
        }
      }
      // Outside sync block. THe only way we get here is if the state is COMMITTER_UPLOADING.

      assert(_state.equals(State.COMMITTER_UPLOADING));

      boolean success = false;
      try {
        // TODO we need to keep a handle to the uploader so that we can stop it via the fsm.stop() call.
        success = saveTheSegment();
      } catch (Exception e) {
        LOGGER.error("Segment upload failed");
      }
      if (!success) {
        // Committer failed when posting the segment. Start over.
        _state = State.ABORTED;
        return SegmentFinalProtocol.RESP_FAILED;
      }

      /*
       * Before we enter this code loop, it is possible that others threads have asked for this FSM, or perhaps
       * we just took too long and this FSM has already been aborted or committed by someone else.
       */
      now = System.currentTimeMillis();
      synchronized (this) {
        if (isDone()) {
          LOGGER.warn("Segment done during upload: state={} segment={} winner={} winningOffset={}",
              _state, _holder.getName(), _winner, _winningOffset);
          return SegmentFinalProtocol.RESP_FAILED;
        }
        LOGGER.info("Committing segment {} at offset {} winner {}", _holder.getName(), offset, instanceId);
        _state = State.COMMITTING;
        success = _segmentManager.commitSegment(_holder.getTableName(), _holder.getName(), _winningOffset);
        if (success) {
          _state = State.COMMITTED;
          LOGGER.info("Committed segment {} at offset {} winner {}", _holder.getName(), offset, instanceId);
          return SegmentFinalProtocol.RESP_COMMIT_SUCCESS;
        }
      }
      return new SegmentFinalProtocol.Response(SegmentFinalProtocol.ControllerResponseStatus.FAILED, -1L);
    }

    // Sleep for a random time to simulate segment upload.
    private boolean saveTheSegment() throws InterruptedException {
      Random random = new Random();
      long maxCommitTime = ValidationConstants.MAX_SEGMENT_COMMIT_TIME_MS;
      long reduction = random.nextInt((int)maxCommitTime/2);
      long sleepTime = maxCommitTime - reduction;

      Thread.sleep(sleepTime);

      // XXX: this can fail
      return true;
    }

    // Pick a winner, preferring this instance if tied for highest.
    // Side-effect: Sets the _winner and _winningOffset
    private void pickWinner(String preferredInstance) {
      long maxSoFar = -1;
      String winner = null;
      for (Map.Entry<String, Long> entry : _commitStateMap.entrySet()) {
        if (entry.getValue() > maxSoFar) {
          maxSoFar = entry.getValue();
          winner = entry.getKey();
        }
      }
      _winningOffset = maxSoFar;
      if (_commitStateMap.get(preferredInstance) == maxSoFar) {
        winner = preferredInstance;
      }
      _winner =  winner;
    }
  }
}
