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
package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ControllerGauge;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.util.TableSizeReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the segment status metrics, regarding tables with fewer replicas than requested
 * and segments in error state
 *
 * May 15, 2016
*/

public class SegmentStatusChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStatusChecker.class);
  private static final int SegmentCheckerDefaultIntervalSeconds = 120;
  private static final int MaxOfflineSegmentsToLog = 5;
  public static final String ONLINE = "ONLINE";
  public static final String ERROR = "ERROR";
  public static final String CONSUMING = "CONSUMING";
  private ScheduledExecutorService _executorService;
  private final ControllerMetrics _metricsRegistry;
  private final ControllerConf _config;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final HelixAdmin _helixAdmin;
  private final long _segmentStatusIntervalSeconds;
  private final int _waitForPushTimeSeconds;
  private final TableSizeReader _tableSizeReader;

  /**
   * Constructs the segment status checker.
   * @param pinotHelixResourceManager The resource checker used to interact with Helix
   * @param config The controller configuration object
   */
  public SegmentStatusChecker(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf config, ControllerMetrics metricsRegistry) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _helixAdmin = pinotHelixResourceManager.getHelixAdmin();
    _config = config;
    _segmentStatusIntervalSeconds = config.getStatusCheckerFrequencyInSeconds();
    _waitForPushTimeSeconds = config.getStatusCheckerWaitForPushTimeInSeconds();
    _metricsRegistry = metricsRegistry;
    HttpConnectionManager httpConnectionManager = new MultiThreadedHttpConnectionManager();
    _tableSizeReader = new TableSizeReader(Executors.newCachedThreadPool(), httpConnectionManager, _pinotHelixResourceManager);

    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("Segment status checker");
        return thread;
      }
    });
  }

  /**
   * Starts the segment status checker.
   */
  public void start() {
    if (_segmentStatusIntervalSeconds == -1) {
      LOGGER.warn("Segment status check interval is -1, status checks disabled.");
      return;
    }

    setStatusToDefault();

    _executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          updateSegmentMetrics();
        } catch (Exception e) {
          LOGGER.warn("Caught exception while running segment status checker", e);
        }
      }
    }, SegmentCheckerDefaultIntervalSeconds, _segmentStatusIntervalSeconds, TimeUnit.SECONDS);
  }


  /**
   * Stops the segment status checker.
   */
  public void stop() {
    if (_executorService == null) {
      return;
    }

    // Shut down the executor
    _executorService.shutdown();

    try {
      _executorService.awaitTermination(SegmentCheckerDefaultIntervalSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Ignored
    }

    _executorService = null;
  }

  /**
   * Runs a segment status pass over the currently loaded tables.
   */
  void updateSegmentMetrics() {
    if (!_pinotHelixResourceManager.isLeader()) {
      LOGGER.info("Skipping Segment Status check, not leader!");
      setStatusToDefault();
      return;
    }

    long startTime = System.nanoTime();

    LOGGER.info("Starting Segment Status check for metrics");

    // Fetch the list of tables
    List<String> allTableNames = _pinotHelixResourceManager.getAllTables();
    String helixClusterName = _pinotHelixResourceManager.getHelixClusterName();
    HelixAdmin helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    int realTimeTableCount = 0;
    int offlineTableCount = 0;
    ZkHelixPropertyStore<ZNRecord> propertyStore= _pinotHelixResourceManager.getPropertyStore();

    for (String tableName : allTableNames) {
      try {
        if (TableNameBuilder.getTableTypeFromTableName(tableName) == TableType.OFFLINE) {
          offlineTableCount++;
        } else {
          realTimeTableCount++;
        }
        IdealState idealState = helixAdmin.getResourceIdealState(helixClusterName, tableName);
        if ((idealState == null) || (idealState.getPartitionSet().isEmpty())) {
          int nReplicasFromIdealState = 1;
          try {
            if (idealState != null) {
              nReplicasFromIdealState = Integer.valueOf(idealState.getReplicas());
            }
          } catch (NumberFormatException e) {
            // Ignore
          }
          _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS, nReplicasFromIdealState);
          _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS, 100);
          _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, 100);
          continue;
        }
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.IDEALSTATE_ZNODE_SIZE, idealState.toString().length());
        ExternalView externalView = helixAdmin.getResourceExternalView(helixClusterName, tableName);

        int nReplicasIdealMax = 0; // Keeps track of maximum number of replicas in ideal state
        int nReplicasExternal = -1; // Keeps track of minimum number of replicas in external view
        int nErrors = 0; // Keeps track of number of segments in error state
        int nOffline = 0; // Keeps track of number segments with no online replicas
        int nSegments = 0; // Counts number of segments
        for (String partitionName : idealState.getPartitionSet()) {
          int nReplicas = 0;
          int nIdeal = 0;
          nSegments++;
          // Skip segments not online in ideal state
          for (Map.Entry<String, String> serverAndState : idealState.getInstanceStateMap(partitionName).entrySet()) {
            if (serverAndState == null) {
              break;
            }
            if (serverAndState.getValue().equals(ONLINE)) {
              nIdeal++;
              break;
            }
          }
          if (nIdeal == 0) {
            // No online segments in ideal state
            continue;
          }
          nReplicasIdealMax = (idealState.getInstanceStateMap(partitionName).size() > nReplicasIdealMax) ? idealState.getInstanceStateMap(partitionName).size() : nReplicasIdealMax;
          if ((externalView == null) || (externalView.getStateMap(partitionName) == null)) {
            // No replicas for this segment
            TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
            if ((tableType != null) && (tableType.equals(TableType.OFFLINE))) {
              OfflineSegmentZKMetadata segmentZKMetadata =
                  ZKMetadataProvider.getOfflineSegmentZKMetadata(propertyStore, tableName, partitionName);
              if (segmentZKMetadata != null
                  && segmentZKMetadata.getPushTime() > System.currentTimeMillis() - _waitForPushTimeSeconds * 1000) {
                // push not yet finished, skip
                continue;
              }
            }
            nOffline++;
            if (nOffline < MaxOfflineSegmentsToLog) {
              LOGGER.warn("Segment {} of table {} has no replicas", partitionName, tableName);
            }
            nReplicasExternal = 0;
            continue;
          }
          for (Map.Entry<String, String> serverAndState : externalView.getStateMap(partitionName).entrySet()) {
            // Count number of online replicas. Ignore if state is CONSUMING.
            // It is possible for a segment to be ONLINE in idealstate, and CONSUMING in EV for a short period of time.
            // So, ignore this combination. If a segment exists in this combination for a long time, we will get
            // kafka-partition-not-consuming alert anyway.
            if (serverAndState.getValue().equals(ONLINE) || serverAndState.getValue().equals(CONSUMING)) {
              nReplicas++;
            }
            if (serverAndState.getValue().equals(ERROR)) {
              nErrors++;
            }
          }
          if (nReplicas == 0) {
            if (nOffline < MaxOfflineSegmentsToLog) {
              LOGGER.warn("Segment {} of table {} has no online replicas", partitionName, tableName);
            }
            nOffline++;
          }
          nReplicasExternal = ((nReplicasExternal > nReplicas) || (nReplicasExternal == -1)) ? nReplicas : nReplicasExternal;
        }
        if (nReplicasExternal == -1) {
          nReplicasExternal = (nReplicasIdealMax == 0) ? 1 : 0;
        }
        // Synchronization provided by Controller Gauge to make sure that only one thread updates the gauge
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS, nReplicasExternal);
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS,
            (nReplicasIdealMax > 0) ? (nReplicasExternal * 100 / nReplicasIdealMax) : 100);
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE, nErrors);
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE,
            (nSegments > 0) ? (100 - (nOffline * 100 / nSegments)) : 100);
        if (nOffline > 0) {
          LOGGER.warn("Table {} has {} segments with no online replicas", tableName, nOffline);
        }
        if (nReplicasExternal < nReplicasIdealMax) {
          LOGGER.warn("Table {} has {} replicas, below replication threshold :{}", tableName, nReplicasExternal,
              nReplicasIdealMax);
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while updating segment status for table {}", e, tableName);

        // Remove the metric for this table
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS, Long.MIN_VALUE);
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS, Long.MIN_VALUE);
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE, Long.MIN_VALUE);
        _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, Long.MIN_VALUE);
      }
    }

    _metricsRegistry.setValueOfGlobalGauge(ControllerGauge.REALTIME_TABLE_COUNT, realTimeTableCount);
    _metricsRegistry.setValueOfGlobalGauge(ControllerGauge.OFFLINE_TABLE_COUNT, offlineTableCount);
    long totalNanos = System.nanoTime() - startTime;
    LOGGER.info("Segment status metrics completed in {}ms",
        TimeUnit.MILLISECONDS.convert(totalNanos, TimeUnit.NANOSECONDS));
  }

  private void setStatusToDefault() {
    List<String> allTableNames = _pinotHelixResourceManager.getAllTables();

    for (String tableName : allTableNames) {
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS, Long.MIN_VALUE);
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS, Long.MIN_VALUE);
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE, Long.MIN_VALUE);
      _metricsRegistry.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, Long.MIN_VALUE);
    }
  }
}
