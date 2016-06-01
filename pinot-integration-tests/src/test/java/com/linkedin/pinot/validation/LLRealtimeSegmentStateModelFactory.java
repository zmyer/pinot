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
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;


public class LLRealtimeSegmentStateModelFactory extends StateModelFactory<StateModel> {
  public static final Logger LOGGER = LoggerFactory.getLogger(LLRealtimeSegmentStateModelFactory.class);
  private final String _instanceId;
  private final String _clusterName;
  private final HelixManager _helixManager;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private static final long maxConsumingAfterOnlineTransitionMs = ValidationConstants.MAX_CATCHUP_ON_CONSUME_TO_ONLINE_MS;

  private final Map<String, PartitionConsumer> _consumerMap = new HashMap<>();

  public LLRealtimeSegmentStateModelFactory(String instanceId, HelixManager helixManager) {
    _instanceId = instanceId;
    _clusterName = ValidationConstants.CLUSTER_NAME;
    _helixManager = helixManager;
  }
  public static String getStateModelDef() {
    return DummyStateModel.getName();
  }

  public void setPropertyStore(ZkHelixPropertyStore propertyStore) {
    _propertyStore = propertyStore;
  }

  @Override
  public StateModel createNewStateModel(String partitionName) {
    return new RealtimeSegmentStateModel(_instanceId);
  }

  @StateModelInfo(states = "{'OFFLINE', 'CONSUMING', 'ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public class RealtimeSegmentStateModel extends StateModel {
//    public final Logger LOGGER = LoggerFactory.getLogger(_instanceId + "-" + RealtimeSegmentStateModel.class);

    private final String _instanceId;

    public RealtimeSegmentStateModel(String instanceId) {
      _instanceId = instanceId;
    }

    private void removeConsumerIfExists(String segmentName) {
      PartitionConsumer consumer = _consumerMap.get(segmentName);
      if (consumer != null) {
        try {
          consumer.stop();
        } catch (InterruptedException e) {
          LOGGER.warn("Exception trying to stop consumer {}", consumer.toString(), e);
        }
        _consumerMap.remove(segmentName);
      }
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      final String segmentName = message.getPartitionName();
      removeConsumerIfExists(segmentName);
      LOGGER.info("OFFLINE to ONLINE for segment {}", segmentName);
    }

    @Transition(from = "OFFLINE", to = "CONSUMING")
    public void onBecomeConsumingFromOffline(Message message, NotificationContext context) {
      final String segmentName = message.getPartitionName();
      LOGGER.info("OFFLINE to CONSUMING for segment " + segmentName);
      RealtimeSegmentNameHolder holder = new RealtimeSegmentNameHolder(segmentName);
      PartitionConsumer consumer = _consumerMap.get(segmentName);
      if (consumer != null) {
        throw new RuntimeException("Consumer exists " + consumer.toString());
      }
      consumer = new PartitionConsumer(holder, ValidationConstants.KAFKA_TOPIC, _instanceId);
      _consumerMap.put(segmentName, consumer);
      consumer.consume();
    }

    @Transition(from = "CONSUMING", to = "ONLINE")
    public void onBecomeOnlineFromConsuming(Message message, NotificationContext context) {
      final String segmentName = message.getPartitionName();
      LOGGER.info("CONSUMING to ONLINE for segment {}", segmentName);
      RealtimeSegmentNameHolder holder = new RealtimeSegmentNameHolder(segmentName);
      PartitionConsumer consumer = _consumerMap.get(segmentName);
      if (consumer == null) {
        // XXX Can an old message come in here?
        throw new RuntimeException("Consumer does not " + consumer.toString());
      }
      // It can happen that we get this transition before we come back from the commit call.
      try {
        if (consumer.waitForCommitted(1000)) {
          // We committed the segment and have replaced it in the map?
          // TBD.
          consumer.stop();
          _consumerMap.remove(segmentName);
          LOGGER.info("Transition completed while waiting for COMMIT");
          return;
        }
        long currentOffset = consumer.stop();
        LLRealtimeSegmentZKMetadata metadata = getRealtimeSegmentZKMetadata(holder.getTableName(), segmentName);
        final long targetOffset = metadata.getEndOffset();
        if (targetOffset > currentOffset) {
          // We don't know how long it will take, so initiate consume with a timeout.
          long newOffset = consumer.catchup(targetOffset, maxConsumingAfterOnlineTransitionMs);
          if (newOffset != targetOffset) {
            // Abort the segment, download and go online
            LOGGER.info("Timeout trying to reach offset. Downloading segment");
            Thread.sleep(5000L);  // Time take to download segment from controller and load it in memory.
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("Exception cacthing up "+ consumer.toString(), e);
      }
      // In any case, remove this consumer.
      _consumerMap.remove(segmentName);
    }

    @Transition(from = "CONSUMING", to = "OFFLINE")
    public void onBecomeOfflineFromConsuming(Message message, NotificationContext context) {
      final String segmentName = message.getPartitionName();
      LOGGER.info("CONSUMING to OFFLINE for segment {}", segmentName);
      PartitionConsumer consumer = _consumerMap.get(segmentName);
      if (consumer == null) {
        // XXX Can an old message come in here?
        LOGGER.warn("No consumer for segment {}", segmentName);
        return;
      }
      try {
        consumer.stop();
        // Do something to discard segment
      } catch (InterruptedException e) {
        LOGGER.warn("Exception stopping consumer {}", consumer.toString(), e);
      }
      _consumerMap.remove(segmentName);
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      final String segmentName = message.getPartitionName();
      LOGGER.info("ONLINE to OFFLINE for segment {}", segmentName);
      PartitionConsumer consumer = _consumerMap.get(segmentName);
      assert consumer == null;
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      final String segmentName = message.getPartitionName();
      LOGGER.info("OFFLINE to DROPPED for segment {}", segmentName);
      // Do whatever to go to dropped
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      final String segmentName = message.getPartitionName();
      LOGGER.info("ONLINE to DROPPED for segment {}", segmentName);
      assert _consumerMap.get(segmentName) == null;
      // Do whatever to go to dropped
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      final String segmentName = message.getPartitionName();
      LOGGER.info("ERROR to OFFLINE for segment {}", segmentName);
      // We were in error state, maybe there is a consumer running. Make an effort to stop it.
      PartitionConsumer consumer = _consumerMap.get(segmentName);
      if (consumer == null) {
        LOGGER.info("No consumer for segment {}", segmentName);
        return;
      }
      try {
        consumer.stop();
      } catch (InterruptedException e) {
        LOGGER.warn("Exception stopping consumer {}", consumer.toString(), e);
      }
      _consumerMap.remove(segmentName);
    }

    public LLRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String tableName, String segmentName) {
      String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      return new LLRealtimeSegmentZKMetadata(_propertyStore.get(
          ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName), null, AccessOption.PERSISTENT));
    }
  }
}
