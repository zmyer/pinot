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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.api.pojos.Instance;


/*
 * A class that we need for low-level realtime segment manager. We don't need to set watches on
 * propstore or segments in this one, so it is different from the existing segment manager.
 */
public class LLRealtimeSegmentManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(LLRealtimeSegmentManager.class);

  private static LLRealtimeSegmentManager instance = null;

  private static final String SEGMENTS_PATH = "/SEGMENTS";
  private final HelixAdmin _helixAdmin;
  private final HelixManager _helixManager;
  private final String _clusterName;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private boolean _amILeader = false;

  // Only for simulation
  private Random _random = new Random();

  private LLRealtimeSegmentManager(HelixAdmin helixAdmin, HelixManager helixManager,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _helixAdmin = helixAdmin;
    _helixManager = helixManager;
    _clusterName = _helixManager.getClusterName();
    _propertyStore = propertyStore;
  }

  public static void create(HelixAdmin helixAdmin, HelixManager helixManager, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    if (instance == null) {
      instance = new LLRealtimeSegmentManager(helixAdmin, helixManager, propertyStore);
    } else {
      LOGGER.error("Instance already created");
    }
  }

  public static LLRealtimeSegmentManager getInstance() {
    if (instance == null) {
      throw new RuntimeException("Instance not created");
    }
    return instance;
  }

  public void addTable(String realtimeTableName, List<Instance> instances) {
    final IdealState idealState = DummyStateModel
        .buildEmptyIdealStateFor(realtimeTableName, instances.size(), _helixAdmin, _clusterName);
    LOGGER.info("Adding realtime table " + realtimeTableName);
    try {
      _helixAdmin.addResource(_clusterName, realtimeTableName, idealState);
      LOGGER.info("Successfully added realtime table " + realtimeTableName);
    } catch (HelixException e) {
      if (e.getMessage().contains("already exists")) {
        LOGGER.warn("Table already exists?", e);
      } else {
        throw e;
      }
    }
    continueAddTable(realtimeTableName, instances, 0L);
  }

  // Also needs to be done during leadership change, for each table that is in the process of being added.
  // On leadership change, this should be called ONLY for realtime tables that are freshly created
  // but aborted during table creation.
  private void continueAddTable(String realtimeTableName, List<Instance> instances, long startOffset) {
    final List<String> instanceNames = new ArrayList<>(instances.size());
    for (Instance instance : instances) {
      instanceNames.add(instance.toInstanceId());
    }

//    final IdealState idealState = PinotTableIdealStateBuilder
//        .buildEmptyIdealStateFor(_realtimeTableName, ValidationConstants.NUM_REPLICAS, _helixAdmin, _clusterName);
    // Map of segment names to the server-instances that hold the segment.
    final Map<String, List<String>> idealStateEntries = new HashMap<String, List<String>>(4);
    // Add a table
//    _helixAdmin.addResource(_clusterName, _realtimeTableName, idealState);
    // Set up propertystore.
    // If controller fails after we add the helix resource but before we set up prop store, it will be obvious and we can
    // drop the resource and add it back again.

    // Create an entry in propertystore for each kafka partition.
    // Any of these may already be there, so bail out clean if they are already present.
    // TODO Get the number of partitions from kafka topic
    List<String> paths = new ArrayList<>(ValidationConstants.NUM_KAFKA_PARTITIONS);
    List<ZNRecord> records = new ArrayList<>(ValidationConstants.NUM_KAFKA_PARTITIONS);
    for (int i = 0; i < ValidationConstants.NUM_KAFKA_PARTITIONS; i++) {
      LLRealtimeSegmentZKMetadata metadata = new LLRealtimeSegmentZKMetadata();
      metadata.setCreationTime(System.currentTimeMillis());
      metadata.setStartOffset(startOffset);
      metadata.setNumReplicas(instances.size());
      metadata.setTableName(realtimeTableName);
      String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
      RealtimeSegmentNameHolder holder = new RealtimeSegmentNameHolder(rawTableName, i, startOffset);
      final String segName = holder.getName();
      metadata.setSegmentName(segName);
      metadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      ZNRecord record = metadata.toZNRecord();
      final String znodePath = SEGMENTS_PATH + "/" + realtimeTableName + "/" + segName;
      paths.add(znodePath);
      records.add(record);
      /*
      try {
        // TODO Can we use setChildren to create all nodes with expected version set to -1?
        _propertyStore.create(znodePath, record, AccessOption.PERSISTENT);
      } catch (HelixException e) {
        if (e.getMessage().contains("already exists")) {
          LOGGER.warn("Property store entry already exists?", e);
        } else {
          throw e;
        }
      }
      */
      idealStateEntries.put(segName, instanceNames);
    }

    _propertyStore.createChildren(paths, records, AccessOption.PERSISTENT);

    HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        return DummyStateModel.addOrUpdateRealtimeSegmentInIdealState(idealState, idealStateEntries);
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
  }

  // Update Zk to do the following:
  // 1. Rewrite metadata with the endOffset and status to DONE
  // 2. Change state of the segment in IDealstate to ONLINE
  // 3. Add a new segment in CONSUMING state.
  public boolean commitSegment(String tableName, final String committingSegmentName, long endOffset) {
    final long now = System.currentTimeMillis();
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    final LLRealtimeSegmentZKMetadata oldSegMetadata = getRealtimeSegmentZKMetadata(tableName, committingSegmentName);
    // TODO Get data from the segment that came in and set it here.
    oldSegMetadata.setEndOffset(endOffset);
    oldSegMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    oldSegMetadata.setEndTime(now);
    final ZNRecord oldZnRecord = oldSegMetadata.toZNRecord();

    final String oldZnodePath = SEGMENTS_PATH + "/" + realtimeTableName + "/" + committingSegmentName;

//    HelixHelper.getAllInstancesForResource(HelixHelper.getTableIdealState(_helixManager, realtimeTableName));

    final List<Instance> instancesForNewSegment = getNewSegmentInstances(realtimeTableName);
    final List<String> newInstances = new ArrayList<>(instancesForNewSegment.size());
    for (Instance instance : instancesForNewSegment) {
      newInstances.add(instance.toInstanceId());
    }
    RealtimeSegmentNameHolder oldHolder = new RealtimeSegmentNameHolder(committingSegmentName);
    final int partitionId = oldHolder.getKafkaPartition();

    final long newStartOffset = endOffset+1;
    RealtimeSegmentNameHolder newHolder = new RealtimeSegmentNameHolder(tableName, partitionId, newStartOffset);
    final String newSegmentName = newHolder.getName();
    LOGGER.info("Adding new segment {}", newSegmentName);
    final LLRealtimeSegmentZKMetadata newSegMetadata = new LLRealtimeSegmentZKMetadata();
    newSegMetadata.setCreationTime(System.currentTimeMillis());
    newSegMetadata.setStartOffset(newStartOffset);
    newSegMetadata.setNumReplicas(ValidationConstants.NUM_REPLICAS);
    newSegMetadata.setTableName(realtimeTableName);
    newSegMetadata.setSegmentName(newSegmentName);
    newSegMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    final ZNRecord newZnRecord = newSegMetadata.toZNRecord();
    final String newZnodePath = SEGMENTS_PATH + "/" + realtimeTableName + "/" + newSegmentName;

    List<String> paths = new ArrayList<>(2);
    paths.add(oldZnodePath);
    paths.add(newZnodePath);
    List<ZNRecord> records = new ArrayList<>(2);
    records.add(oldZnRecord);
    records.add(newZnRecord);
    _propertyStore.setChildren(paths, records, AccessOption.PERSISTENT);

    if (_random.nextInt(100) < 30) {
      // 30% chance of controller failure during commit
      LOGGER.info("====== Failing to commit segment {}", newSegmentName);
      System.exit(1);
    }

    HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        return DummyStateModel
            .updateForNewRealtimeSegment(idealState, newInstances, committingSegmentName, newSegmentName);
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));

    return true;
  }

  private List<Instance> getNewSegmentInstances(String realtimeTableName) {
    // Read the kafka partition entry in PROPERTYSTORE to get this list.
    return ValidationConstants.generateInstanceNames();
  }

  // Should go to zk Utils. also used in server
  public LLRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String tableName, String segmentName) {
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    return new LLRealtimeSegmentZKMetadata(_propertyStore.get(
        ZKMetadataProvider.constructPropertyStorePathForSegment(realtimeTableName, segmentName), null, AccessOption.PERSISTENT));
  }

  // Needed for subscribeChildChanges
//    @Override
//    public void handleChildChange(String parentPath, List<String> currentChilds)
//        throws Exception {
//      LOGGER.info("NOT NEEDED PinotRealtimeSegmentManager.handleChildChange: {}", parentPath);
//    processPropertyStoreChange(parentPath);
//    for (String table : currentChilds) {
//      if (table.endsWith("_REALTIME")) {
//        LOGGER.info("PinotRealtimeSegmentManager.handleChildChange with table: {}", parentPath + "/" + table);
//        processPropertyStoreChange(parentPath + "/" + table);
//      }
//    }
//    }

  // Needed for subscribeDataChanges
//    @Override
//    public void handleDataChange(String dataPath, Object data)
//        throws Exception {
//      LOGGER.info("NOT NEEDED PinotRealtimeSegmentManager.handleDataChange: {}", dataPath);
//    processPropertyStoreChange(dataPath);
//    }

  // Needed for subscribeDataChanges
//    @Override
//    public void handleDataDeleted(String dataPath)
//        throws Exception {
//      LOGGER.info("NOT NEEDED PinotRealtimeSegmentManager.handleDataDeleted: {}", dataPath);
//    processPropertyStoreChange(dataPath);
//    }

  private boolean isLeader() {
    return _helixManager.isLeader();
  }

//    private void processPropertyStoreChange(String path) {
//      try {
//        LOGGER.info("Processing change notification for path: {}", path);
//        refreshWatchers(path);
//
//        if (isLeader()) {
        // Optimize as follows
      /*
      if (path.equals(CONTROLLER_LEADER_CHANGE)) {
        onBecomeLeader();
      } else if (path.matches(REALTIME_TABLE_CONFIG_PROPERTY_STORE_PATH_PATTERN)) {
        onAddingNewTable();
      } else if (path.matches(REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN)) {
        onSegmentChange();
      }
      */
//          if (path.matches(REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN) ||
//              path.matches(REALTIME_TABLE_CONFIG_PROPERTY_STORE_PATH_PATTERN) ||
//              path.equals(CONTROLLER_LEADER_CHANGE)) {
//            assignRealtimeSegmentsToServerInstancesIfNecessary();
//          }
//        } else {
//          LOGGER.info("Not the leader of this cluster, ignoring realtime segment property store change.");
//        }
//      } catch (Exception e) {
//        LOGGER.error("Caught exception while processing change for path {}", path, e);
//        Utils.rethrowException(e);
//      }
//    }

  public void onBecomeLeader() {
    if (isLeader()) {
      if (!_amILeader) {
        // We were not leader before, now we are.
        _amILeader = true;
        LOGGER.info("Do things we need to do when we get leadership");
        // Scanning tables to check for incomplete table additions is optional if we make table addtition operations
        // idempotent.The user can retry the failed operation and it will work.
        //
        // Go through all partitions of all tables that have LL configured, and check that they have as many
        // segments in CONSUMING state in Idealstate as there are kafka partitions.
        completeCommittingSegments();
      } else {
        // We already had leadership, nothing to do.
        LOGGER.info("Already leader. Duplicate notification");
      }
    } else {
      _amILeader = false;
      LOGGER.info("Lost leadership");
    }
  }

  private void completeCommittingSegments() {
    // Check all tables here.
    completeCommittingSegments(ValidationConstants.REALTIME_TABLE_NAME);
  }

  private void completeCommittingSegments(String realtimeTableName) {
    final int nPartitions = getNumPartitions(realtimeTableName);
    final List<Instance> instances = getNewSegmentInstances(realtimeTableName);
    final List<String> instanceNames = new ArrayList<>(instances.size());
    for (Instance instance : instances) {
      instanceNames.add(instance.toInstanceId());
    }
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, realtimeTableName);
    Set<String> segmentNamesIS = idealState.getPartitionSet();
    List<ZNRecord> psSegments = _propertyStore.getChildren(SEGMENTS_PATH + "/" + realtimeTableName, null, 0);
    final List<RealtimeSegmentNameHolder> segNameHolders = new ArrayList<>(psSegments.size());
    for (ZNRecord segment : psSegments) {
      segNameHolders.add(new RealtimeSegmentNameHolder(segment.getId()));
    }

    Collections.sort(segNameHolders, Collections.reverseOrder());
    int curPartition = nPartitions-1;
    final int nSegments = segNameHolders.size();

    for (int i = 0; i < nSegments; i++) {
      final RealtimeSegmentNameHolder holder = segNameHolders.get(i);
      final String segmentName = holder.getName();
      if (holder.getKafkaPartition() == curPartition) {
        if (!segmentNamesIS.contains(segmentName)) {
          LOGGER.info("{}:Repairing segment for partition {}. Segment {} not found in idealstate", realtimeTableName, curPartition, segmentName);
          RealtimeSegmentNameHolder prevHolder = null;
          if (i < nSegments-1) {
            prevHolder = segNameHolders.get(i+1);
          }
          final String prevSegmentName = prevHolder == null ? null : prevHolder.getName();
          HelixHelper.updateIdealState(_helixManager, realtimeTableName, new Function<IdealState, IdealState>() {
            @Override
            public IdealState apply(IdealState idealState) {
              return DummyStateModel
                  .updateForNewRealtimeSegment(idealState, instanceNames, prevSegmentName, segmentName);
            }
          }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
        }
      }
      if (curPartition == 0) {
        break;
      }
      curPartition--;
    }
  }

  private int getNumPartitions(String realtimeTableName) {
    return ValidationConstants.NUM_KAFKA_PARTITIONS;
  }

//    private void onAddingNewTable() {
    // Nothing to do here, because the controller is the one that alters the PROPERTYSTORE.
//    }

//    private void assignRealtimeSegmentsToServerInstancesIfNecessary() {
//      LOGGER.info("assignRealtimeSegmentsToServerInstancesIfNecessary()");
//    }

  // We call this when the table config changes to add a realtime table.
//    private void refreshWatchers(String path) {
//      LOGGER.info("refreshWatchers(" + path + ")");
//    List<Stat> stats = new ArrayList<>();
//    List<ZNRecord> tableConfigs = _propertyStore.getChildren(TABLE_CONFIG, stats, 0);
//
//    if (tableConfigs == null) {
//      return;
//    }

//    for (ZNRecord tableConfig : tableConfigs) {
//      try {
//        AbstractTableConfig abstractTableConfig = AbstractTableConfig.fromZnRecord(tableConfig);
//        if (abstractTableConfig.isRealTime())
//      {
//        final String realtimeTable = _realtimeTableName;  // Need to derive from the table configs.
//        String realtimeSegmentsPathForTable = _propertyStorePath + SEGMENTS_PATH + "/" + realtimeTable;
//
//        LOGGER.info("Setting data/s comchild changes watch for real-time table '{}'", realtimeTable);
//          _zkClient.subscribeDataChanges(realtimeSegmentsPathForTable, this);
//          _zkClient.subscribeChildChanges(realtimeSegmentsPathForTable, this);

//        List<String> childNames = _propertyStore.getChildNames(SEGMENTS_PATH + "/" + realtimeTable, 0);
//
//        if (childNames != null && !childNames.isEmpty()) {
//          for (String segmentName : childNames) {
//            String segmentPath = realtimeSegmentsPathForTable + "/" + segmentName;
//            LLRealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
//                getRealtimeSegmentZKMetadata(_propertyStore, _realtimeTableName, segmentName);
//            if (realtimeSegmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.IN_PROGRESS) {
//              LOGGER.info("Setting data change watch for real-time segment currently being consumed: {}", segmentPath);
//                _zkClient.subscribeDataChanges(segmentPath, this);
//            }
//          }
//        }
//      }
//      }
//      catch (JSONException e) {
//        LOGGER.error("Caught exception while reading table config", e);
//      } catch (IOException e) {
//        LOGGER.error("Caught exception while setting change listeners for realtime tables/segments", e);
//      }
//    }
//    }
}
