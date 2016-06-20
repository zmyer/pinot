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

import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.CustomModeISBuilder;


public class DummyStateModel {
  private static final String STATE_MODEL_NAME = "RealtimeSegmentStateModel";

  private static final String OFFLINE_STATE = "OFFLINE";
  private static final String CONSUMING_STATE = "CONSUMING";
  private static final String ONLINE_STATE = "ONLINE";
  private static final String DROPPED_STATE = "DROPPED";

  public StateModelDefinition defineStateModel() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);
    // Add states and their rank to indicate priority. Lower the rank higher the
    // priority
    builder.initialState(OFFLINE_STATE);

    builder.addState(ONLINE_STATE);
    builder.addState(CONSUMING_STATE);
    builder.addState(OFFLINE_STATE);
    builder.addState(DROPPED_STATE);
    // Set the initial state when the node starts

    // Add transitions between the states.
    builder.addTransition(CONSUMING_STATE, ONLINE_STATE);
    builder.addTransition(OFFLINE_STATE, CONSUMING_STATE);
    builder.addTransition(OFFLINE_STATE, ONLINE_STATE);
    builder.addTransition(CONSUMING_STATE, OFFLINE_STATE);
    builder.addTransition(ONLINE_STATE, OFFLINE_STATE);
    builder.addTransition(OFFLINE_STATE, DROPPED_STATE);

    // set constraints on states.
    // static constraint
    builder.dynamicUpperBound(ONLINE_STATE, "R");
    // dynamic constraint, R means it should be derived based on the replication
    // factor.
    builder.dynamicUpperBound(CONSUMING_STATE, "R");

    StateModelDefinition statemodelDefinition = builder.build();
    return statemodelDefinition;
  }

  public static String getName() {
    return STATE_MODEL_NAME;
  }

  public static String getConsumingStateName() {
    return CONSUMING_STATE;
  }

  public static String getOnlineStateName() {
    return ONLINE_STATE;
  }

  // Map of segmentName to list of instances that is supposed to host the segment.
  public static IdealState addOrUpdateRealtimeSegmentInIdealState(IdealState state, Map<String, List<String>> isEntryMap) {
    for (Map.Entry<String, List<String>> entry : isEntryMap.entrySet()) {
      final String segmentId = entry.getKey();
      final Map<String, String> stateMap = state.getInstanceStateMap(segmentId);
      // if the segment name already exists, clear it.
      if (stateMap != null) {
        stateMap.clear();
      }
      for (String instanceName : entry.getValue()) {
        state.setPartitionState(segmentId, instanceName, CONSUMING_STATE);
      }
    }
    return state;
  }

  public static IdealState updateForNewRealtimeSegment(IdealState idealState,
      List<String> newInstances, String oldSegmentName, String newSegmentName) {
    if (oldSegmentName != null) {
      // Update the old ones to be ONLINE
      Map<String, String> stateMap = idealState.getInstanceStateMap(oldSegmentName);
      for (String key : stateMap.keySet()) {
        stateMap.put(key, ONLINE_STATE);
      }
    }
    Map<String, String> stateMap = idealState.getInstanceStateMap(newSegmentName);
    if (stateMap != null) {
      stateMap.clear();
    }
    for (String instance : newInstances) {
      idealState.setPartitionState(newSegmentName, instance, CONSUMING_STATE);
    }

    return idealState;
  }

  public static IdealState buildEmptyIdealStateFor(String tableName, int numCopies, HelixAdmin helixAdmin,
      String helixClusterName) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(tableName);
    final int replicas = numCopies;
    customModeIdealStateBuilder
        .setStateModel(STATE_MODEL_NAME)
        .setNumPartitions(0).setNumReplica(replicas).setMaxPartitionsPerNode(1);
    final IdealState idealState = customModeIdealStateBuilder.build();
    idealState.setInstanceGroupTag(tableName);
    return idealState;
  }
}
