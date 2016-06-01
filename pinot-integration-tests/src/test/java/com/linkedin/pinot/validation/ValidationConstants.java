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
import java.util.List;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.api.pojos.Instance;


public class ValidationConstants {
  public static final String CLUSTER_NAME = "ValidationCluster";
  public static final String ZK_ADDR = "localhost:2181";
  public static final String KAFKA_TOPIC = "SomeTopic";

  public static final String TABLE_NAME = "ValidationTable";
  public static final String REALTIME_TABLE_NAME = TABLE_NAME + "_REALTIME";
  public static final int NUM_KAFKA_PARTITIONS = 1;
  public static final int NUM_REPLICAS = 3;
  public static final int NUM_SERVERS = 3;

  public static final String SERVER_HOST_NAME = "localhost";
  public static final String SERVER_TAG = "SomeTag";

  public static final long MIN_CONSUME_TIME_MS = 60000; // One minute + a few random seconds
  public static final long CONSUME_TIME_VARIATION_MS = 3000; // 3 seconds

  public static final long MAX_HOLD_TIME_MS = 3000;  // Max time servers will back-off before retransmit during commit
  public static final long MAX_SEGMENT_COMMIT_TIME_MS = 15000;  // Time taken to commit a segment

  // max time allowed to consume further if we get a ONLINE while in CONSUMED state on server
  public static final long MAX_CATCHUP_ON_CONSUME_TO_ONLINE_MS = 5000L;

  public static final int serverPortStart = 7001;

  // Generates all instance names that host the realtime resource for this experiment
  public static List<Instance> generateInstanceNames() {
    int port = serverPortStart;
    List<Instance> result = new ArrayList<Instance>(4);
    for (int i = 0; i < NUM_SERVERS; i++) {
      Instance instance = new Instance(SERVER_HOST_NAME, Integer.toString(port++), CommonConstants.Helix.SERVER_INSTANCE_TYPE, SERVER_TAG);
      result.add(instance);
    }
    return result;
  }

}
