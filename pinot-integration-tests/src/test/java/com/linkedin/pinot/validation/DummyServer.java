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

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkUtils;
import com.linkedin.pinot.controller.api.pojos.Instance;


public class DummyServer {
  public static final Logger LOGGER = LoggerFactory.getLogger(DummyServer.class);

  private final int _port;
  private final Instance _instance;
  private final String _instanceName;
  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final String _clusterName = ValidationConstants.CLUSTER_NAME;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private DummyServer(int port) throws Exception {
    _port = port;
    _instance = new Instance(ValidationConstants.SERVER_HOST_NAME, Integer.toString(_port),
        CommonConstants.Helix.SERVER_INSTANCE_TYPE, ValidationConstants.SERVER_TAG);
    _instanceName = _instance.toInstanceId();
    _helixManager = HelixManagerFactory
        .getZKHelixManager(_clusterName, _instanceName, InstanceType.PARTICIPANT, ValidationConstants.ZK_ADDR);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    LLRealtimeSegmentStateModelFactory stateModelFactory = new LLRealtimeSegmentStateModelFactory(_instance.toInstanceId(), _helixManager);
    ControllerLeaderLocator.create(_helixManager);
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    _propertyStore = ZkUtils.getZkPropertyStore(_helixManager, _clusterName, false /* No recursive Watches */);
    stateModelFactory.setPropertyStore(_propertyStore);
    // Kick off state transitions only after we have set the propertystore handle (which needs a connect to happen).
    // Don't know what will happen if transitions come before this.
    stateMachineEngine.registerStateModelFactory(LLRealtimeSegmentStateModelFactory.getStateModelDef(), stateModelFactory);
  }

  public static void main(String[] args) throws Exception {
    int serverPort = 7001;

    if (args.length > 0) {
      try {
        serverPort = Integer.valueOf(args[0]);
      } catch (Exception e) {
        LOGGER.warn("Could not parse " + args[0] + " as controller port");
      }
    }

    DummyServer server = new DummyServer(serverPort);

    LOGGER.info("Server " + serverPort + " started");
    while(true) {
      Thread.sleep(10000L);
    }
  }
}
