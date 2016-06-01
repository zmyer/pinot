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

import java.util.List;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.restlet.routing.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.restlet.PinotRestletApplication;
import com.linkedin.pinot.common.utils.ZkUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;


//public class DummyController implements IZkDataListener, IZkChildListener {
public class DummyController {
  public static final Logger LOGGER = LoggerFactory.getLogger(DummyController.class);

//  private static final String CONTROLLER_LEADER_CHANGE = "CONTROLLER_LEADER_CHANGE";
//  private static final String TABLE_CONFIG = "/CONFIGS/TABLE";
  private static final String SEGMENTS_PATH = "/SEGMENTS";
//  private static final String REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN =
//      ".*/SEGMENTS/.*_REALTIME|.*/SEGMENTS/.*_REALTIME/.*";
//  private static final String REALTIME_TABLE_CONFIG_PROPERTY_STORE_PATH_PATTERN = ".*/TABLE/.*REALTIME";

  private final String _controllerPortStr;
  private final String _clusterName;
  private final String _zkAddr;
  private final String _instanceName;
  private final HelixManager _helixManager;
//  private final PinotHelixResourceManager _helixResourceManager;
//  private final String _propertyStorePath;
//  private final String _tableConfigPath;
//  private final ZkClient _zkClient;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final HelixAdmin _helixAdmin;

  private final String _realtimeTableName = ValidationConstants.REALTIME_TABLE_NAME;
//  private boolean amILeader = false;
  private final AdminApiService _adminApiService;
  private final LLRealtimeSegmentManager _segmentManager;

  // Also starts the controller.
  private DummyController(int controllerPort) {

    _adminApiService = new AdminApiService();
    _controllerPortStr = Integer.toString(controllerPort);
    ControllerConf conf = ControllerTestUtils.getDefaultControllerConfiguration();
    conf.setControllerPort(_controllerPortStr);

    // Setup
    _clusterName = ValidationConstants.CLUSTER_NAME;
    _zkAddr = ValidationConstants.ZK_ADDR;

    /*
    _zkClient = new ZkClient(_zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    _zkClient.waitUntilConnected();
    */

    conf.setZkStr(_zkAddr);
    _instanceName = "localhost_" + _controllerPortStr;
    String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkAddr);
    _helixManager = HelixSetupUtils.setup(_clusterName, helixZkURL, _instanceName);
    _helixAdmin = _helixManager.getClusterManagmentTool();
//    HelixDataAccessor helixDataAccessor = helixZkManager.getHelixDataAccessor();
//    PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
//    _helixResourceManager = new PinotHelixResourceManager(conf);
    _propertyStore = ZkUtils.getZkPropertyStore(_helixManager, _clusterName, false /* No recursive Watches */);
    // Add realtime watchers
//    _propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, _clusterName);
//    _tableConfigPath = _propertyStorePath + TABLE_CONFIG;
    LLRealtimeSegmentManager.create(_helixAdmin, _helixManager, _propertyStore);
    _segmentManager = LLRealtimeSegmentManager.getInstance();
    SegmentFinalizer.create(_helixManager, _segmentManager);
    /*
    try {
      _zkClient.create(_tableConfigPath + "/" + ValidationConstants.TABLE_NAME, new ZNRecord("Dummy entry"),
          CreateMode.PERSISTENT);
    } catch (Exception e) {
      if (e.getCause() instanceof  KeeperException.NodeExistsException) {
        LOGGER.info("Resource already exists in helix");
      } else {
        throw e;
      }
    }
    */
    oneTimeSetup();

    _helixManager.addControllerListener(new ControllerChangeListener() {
      @Override
      public void onControllerChange(NotificationContext changeContext) {
        LOGGER.info("onControllerChange:" + this + ":" + changeContext + ":" + changeContext.getEventName() + ":"
            + changeContext.getType());
//        processPropertyStoreChange(CONTROLLER_LEADER_CHANGE);
        // Notify all that want a controller leader change notification
        _segmentManager.onBecomeLeader();
      }
    });

    // XXX The controller need not set any watches.
    // It can create segments on table creation, and also add to the IDEALSTATE?
//    _zkClient.subscribeChildChanges(_tableConfigPath, this);
//    _zkClient.subscribeDataChanges(_tableConfigPath, this);
  }

  public static void main(String[] args)
      throws Exception {
    int controllerPort = 11984;
    if (args.length > 0) {
      try {
        controllerPort = Integer.valueOf(args[0]);
      } catch (Exception e) {
        LOGGER.warn("Could not parse " + args[0] + " as controller port");
      }
    }

    DummyController controller = new DummyController(controllerPort);

    LOGGER.info("Controller " + controllerPort + " started");
    while (true) {
      Thread.sleep(10000L);
    }
  }

    private void oneTimeSetup() {
      // For this experiment, adding server instances is a one-time setup. Otherwise, it is done by active controllers
      // and we don't need any watches.
      addServerInstances(ValidationConstants.NUM_SERVERS);

      // For this experiment, adding a state model is a one time setup. Otherwise, it is done at cluster setup time?
      DummyStateModel stateModel = new DummyStateModel();
      StateModelDefinition definition = stateModel.defineStateModel();
      try {
        _helixAdmin.addStateModelDef(_clusterName, DummyStateModel.getName(), definition);
      } catch (HelixException e) {
        if (e.getMessage().contains("already exists")) {
          LOGGER.warn("State model already exists? ", e);
        } else {
          throw e;
        }
      }

      // Admin API
      _adminApiService.start(Integer.valueOf(_controllerPortStr));

//      _segmentManager.addTable(_realtimeTableName, ValidationConstants.generateInstanceNames());
    }
  private void addServerInstances(int nServers) {
    List<Instance> instances = ValidationConstants.generateInstanceNames();
    List<String> helixInstances = HelixHelper.getAllInstances(_helixAdmin, _clusterName);
    for (Instance instance : instances) {
      if (helixInstances.contains(instance.toInstanceId())) {
        LOGGER.info("Instance " + instance.toInstanceId() + " already exists");
      } else {
        _helixAdmin.addInstance(_clusterName, instance.toInstanceConfig());
      }
    }
  }

  //  private LLRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableName, String segmentName) {
//    return new LLRealtimeSegmentZKMetadata(propertyStore.get(ZKMetadataProvider.constructPropertyStorePathForSegment(
//        tableName, segmentName), null, AccessOption.PERSISTENT));
//  }

  // This is there for the experiment purpose. In the real world, we will already have this class
  // and we just need to add the new restlet resources while configuring router.
  public static class DummyControlerAPIs extends PinotRestletApplication {
    @Override
    protected void configureRouter(Router router) {
      attachRoutesForClass(router, SegmentConsumedResource.class);
      attachRoutesForClass(router, SegmentCommitResource.class);
      attachRoutesForClass(router, DummyTableAddResource.class);
    }
  }

  public static class AdminApiService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdminApiService.class);
    Component adminApiComponent;
    boolean started = false;

    public AdminApiService() {
      adminApiComponent = new Component();
    }

    public boolean start(int httpPort) {
      if (httpPort <= 0) {
        LOGGER.warn("Invalid admin API port: {}. Not starting admin service", httpPort);
        return false;
      }
      adminApiComponent.getServers().add(Protocol.HTTP, httpPort);
      adminApiComponent.getClients().add(Protocol.FILE);
      adminApiComponent.getClients().add(Protocol.JAR);
      adminApiComponent.getClients().add(Protocol.WAR);

      DummyControlerAPIs adminEndpointApplication = new DummyControlerAPIs();
      final Context applicationContext = adminApiComponent.getContext().createChildContext();
      adminEndpointApplication.setContext(applicationContext);
//      applicationContext.getAttributes().put(ServerInstance.class.toString(), serverInstance);

      adminApiComponent.getDefaultHost().attach(adminEndpointApplication);
      LOGGER.info("Will start admin API endpoint on port {}", httpPort);
      try {
        adminApiComponent.start();
        started = true;
        return true;
      } catch (Exception e) {
        LOGGER.warn("Failed to start admin service. Continuing with errors", e);
        return false;
      }
    }

    public void stop() {
      if (!started) {
        return;
      }
      try {
        adminApiComponent.stop();
      } catch (Exception e) {
        LOGGER.warn("Failed to stop admin API component. Continuing with errors", e);
      }
    }
  }
}
