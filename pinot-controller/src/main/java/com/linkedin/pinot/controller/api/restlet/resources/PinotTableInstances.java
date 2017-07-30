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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.alibaba.fastjson.JSONArray;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTableInstances extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableInstances.class);
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableInstances() throws IOException {
    baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "schemasTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
  }

  @Override
  @Get
  public Representation get() {
    final String tableName = (String) getRequest().getAttributes().get(TABLE_NAME);
    final String type = getQueryValue(TABLE_TYPE);
    if (tableName == null) {
      String error = new String("Error: Table " + tableName + " not found.");
      LOGGER.error(error);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(error);
    }

    try {
      return getTableInstances(tableName, type);
    } catch (Exception e) {
      LOGGER.error("Caught exception fetching instances for table ", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_INSTANCES_GET_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }

  }

  @HttpVerb("get")
  @Summary("Lists table instances for a given table")
  @Tags({"instance", "table"})
  @Paths({
      "/tables/{tableName}/instances"
  })
  private Representation getTableInstances(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to list instances", required = true)
      String tableName, String type)
      throws JSONException, IOException {
    JSONObject ret = new JSONObject();
    ret.put("tableName", tableName);
    JSONArray brokers = new JSONArray();
    JSONArray servers = new JSONArray();

    if (type == null || type.toLowerCase().equals("broker")) {

      if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
        JSONObject e = new JSONObject();
        e.put("tableType", "offline");
        JSONArray a = new JSONArray();
        for (String ins : _pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.OFFLINE)) {
          a.add(ins);
        }
        e.put("instances", a);
        brokers.add(e);
      }

      if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        JSONObject e = new JSONObject();
        e.put("tableType", "realtime");
        JSONArray a = new JSONArray();
        for (String ins : _pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.REALTIME)) {
          a.add(ins);
        }
        e.put("instances", a);
        brokers.add(e);
      }
    }

    if (type == null || type.toLowerCase().equals("server")) {
      if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
        JSONObject e = new JSONObject();
        e.put("tableType", "offline");
        JSONArray a = new JSONArray();
        for (String ins : _pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.OFFLINE)) {
          a.add(ins);
        }
        e.put("instances", a);
        servers.add(e);
      }

      if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        JSONObject e = new JSONObject();
        e.put("tableType", "realtime");
        JSONArray a = new JSONArray();
        for (String ins : _pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)) {
          a.add(ins);
        }
        e.put("instances", a);
        servers.add(e);
      }
    }

    ret.put("brokers", brokers);
    ret.put("server", servers);
    return new StringRepresentation(ret.toString());
  }
}
