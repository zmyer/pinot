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
package com.linkedin.pinot.broker.routing;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.broker.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.broker.routing.PercentageBasedRoutingTableSelector;
import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.broker.routing.builder.BalancedRandomRoutingTableBuilder;
import com.linkedin.pinot.broker.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TableConfig.Builder;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.transport.common.SegmentIdSet;


public class RandomRoutingTableTest {

  @Test
  public void testHelixExternalViewBasedRoutingTable()
      throws Exception {
    URL resourceUrl = getClass().getClassLoader().getResource("SampleExternalView.json");
    Assert.assertNotNull(resourceUrl);
    String fileName = resourceUrl.getFile();
    String tableName = "testTable_OFFLINE";
    InputStream evInputStream = new FileInputStream(fileName);
    ZNRecordSerializer znRecordSerializer = new ZNRecordSerializer();
    ZNRecord externalViewRecord = (ZNRecord) znRecordSerializer.deserialize(IOUtils.toByteArray(evInputStream));
    int totalRuns = 10000;
    RoutingTableBuilder routingStrategy = new BalancedRandomRoutingTableBuilder(10);
    HelixExternalViewBasedRouting routingTable =
        new HelixExternalViewBasedRouting(null, new PercentageBasedRoutingTableSelector(), null,
            new BaseConfiguration());

    //routingTable.setSmallClusterRoutingTableBuilder(routingStrategy);

    ExternalView externalView = new ExternalView(externalViewRecord);

    routingTable.markDataResourceOnline(generateTableConfig(tableName), externalView, getInstanceConfigs(externalView));

    double[] globalArrays = new double[9];

    for (int numRun = 0; numRun < totalRuns; ++numRun) {
      RoutingTableLookupRequest request = new RoutingTableLookupRequest(tableName, Collections.<String>emptyList(), null);
      Map<ServerInstance, SegmentIdSet> serversMap = routingTable.findServers(request);
      TreeSet<ServerInstance> serverInstances = new TreeSet<ServerInstance>(serversMap.keySet());

      int i = 0;

      double[] arrays = new double[9];
      for (ServerInstance serverInstance : serverInstances) {
        globalArrays[i] += serversMap.get(serverInstance).getSegments().size();
        arrays[i++] = serversMap.get(serverInstance).getSegments().size();
      }
      for (int j = 0; i < arrays.length; ++j) {
        Assert.assertTrue(arrays[j] / totalRuns <= 31);
        Assert.assertTrue(arrays[j] / totalRuns >= 28);
      }
//      System.out.println(Arrays.toString(arrays) + " : " + new StandardDeviation().evaluate(arrays) + " : " + new Mean().evaluate(arrays));
    }
    for (int i = 0; i < globalArrays.length; ++i) {
      Assert.assertTrue(globalArrays[i] / totalRuns <= 31);
      Assert.assertTrue(globalArrays[i] / totalRuns >= 28);
    }
//    System.out.println(Arrays.toString(globalArrays) + " : " + new StandardDeviation().evaluate(globalArrays) + " : "
//        + new Mean().evaluate(globalArrays));
  }

  /**
   * Returns a list of configs containing all instances in the external view.
   * @param externalView From which to extract the instance list from.
   * @return Instance Config list
   */
  private List<InstanceConfig> getInstanceConfigs(ExternalView externalView) {
    List<InstanceConfig> instanceConfigList = new ArrayList<>();
    Set<String> instanceSet = new HashSet<>();

    // Collect all unique instances
    for (String partitionName : externalView.getPartitionSet()) {
      for (String instance : externalView.getStateMap(partitionName).keySet()) {
        if (!instanceSet.contains(instance)) {
          instanceConfigList.add(new InstanceConfig(instance));
          instanceSet.add(instance);
        }
      }
    }

    return instanceConfigList;
  }
  
  private TableConfig generateTableConfig(String tableName) throws Exception {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    Builder builder = new TableConfig.Builder(tableType);
    builder.setTableName(tableName);
    return builder.build();
  }

}
