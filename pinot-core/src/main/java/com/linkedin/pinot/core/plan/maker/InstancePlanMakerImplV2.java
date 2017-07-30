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
package com.linkedin.pinot.core.plan.maker;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.AggregationGroupByPlanNode;
import com.linkedin.pinot.core.plan.AggregationPlanNode;
import com.linkedin.pinot.core.plan.CombinePlanNode;
import com.linkedin.pinot.core.plan.GlobalPlanImplV0;
import com.linkedin.pinot.core.plan.InstanceResponsePlanNode;
import com.linkedin.pinot.core.plan.MetadataBasedAggregationPlanNode;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.SelectionPlanNode;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>InstancePlanMakerImplV2</code> class is the default implementation of {@link PlanMaker}.
 */
public class InstancePlanMakerImplV2 implements PlanMaker {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstancePlanMakerImplV2.class);

  // TODO: Fix the runtime trimming and add back the number of aggregation groups limit.
  // TODO: Need to revisit the runtime trimming solution. Current solution will remove group keys that should not be removed.
  // Limit on number of groups, beyond which results are truncated.
  // private static final String NUM_AGGR_GROUPS_LIMIT = "num.aggr.groups.limit";
  // private static final int DEFAULT_NUM_AGGR_GROUPS_LIMIT = 100_000;
  private final int _numAggrGroupsLimit = Integer.MAX_VALUE;

  /**
   * Default constructor.
   */
  public InstancePlanMakerImplV2() {
//    _numAggrGroupsLimit = DEFAULT_NUM_AGGR_GROUPS_LIMIT;
  }

  /**
   * Constructor for usage when client requires to pass {@link QueryExecutorConfig} to this class.
   * <ul>
   *   <li>Set limit on number of aggregation groups in query result.</li>
   * </ul>
   *
   * @param queryExecutorConfig query executor configuration.
   */
  public InstancePlanMakerImplV2(QueryExecutorConfig queryExecutorConfig) {
    // TODO: Read the limit on number of aggregation groups in query result from config.
    // _numAggrGroupsLimit = queryExecutorConfig.getConfig().getInt(NUM_AGGR_GROUPS_LIMIT, DEFAULT_NUM_AGGR_GROUPS_LIMIT);
    // LOGGER.info("Maximum number of allowed groups for group-by query results: '{}'", _numAggrGroupsLimit);
  }

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    // Aggregation query.
    if (brokerRequest.isSetAggregationsInfo()) {
      if (brokerRequest.isSetGroupBy()) {
        // Aggregation group-by query.
        return new AggregationGroupByPlanNode(indexSegment, brokerRequest, _numAggrGroupsLimit);
      } else {
          // Aggregation only query.
        if (isFitForMetadataBasedPlan(brokerRequest)) {
          return new MetadataBasedAggregationPlanNode(indexSegment, brokerRequest.getAggregationsInfo());
        } else {
          return new AggregationPlanNode(indexSegment, brokerRequest);
        }
      }
    }

    // Selection query.
    if (brokerRequest.isSetSelections()) {
      return new SelectionPlanNode(indexSegment, brokerRequest);
    }

    throw new UnsupportedOperationException("The query contains no aggregation or selection.");
  }

  @Override
  public Plan makeInterSegmentPlan(List<SegmentDataManager> segmentDataManagers, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs) {
    // TODO: pass in List<IndexSegment> directly.
    List<IndexSegment> indexSegments = new ArrayList<>(segmentDataManagers.size());
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      indexSegments.add(segmentDataManager.getSegment());
    }
    BrokerRequestPreProcessor.preProcess(indexSegments, brokerRequest);

    List<PlanNode> planNodes = new ArrayList<>();
    for (IndexSegment indexSegment : indexSegments) {
      planNodes.add(makeInnerSegmentPlan(indexSegment, brokerRequest));
    }
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, brokerRequest, executorService, timeOutMs);

    return new GlobalPlanImplV0(new InstanceResponsePlanNode(combinePlanNode));
  }

  /**
   * Helper method to identify if query is fit to be be served purely based on metadata.
   * Currently only count(*) queries without any filters are supported.
   *
   * TODO: Add support for queries other than count(*)
   *
   * @param brokerRequest Broker request
   * @return True if query can be served using metadata, false otherwise.
   */
  private boolean isFitForMetadataBasedPlan(BrokerRequest brokerRequest) {
    FilterQuery filterQuery = brokerRequest.getFilterQuery();
    if (filterQuery != null) {
      return false;
    }

    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo != null && aggregationsInfo.size() == 1 && !brokerRequest.isSetGroupBy()) {
      if (aggregationsInfo.get(0)
          .getAggregationType()
          .equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.COUNT.getName())) {
        return true;
      }
    }
    return false;
  }
}
