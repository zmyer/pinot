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
package com.linkedin.pinot.core.query.aggregation.groupby;

import com.linkedin.pinot.common.response.broker.GroupByResult;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.Nonnull;


/**
 * The <code>AggregationGroupByTrimmingService</code> class provides trimming service for aggregation group-by query.
 */
// TODO: revisit the trim algorithm, implement trim on all Object but not only for Comparable.
public class AggregationGroupByTrimmingService {
  public static final String GROUP_KEY_DELIMITER = "\t";

  private final int _numAggregationFunctions;
  private final boolean[] _minOrders;

  private final int _groupByTopN;
  // To keep the precision, _trimSize is the larger of (_groupByTopN * 5) or 5000.
  private final int _trimSize;
  // To trigger the trimming, number of groups should be larger than _trimThreshold which is (_trimSize * 4).
  private final int _trimThreshold;

  public AggregationGroupByTrimmingService(@Nonnull AggregationFunctionContext[] aggregationFunctionContexts,
      int groupByTopN) {
    _numAggregationFunctions = aggregationFunctionContexts.length;
    _minOrders = new boolean[_numAggregationFunctions];
    for (int i = 0; i < _numAggregationFunctions; i++) {
      String aggregationFunctionName = aggregationFunctionContexts[i].getAggregationFunction().getName();
      if (aggregationFunctionName.equals(AggregationFunctionFactory.AggregationFunctionType.MIN.getName())
          || aggregationFunctionName.equals(AggregationFunctionFactory.AggregationFunctionType.MINMV.getName())) {
        _minOrders[i] = true;
      }
    }
    _groupByTopN = groupByTopN;
    _trimSize = Math.max(_groupByTopN * 5, 5000);
    _trimThreshold = _trimSize * 4;
  }

  public AggregationGroupByTrimmingService(@Nonnull AggregationFunction[] aggregationFunctions, int groupByTopN) {
    _numAggregationFunctions = aggregationFunctions.length;
    _minOrders = new boolean[_numAggregationFunctions];
    for (int i = 0; i < _numAggregationFunctions; i++) {
      String aggregationFunctionName = aggregationFunctions[i].getName();
      if (aggregationFunctionName.equals(AggregationFunctionFactory.AggregationFunctionType.MIN.getName())
          || aggregationFunctionName.equals(AggregationFunctionFactory.AggregationFunctionType.MINMV.getName())) {
        _minOrders[i] = true;
      }
    }
    _groupByTopN = groupByTopN;
    _trimSize = Math.max(_groupByTopN * 5, 5000);
    _trimThreshold = _trimSize * 4;
  }

  /**
   * Given a map from group key to the intermediate results for multiple aggregation functions, trim the results to
   * desired size and put them into a list of maps from group key to intermediate result for each aggregation function.
   */
  @Nonnull
  public List<Map<String, Object>> trimIntermediateResultsMap(@Nonnull Map<String, Object[]> intermediateResultsMap) {
    List<Map<String, Object>> trimmedResults = new ArrayList<>(_numAggregationFunctions);
    for (int i = 0; i < _numAggregationFunctions; i++) {
      trimmedResults.add(new HashMap<String, Object>());
    }

    if (intermediateResultsMap.isEmpty()) {
      return trimmedResults;
    }

    if (intermediateResultsMap.size() > _trimThreshold) {
      // Need to trim.

      // Construct the priority queues.
      @SuppressWarnings("unchecked")
      PriorityQueue<GroupKeyResultPair>[] priorityQueues = new PriorityQueue[_numAggregationFunctions];
      Object[] sampleResults = intermediateResultsMap.values().iterator().next();
      for (int i = 0; i < _numAggregationFunctions; i++) {
        if (sampleResults[i] instanceof Comparable) {
          priorityQueues[i] = new PriorityQueue<>(_trimSize + 1, getGroupKeyResultPairComparator(_minOrders[i]));
        }
      }

      // Fill results into the priority queues.
      for (Map.Entry<String, Object[]> entry : intermediateResultsMap.entrySet()) {
        String groupKey = entry.getKey();
        Object[] intermediateResults = entry.getValue();
        for (int i = 0; i < _numAggregationFunctions; i++) {
          PriorityQueue<GroupKeyResultPair> priorityQueue = priorityQueues[i];
          if (priorityQueue == null) {
            trimmedResults.get(i).put(groupKey, intermediateResults[i]);
          } else {
            GroupKeyResultPair newValue = new GroupKeyResultPair(groupKey, (Comparable) intermediateResults[i]);
            addToPriorityQueue(priorityQueue, newValue, _trimSize);
          }
        }
      }

      // Fill trimmed results into the maps.
      for (int i = 0; i < _numAggregationFunctions; i++) {
        PriorityQueue<GroupKeyResultPair> priorityQueue = priorityQueues[i];
        if (priorityQueue != null) {
          while (!priorityQueue.isEmpty()) {
            GroupKeyResultPair groupKeyResultPair = priorityQueue.poll();
            trimmedResults.get(i).put(groupKeyResultPair._groupKey, groupKeyResultPair._result);
          }
        }
      }
    } else {
      // No need to trim.
      for (Map.Entry<String, Object[]> entry : intermediateResultsMap.entrySet()) {
        String groupKey = entry.getKey();
        Object[] intermediateResults = entry.getValue();
        for (int i = 0; i < _numAggregationFunctions; i++) {
          trimmedResults.get(i).put(groupKey, intermediateResults[i]);
        }
      }
    }

    return trimmedResults;
  }

  /**
   * Given an array of maps from group key to final result for each aggregation function, trim the results to topN size.
   */
  @SuppressWarnings("unchecked")
  @Nonnull
  public List<GroupByResult>[] trimFinalResults(@Nonnull Map<String, Comparable>[] finalResultMaps) {
    List<GroupByResult>[] trimmedResults = new List[_numAggregationFunctions];

    for (int i = 0; i < _numAggregationFunctions; i++) {
      LinkedList<GroupByResult> groupByResults = new LinkedList<>();
      trimmedResults[i] = groupByResults;
      Map<String, Comparable> finalResultMap = finalResultMaps[i];
      if (finalResultMap.isEmpty()) {
        continue;
      }

      // Construct the priority queues.
      PriorityQueue<GroupKeyResultPair> priorityQueue =
          new PriorityQueue<>(_groupByTopN + 1, getGroupKeyResultPairComparator(_minOrders[i]));

      // Fill results into the priority queues.
      for (Map.Entry<String, Comparable> entry : finalResultMap.entrySet()) {
        String groupKey = entry.getKey();
        Comparable finalResult = entry.getValue();

        GroupKeyResultPair newValue = new GroupKeyResultPair(groupKey, finalResult);
        addToPriorityQueue(priorityQueue, newValue, _groupByTopN);
      }

      // Fill trimmed results into the list.
      while (!priorityQueue.isEmpty()) {
        GroupKeyResultPair groupKeyResultPair = priorityQueue.poll();
        GroupByResult groupByResult = new GroupByResult();
        // Do not remove trailing empty strings.
        String[] groupKeys = groupKeyResultPair._groupKey.split(GROUP_KEY_DELIMITER, -1);
        groupByResult.setGroup(Arrays.asList(groupKeys));
        groupByResult.setValue(AggregationFunctionUtils.formatValue(groupKeyResultPair._result));
        groupByResults.addFirst(groupByResult);
      }
    }

    return trimmedResults;
  }

  /**
   * Helper method to add a value into priority queue:
   * <ul>
   *   <li> If the queue size is less than maxQueueSize, then the element is simply added into the priority queue. </li>
   *   <li> If the queue size is >= maxQueueSize, then the given value is compared against the top of priority queue.
   *        If value is 'better' than the top, then it is inserted into the queue, and the top element is removed,
   *        to keep the size of the queue bounded. </li>
   *   <li> If max queue size is <= 0, then simply returns. Caller is responsible for ensuring a valid value of
   *        max queue size is provided. </li>
   * </ul>
   * @param priorityQueue Priority queue into which the element needs to be inserted.
   * @param value Value to be inserted.
   * @param maxQueueSize Max allowed queue size.
   */
  private void addToPriorityQueue(PriorityQueue<GroupKeyResultPair> priorityQueue, GroupKeyResultPair value, int maxQueueSize) {
    // If maxQueueSize is zero, then simply return. Caller should check the validity of maxQueueSize.
    if (maxQueueSize <= 0) {
      return;
    }

    if (priorityQueue.size() >= maxQueueSize) {
      GroupKeyResultPair topValue = priorityQueue.peek();
      if (priorityQueue.comparator().compare(topValue, value) < 0) {
        priorityQueue.poll();
        priorityQueue.add(value);
      }
    } else {
      priorityQueue.add(value);
    }
  }

  private static class GroupKeyResultPair {
    public String _groupKey;
    public Comparable _result;

    public GroupKeyResultPair(@Nonnull String groupKey, @Nonnull Comparable result) {
      _groupKey = groupKey;
      _result = result;
    }
  }

  private static Comparator<GroupKeyResultPair> getGroupKeyResultPairComparator(boolean minOrder) {
    if (minOrder) {
      return new Comparator<GroupKeyResultPair>() {
        @SuppressWarnings("unchecked")
        @Override
        public int compare(GroupKeyResultPair o1, GroupKeyResultPair o2) {
          return o2._result.compareTo(o1._result);
        }
      };
    } else {
      return new Comparator<GroupKeyResultPair>() {
        @SuppressWarnings("unchecked")
        @Override
        public int compare(GroupKeyResultPair o1, GroupKeyResultPair o2) {
          return o1._result.compareTo(o2._result);
        }
      };
    }
  }
}
