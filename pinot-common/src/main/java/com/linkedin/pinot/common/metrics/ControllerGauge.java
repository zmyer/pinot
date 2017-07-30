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
package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;


/**
 * Controller gauges.
 */
public enum ControllerGauge implements AbstractMetrics.Gauge {
  NUMBER_OF_REPLICAS("replicas", false), // Number of complete replicas of table in external view containing
                                         // all segments online in ideal state
  PERCENT_OF_REPLICAS("percent", false), // Percentage of complete online replicas in external view as compared
                                         // to replicas in ideal state
  SEGMENTS_IN_ERROR_STATE("segments", false),
  PERCENT_SEGMENTS_AVAILABLE("segments", false), // Percentage of segments with at least one online replica in external view
                                          // as compared to total number of segments in ideal state
  IDEALSTATE_ZNODE_SIZE("idealstate", false),
  REALTIME_TABLE_COUNT("TableCount", true),
  OFFLINE_TABLE_COUNT("TableCount", true);

  private final String gaugeName;
  private final String unit;
  private final boolean global;

  ControllerGauge(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.gaugeName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getGaugeName() {
    return gaugeName;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  /**
   * Returns true if the gauge is global (not attached to a particular resource)
   *
   * @return true if the gauge is global
   */
  @Override
  public boolean isGlobal() {
    return global;
  }
}
