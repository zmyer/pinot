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
package com.linkedin.pinot.common.config;

import java.util.Set;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.json.JSONException;
import org.json.JSONObject;


@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableTaskConfig {
  private static final String ENABLED_TASK_TYPES_KEY = "enabledTaskTypes";

  private Set<String> _enabledTaskTypes;

  public void setEnabledTaskTypes(Set<String> enabledTaskTypes) {
    _enabledTaskTypes = enabledTaskTypes;
  }

  public Set<String> getEnabledTaskTypes() {
    return _enabledTaskTypes;
  }

  @JsonIgnore
  public boolean isTaskTypeEnabled(String taskType) {
    return _enabledTaskTypes.contains(taskType);
  }

  @Override
  public String toString() {
    JSONObject jsonConfig = new JSONObject();
    try {
      jsonConfig.put(ENABLED_TASK_TYPES_KEY, _enabledTaskTypes);
      return jsonConfig.toString(2);
    } catch (JSONException e) {
      return e.toString();
    }
  }
}
