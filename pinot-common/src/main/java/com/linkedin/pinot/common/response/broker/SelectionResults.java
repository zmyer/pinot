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
package com.linkedin.pinot.common.response.broker;

import java.io.Serializable;
import java.util.List;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;


@JsonPropertyOrder({"columns", "results"})
public class SelectionResults {
  private List<String> _columns;
  private List<Serializable[]> _rows;

  @JsonCreator
  public SelectionResults(@JsonProperty("columns") List<String> columns,
      @JsonProperty("results") List<Serializable[]> results) {
    _columns = columns;
    _rows = results;
  }

  @JsonProperty("columns")
  public List<String> getColumns() {
    return _columns;
  }

  @JsonProperty("columns")
  public void setColumns(List<String> columns) {
    _columns = columns;
  }

  @JsonProperty("results")
  public List<Serializable[]> getRows() {
    return _rows;
  }

  @JsonProperty("results")
  public void setRows(List<Serializable[]> rows) {
    _rows = rows;
  }
}
