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

import java.util.HashMap;
import java.util.Map;

public class GroupByUtils {

  private static final String GROUP_BY_FUNCTION_DELIMETER = "\\$\\$\\$";

  public static String[] parseGroupByColumn(String groupByField) {
    return groupByField.split(GROUP_BY_FUNCTION_DELIMETER);
  }

  public static String getGroupByFunction(String groupByField) {
    String[] splitFields = parseGroupByColumn(groupByField);
    if (splitFields.length > 1) {
      return splitFields[0];
    }
    return null;
  }

  public static String getGroupByColumn(String groupByField) {
    String[] splitFields = parseGroupByColumn(groupByField);
    if (splitFields.length > 1) {
      return splitFields[1];
    }
    return groupByField;
  }

  public static Map<String, String> getGroupByParams(String groupByField) {
    String[] splitFields = parseGroupByColumn(groupByField);
    Map<String, String> parmas = new HashMap<String, String>();
    for (int j = 2; j < splitFields.length; ++j) {
      String[] splits = splitFields[j].split("=", 2);
      parmas.put(splits[0], splits[1]);
    }
    return parmas;
  }

  public static void main(String[] args) {
    String x = "to_lower$$$column1$$$x=y$$$a=b$$$c=d";
    System.out.println(x);
    System.out.println(getGroupByColumn(x));
    System.out.println(getGroupByFunction(x));
    System.out.println(getGroupByParams(x).size());
    System.out.println(getGroupByParams(x).get("x"));
    System.out.println(getGroupByParams(x).get("a"));
    System.out.println(getGroupByParams(x).get("c"));
  }
}
