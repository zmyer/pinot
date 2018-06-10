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

package com.linkedin.pinot.controller.helix.core.rebalance;

/**
 * Constants for rebalance user config properties
 */
public class RebalanceUserConfigConstants {

  public static final String DRYRUN = "dryRun";
  /** Whether consuming segments should also be rebalanced or not */
  public static final String INCLUDE_CONSUMING = "includeConsuming";

  public static final boolean DEFAULT_DRY_RUN = true;
  public static final boolean DEFAULT_INCLUDE_CONSUMING = false;
}
