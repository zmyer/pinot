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

package com.linkedin.pinot.validation;

public class RealtimeSegmentNameHolder implements Comparable<RealtimeSegmentNameHolder> {
  private final int _kafkaPartition;
  private final String _tableName;
  private final long _startOffset;

  public RealtimeSegmentNameHolder(String segmentName) {
    String[] parts = segmentName.split("_");
    _tableName = parts[0];
    _kafkaPartition = Integer.valueOf(parts[1]);
    _startOffset = Long.valueOf(parts[2]);
  }

  public RealtimeSegmentNameHolder(String tableName, int kafkaPartition, long startOffset) {
    // TODO add more fields like start time
    // TODO make it different from existing format.
    _kafkaPartition = kafkaPartition;
    _startOffset = startOffset;
    _tableName = tableName;
  }

  public String getName() {
    return serialize();
  }

  public int getKafkaPartition() {
    return _kafkaPartition;
  }

  public String getTableName() {
    return _tableName;
  }

  public long getStartOffset() {
    return _startOffset;
  }

  // A simple serde for now.
  private String serialize() {
    return _tableName + "_" + _kafkaPartition + "_" + Long.toString(_startOffset);
  }

  @Override
  public int compareTo(RealtimeSegmentNameHolder other) {
    if (_kafkaPartition < other._kafkaPartition) {
      return -1;
    } else if (_kafkaPartition > other._kafkaPartition) {
      return 1;
    } else {
      if (_startOffset > other._startOffset) {
        return 1;
      } else if (_startOffset < other._startOffset) {
        return -1;
      }
    }
    return 0;
  }
}
