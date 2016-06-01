/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.Map;
import org.apache.helix.ZNRecord;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.EqualityUtils;


public class LLRealtimeSegmentZKMetadata extends RealtimeSegmentZKMetadata {
  private static final String START_OFFSET = "segment.realtime.startOffset";
  private static final String END_OFFSET = "segment.realtime.endOffset";
  private static final String NUM_REPLICAS = "segment.realtime.numReplicas";

  private long _startOffset;
  private long _endOffset;
  private int _numReplicas;

  public LLRealtimeSegmentZKMetadata() {
    super();
  }

  public LLRealtimeSegmentZKMetadata(ZNRecord znRecord) {
    super(znRecord);
    _startOffset = Long.valueOf(znRecord.getSimpleField(START_OFFSET));
    _numReplicas = Byte.valueOf(znRecord.getSimpleField(NUM_REPLICAS));
    _endOffset = Long.valueOf(znRecord.getSimpleField(END_OFFSET));
  }

  public long getStartOffset() {
    return _startOffset;
  }

  public long getEndOffset() {
    return _endOffset;
  }

  public int getNumReplicas() {
    return _numReplicas;
  }

  public void setStartOffset(long startOffset) {
    _startOffset = startOffset;
  }

  public void setEndOffset(long endOffset) {
    _endOffset = endOffset;
  }

  public void setNumReplicas(int numReplicas) {
    _numReplicas = numReplicas;
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = super.toZNRecord();
    znRecord.setLongField(START_OFFSET, _startOffset);
    znRecord.setLongField(END_OFFSET, _endOffset);
    znRecord.setIntField(NUM_REPLICAS, _numReplicas);
    return znRecord;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    String newline = "\n";
    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newline);
    result.append("  " + super.getClass().getName() + " : " + super.toString());
    result.append(newline);
    result.append("  " + START_OFFSET + " : " + _startOffset + ",");
    result.append("  " + END_OFFSET + " : " + _endOffset);
    result.append(newline);
    result.append("}");
    return result.toString();
  }

  @Override
  public boolean equals(Object segmentMetadata) {
    if (EqualityUtils.isSameReference(this, segmentMetadata)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, segmentMetadata)) {
      return false;
    }

    LLRealtimeSegmentZKMetadata metadata = (LLRealtimeSegmentZKMetadata) segmentMetadata;
    return super.equals(metadata) && EqualityUtils.isEqual(_startOffset, metadata._startOffset) && EqualityUtils.isEqual(_endOffset, metadata._endOffset);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result =  EqualityUtils.hashCodeOf(result, _startOffset);
    result =  EqualityUtils.hashCodeOf(result, _startOffset);
    return result;
  }

  @Override
  public Map<String, String> toMap() {
    Map<String, String> configMap = super.toMap();
    configMap.put(START_OFFSET, Long.toString(_startOffset));
    configMap.put(END_OFFSET, Long.toString(_endOffset));
    return configMap;
  }
}
