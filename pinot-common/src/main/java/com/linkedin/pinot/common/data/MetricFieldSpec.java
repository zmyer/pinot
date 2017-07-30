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
package com.linkedin.pinot.common.data;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.EqualityUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


/**
 * The <code>MetricFieldSpec</code> class contains all specs related to any metric field (column) in {@link Schema}.
 * <p>Different with {@link DimensionFieldSpec}, inside <code>MetricFieldSpec</code> we allow user defined
 * {@link DerivedMetricType} and <code>fieldSize</code>.
 * <p>{@link DerivedMetricType} is used when the metric field is derived from some other fields (e.g. HLL).
 * <p><code>fieldSize</code> is used to mark the size of the value when the size is not constant (e.g. STRING).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class MetricFieldSpec extends FieldSpec {
  private static final int UNDEFINED_FIELD_SIZE = -1;

  // These two fields are for derived metric fields.
  private int _fieldSize = UNDEFINED_FIELD_SIZE;
  private DerivedMetricType _derivedMetricType = null;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public MetricFieldSpec() {
    super();
  }

  public MetricFieldSpec(@Nonnull String name, @Nonnull DataType dataType) {
    super(name, dataType, true);
  }

  public MetricFieldSpec(@Nonnull String name, @Nonnull DataType dataType, @Nonnull Object defaultNullValue) {
    super(name, dataType, true, defaultNullValue);
  }

  // For derived metric fields.
  public MetricFieldSpec(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
      @Nonnull DerivedMetricType derivedMetricType) {
    super(name, dataType, true);
    Preconditions.checkArgument(fieldSize > 0, "Field size must be a positive number.");
    _fieldSize = fieldSize;
    _derivedMetricType = derivedMetricType;
  }

  // For derived metric fields.
  public MetricFieldSpec(@Nonnull String name, @Nonnull DataType dataType, int fieldSize,
      @Nonnull DerivedMetricType derivedMetricType, @Nonnull Object defaultNullValue) {
    super(name, dataType, true, defaultNullValue);
    Preconditions.checkArgument(fieldSize > 0, "Field size must be a positive number.");
    _fieldSize = fieldSize;
    _derivedMetricType = derivedMetricType;
  }

  public int getFieldSize() {
    if (_fieldSize == UNDEFINED_FIELD_SIZE) {
      return getDataType().size();
    } else {
      return _fieldSize;
    }
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setFieldSize(int fieldSize) {
    _fieldSize = fieldSize;
  }

  @Nullable
  public DerivedMetricType getDerivedMetricType() {
    return _derivedMetricType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDerivedMetricType(@Nullable DerivedMetricType derivedMetricType) {
    _derivedMetricType = derivedMetricType;
  }

  @JsonIgnore
  @Nonnull
  @Override
  public FieldType getFieldType() {
    return FieldType.METRIC;
  }

  @Override
  public void setSingleValueField(boolean isSingleValueField) {
    Preconditions.checkArgument(isSingleValueField, "Unsupported multi-value for metric field.");
  }

  @JsonIgnore
  public boolean isDerivedMetric() {
    return _derivedMetricType != null;
  }

  /**
   * The <code>DerivedMetricType</code> enum is assigned for all metric fields to allow derived metric field, a
   * customized type which is not included in DataType.
   * <p>It is currently used for derived field recognition in star tree <code>MetricBuffer</code>, may have other use
   * cases later.
   * <p>Generally, a customized type value should be converted to a standard
   * {@link com.linkedin.pinot.common.data.FieldSpec.DataType} for storage, and converted back when needed.
   */
  public enum DerivedMetricType {
    // HLL derived metric type.
    HLL
  }

  @Override
  public String toString() {
    return "< field type: METRIC, field name: " + getName() + ", data type: " + getDataType() + ", default null value: "
        + getDefaultNullValue() + ", field size: " + getFieldSize() + ", derived metric type: " + _derivedMetricType
        + " >";
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object instanceof MetricFieldSpec) {
      MetricFieldSpec that = (MetricFieldSpec) object;

      return getName().equals(that.getName())
          && getDataType() == that.getDataType()
          && getDefaultNullValue().equals(that.getDefaultNullValue())
          && getFieldSize() == that.getFieldSize()
          && _derivedMetricType == that._derivedMetricType;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = EqualityUtils.hashCodeOf(result, getDataType());
    result = EqualityUtils.hashCodeOf(result, getDefaultNullValue());
    result = EqualityUtils.hashCodeOf(result, getFieldSize());
    result = EqualityUtils.hashCodeOf(result, _derivedMetricType);
    return result;
  }
}
