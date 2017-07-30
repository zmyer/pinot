package com.linkedin.thirdeye.client.diffsummary;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


class Row {
  Dimensions dimensions;
  DimensionValues dimensionValues;
  double baselineValue;
  double currentValue;

  public Dimensions getDimensions() {
    return dimensions;
  }

  public void setDimensions(Dimensions dimensions) {
    this.dimensions = dimensions;
  }

  public DimensionValues getDimensionValues() {
    return dimensionValues;
  }

  public void setDimensionValues(DimensionValues dimensionValues) {
    this.dimensionValues = dimensionValues;
  }

  public double getBaselineValue() {
    return baselineValue;
  }

  public void setBaselineValue(double baselineValue) {
    this.baselineValue = baselineValue;
  }

  public double getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(double currentValue) {
    this.currentValue = currentValue;
  }

  public double ratio() {
    return currentValue / baselineValue;
  }

  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
