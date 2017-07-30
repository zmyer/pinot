package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.constant.MetricAggFunction;

import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown=true)
public class MetricConfigBean extends AbstractBean {

  public static double DEFAULT_THRESHOLD = 0.01;
  public static String DERIVED_METRIC_ID_PREFIX = "id";
  public static final String ALIAS_JOINER = "::";
  public static final String URL_TEMPLATE_START_TIME = "startTime";
  public static final String URL_TEMPLATE_END_TIME = "endTime";
  public static final MetricAggFunction DEFAULT_AGG_FUNCTION = MetricAggFunction.SUM;

  private String name;

  private String dataset;

  private String alias;

  private MetricType datatype;

  private boolean derived = false;

  private String derivedMetricExpression;

  private MetricAggFunction defaultAggFunction = DEFAULT_AGG_FUNCTION;

  private Double rollupThreshold = DEFAULT_THRESHOLD;

  private boolean inverseMetric = false;

  private String cellSizeExpression;

  private boolean active = true;

  private Map<String, String> extSourceLinkInfo;

  private Map<String, String> extSourceLinkTimeGranularity;

  private Map<String, String> metricProperties = null;


  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public MetricType getDatatype() {
    return datatype;
  }

  public void setDatatype(MetricType datatype) {
    this.datatype = datatype;
  }

  public boolean isDerived() {
    return derived;
  }

  public void setDerived(boolean derived) {
    this.derived = derived;
  }

  public String getDerivedMetricExpression() {
    return derivedMetricExpression;
  }

  public void setDerivedMetricExpression(String derivedMetricExpression) {
    this.derivedMetricExpression = derivedMetricExpression;
  }

  public MetricAggFunction getDefaultAggFunction() {
    return defaultAggFunction;
  }

  public void setDefaultAggFunction(MetricAggFunction defaultAggFunction) {
    this.defaultAggFunction = defaultAggFunction;
  }

  public Double getRollupThreshold() {
    return rollupThreshold;
  }

  public void setRollupThreshold(Double rollupThreshold) {
    this.rollupThreshold = rollupThreshold;
  }

  public boolean isInverseMetric() {
    return inverseMetric;
  }

  public void setInverseMetric(boolean inverseMetric) {
    this.inverseMetric = inverseMetric;
  }

  public String getCellSizeExpression() {
    return cellSizeExpression;
  }

  public void setCellSizeExpression(String cellSizeExpression) {
    this.cellSizeExpression = cellSizeExpression;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public Map<String, String> getExtSourceLinkInfo() {
    return extSourceLinkInfo;
  }

  public void setExtSourceLinkInfo(Map<String, String> extSourceLinkInfo) {
    this.extSourceLinkInfo = extSourceLinkInfo;
  }

  public Map<String, String> getExtSourceLinkTimeGranularity() {
    return extSourceLinkTimeGranularity;
  }

  public void setExtSourceLinkTimeGranularity(Map<String, String> extSourceLinkTimeGranularity) {
    this.extSourceLinkTimeGranularity = extSourceLinkTimeGranularity;
  }

  public Map<String, String> getMetricProperties() {
    return metricProperties;
  }

  public void setMetricProperties(Map<String, String> metricProperties) {
    this.metricProperties = metricProperties;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MetricConfigBean)) {
      return false;
    }
    MetricConfigBean mc = (MetricConfigBean) o;
    return Objects.equals(getId(), mc.getId())
        && Objects.equals(name, mc.getName())
        && Objects.equals(dataset, mc.getDataset())
        && Objects.equals(alias, mc.getAlias())
        && Objects.equals(derived, mc.isDerived())
        && Objects.equals(derivedMetricExpression, mc.getDerivedMetricExpression())
        && Objects.equals(defaultAggFunction, mc.getDefaultAggFunction())
        && Objects.equals(rollupThreshold, mc.getRollupThreshold())
        && Objects.equals(inverseMetric, mc.isInverseMetric())
        && Objects.equals(cellSizeExpression, mc.getCellSizeExpression())
        && Objects.equals(active, mc.isActive())
        && Objects.equals(extSourceLinkInfo, mc.getExtSourceLinkInfo())
        && Objects.equals(metricProperties, mc.getMetricProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), dataset, alias, derived, derivedMetricExpression, defaultAggFunction, rollupThreshold,
        inverseMetric, cellSizeExpression, active, extSourceLinkInfo, metricProperties);
  }
}
