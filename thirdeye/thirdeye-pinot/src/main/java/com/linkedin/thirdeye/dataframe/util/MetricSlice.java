package com.linkedin.thirdeye.dataframe.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;


/**
 * Selector for time series and aggregate values of a specific metric, independent of
 * data source.
 */
public final class MetricSlice {
  public static final TimeGranularity NATIVE_GRANULARITY = new TimeGranularity(0, TimeUnit.MILLISECONDS);

  final long metricId;
  final long start;
  final long end;
  final Multimap<String, String> filters;
  final TimeGranularity granularity;

  MetricSlice(long metricId, long start, long end, Multimap<String, String> filters, TimeGranularity granularity) {
    this.metricId = metricId;
    this.start = start;
    this.end = end;
    this.filters = filters;
    this.granularity = granularity;
  }

  public long getMetricId() {
    return metricId;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public Multimap<String, String> getFilters() {
    return filters;
  }

  public TimeGranularity getGranularity() {
    return granularity;
  }

  public MetricSlice withStart(long start) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }

  public MetricSlice withEnd(long end) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }

  public MetricSlice withFilters(Multimap<String, String> filters) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }

  public MetricSlice withGranularity(TimeGranularity granularity) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }

  public static MetricSlice from(long metricId, long start, long end) {
    return new MetricSlice(metricId, start, end, ArrayListMultimap.<String, String>create(), NATIVE_GRANULARITY);
  }

  public static MetricSlice from(long metricId, long start, long end, Multimap<String, String> filters) {
    return new MetricSlice(metricId, start, end, filters, NATIVE_GRANULARITY);
  }

  public static MetricSlice from(long metricId, long start, long end, Multimap<String, String> filters, TimeGranularity granularity) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricSlice that = (MetricSlice) o;
    return metricId == that.metricId && start == that.start && end == that.end && Objects.equals(filters, that.filters)
        && Objects.equals(granularity, that.granularity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricId, start, end, filters, granularity);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
