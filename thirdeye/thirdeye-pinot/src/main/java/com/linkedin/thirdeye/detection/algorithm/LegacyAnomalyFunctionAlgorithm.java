package com.linkedin.thirdeye.detection.algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * The Legacy anomaly function algorithm. This can run existing anomaly functions.
 */
public class LegacyAnomalyFunctionAlgorithm extends DetectionPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(LegacyAnomalyFunctionAlgorithm.class);
  private static String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";
  private static String PROP_SPEC = "specs";
  private static String PROP_METRIC_URN = "metricUrn";

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private BaseAnomalyFunction anomalyFunction;
  private String metricUrn;
  private MetricEntity metricEntity;

  /**
   * Instantiates a new Legacy anomaly function algorithm.
   *
   * @param provider the provider
   * @param config the config
   * @param startTime the start time
   * @param endTime the end time
   * @throws Exception the exception
   */
  public LegacyAnomalyFunctionAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);
    // TODO: Round start and end time stamps
    Preconditions.checkArgument(config.getProperties().containsKey(PROP_ANOMALY_FUNCTION_CLASS));
    String anomalyFunctionClassName = MapUtils.getString(config.getProperties(), PROP_ANOMALY_FUNCTION_CLASS);
    anomalyFunction = (BaseAnomalyFunction) Class.forName(anomalyFunctionClassName).newInstance();
    String specs = OBJECT_MAPPER.writeValueAsString(MapUtils.getMap(config.getProperties(), PROP_SPEC));
    anomalyFunction.init(OBJECT_MAPPER.readValue(specs, AnomalyFunctionDTO.class));
    this.metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    metricEntity = MetricEntity.fromURN(metricUrn, 1.0);
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    LOG.info("Running legacy anomaly detection for time range {} to {}", this.startTime, this.endTime);

    Collection<MergedAnomalyResultDTO> historyMergedAnomalies;
    if (anomalyFunction.useHistoryAnomaly() && config.getId() != null) {
      AnomalySlice slice =
          new AnomalySlice().withConfigId(config.getId()).withStart(this.startTime).withEnd(this.endTime);
      historyMergedAnomalies = this.provider.fetchAnomalies(Collections.singletonList(slice)).get(slice);
    } else {
      historyMergedAnomalies = Collections.emptyList();
    }

    final DimensionMap dimension = getDimensionMap();

    MetricConfigDTO metricConfig =
        this.provider.fetchMetrics(Collections.singleton(metricEntity.getId())).get(metricEntity.getId());

    // get time series
    DataFrame df = DataFrame.builder(COL_TIME + ":LONG", COL_VALUE + ":DOUBLE").build();
    List<Pair<Long, Long>> timeIntervals = anomalyFunction.getDataRangeIntervals(this.startTime, this.endTime);
    for (Pair<Long, Long> startEndInterval : timeIntervals) {
      MetricSlice slice =
          MetricSlice.from(metricEntity.getId(), startEndInterval.getFirst(), startEndInterval.getSecond(),
              metricEntity.getFilters());
      DataFrame currentDf = this.provider.fetchTimeseries(Collections.singleton(slice)).get(slice);
      df = df.append(currentDf);
    }

    MetricTimeSeries metricTimeSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(
        Collections.singletonList(new MetricSpec(metricConfig.getName(), MetricType.DOUBLE))));

    LongSeries timestamps = df.getLongs(COL_TIME);
    for (int i = 0; i < timestamps.size(); i++) {
      metricTimeSeries.set(timestamps.get(i), metricConfig.getName(), df.getDoubles(COL_VALUE).get(i));
    }

    List<AnomalyResult> result =
        anomalyFunction.analyze(dimension, metricTimeSeries, new DateTime(this.startTime), new DateTime(this.endTime),
            new ArrayList<>(historyMergedAnomalies));

    Collection<MergedAnomalyResultDTO> mergedAnomalyResults =
        Collections2.transform(result, new Function<AnomalyResult, MergedAnomalyResultDTO>() {
          @Override
          public MergedAnomalyResultDTO apply(AnomalyResult result) {
            MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
            anomaly.populateFrom(result);
            anomaly.setDimensions(dimension);
            return anomaly;
          }
        });

    LOG.info("Detected {} anomalies for {}", mergedAnomalyResults.size(), this.metricUrn);

    return new DetectionPipelineResult(new ArrayList<>(mergedAnomalyResults));
  }

  private DimensionMap getDimensionMap() {
    DimensionMap dimensionMap = new DimensionMap();
    for (Map.Entry<String, String> entry : metricEntity.getFilters().entries()) {
      dimensionMap.put(entry.getKey(), entry.getValue());
    }
    return dimensionMap;
  }
}
