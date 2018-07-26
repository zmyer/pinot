package com.linkedin.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class DefaultDataProvider implements DataProvider {
  private static final long TIMEOUT = 60000;

  private final ExecutorService executor = Executors.newCachedThreadPool();

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final TimeSeriesLoader timeseriesLoader;
  private final AggregationLoader aggregationLoader;
  private final DetectionPipelineLoader loader;

  public DefaultDataProvider(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, EventManager eventDAO,
      MergedAnomalyResultManager anomalyDAO, TimeSeriesLoader timeseriesLoader, AggregationLoader aggregationLoader,
      DetectionPipelineLoader loader) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.eventDAO = eventDAO;
    this.anomalyDAO = anomalyDAO;
    this.timeseriesLoader = timeseriesLoader;
    this.aggregationLoader = aggregationLoader;
    this.loader = loader;
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices) {
    try {
      Map<MetricSlice, Future<DataFrame>> futures = new HashMap<>();
      for (final MetricSlice slice : slices) {
        futures.put(slice, this.executor.submit(new Callable<DataFrame>() {
          @Override
          public DataFrame call() throws Exception {
            return DefaultDataProvider.this.timeseriesLoader.load(slice);
          }
        }));
      }

      final long deadline = System.currentTimeMillis() + TIMEOUT;
      Map<MetricSlice, DataFrame> output = new HashMap<>();
      for (MetricSlice slice : slices) {
        output.put(slice, futures.get(slice).get(makeTimeout(deadline), TimeUnit.MILLISECONDS));
      }
      return output;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices, final List<String> dimensions) {
    try {
      Map<MetricSlice, Future<DataFrame>> futures = new HashMap<>();
      for (final MetricSlice slice : slices) {
        futures.put(slice, this.executor.submit(new Callable<DataFrame>() {
          @Override
          public DataFrame call() throws Exception {
            return DefaultDataProvider.this.aggregationLoader.loadAggregate(slice, dimensions);
          }
        }));
      }

      final long deadline = System.currentTimeMillis() + TIMEOUT;
      Map<MetricSlice, DataFrame> output = new HashMap<>();
      for (MetricSlice slice : slices) {
        output.put(slice, futures.get(slice).get(makeTimeout(deadline), TimeUnit.MILLISECONDS));
      }
      return output;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices) {
    Multimap<AnomalySlice, MergedAnomalyResultDTO> output = ArrayListMultimap.create();
    for (AnomalySlice slice : slices) {
      List<Predicate> predicates = new ArrayList<>();
      if (slice.end >= 0)
        predicates.add(Predicate.LT("startTime", slice.end));
      if (slice.start >= 0)
        predicates.add(Predicate.GT("endTime", slice.start));
      if (slice.configId >= 0)
        predicates.add(Predicate.EQ("detectionConfigId", slice.configId));

      if (predicates.isEmpty())
        throw new IllegalArgumentException("Must provide at least one of start, end, or detectionConfigId");

      List<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findByPredicate(AND(predicates));
      Iterator<MergedAnomalyResultDTO> itAnomaly = anomalies.iterator();
      while (itAnomaly.hasNext()) {
        if (!slice.match(itAnomaly.next())) {
          itAnomaly.remove();
        }
      }

      output.putAll(slice, anomalies);
    }
    return output;
  }

  @Override
  public Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices) {
    Multimap<EventSlice, EventDTO> output = ArrayListMultimap.create();
    for (EventSlice slice : slices) {
      List<Predicate> predicates = new ArrayList<>();
      if (slice.end >= 0)
        predicates.add(Predicate.LT("startTime", slice.end));
      if (slice.start >= 0)
        predicates.add(Predicate.GT("endTime", slice.start));

      if (predicates.isEmpty())
        throw new IllegalArgumentException("Must provide at least one of start, or end");
      List<EventDTO> events = this.eventDAO.findByPredicate(AND(predicates));
      Iterator<EventDTO> itEvent = events.iterator();
      while (itEvent.hasNext()) {
        if (!slice.match(itEvent.next())) {
          itEvent.remove();
        }
      }

      output.putAll(slice, events);
    }
    return output;
  }

  @Override
  public Map<Long, MetricConfigDTO> fetchMetrics(Collection<Long> ids) {
    List<MetricConfigDTO> metrics = this.metricDAO.findByPredicate(Predicate.IN("baseId", ids.toArray()));

    Map<Long, MetricConfigDTO> output = new HashMap<>();
    for (MetricConfigDTO metric : metrics) {
      if (metric != null) {
        output.put(metric.getId(), metric);
      }
    }
    return output;
  }

  @Override
  public Map<String, DatasetConfigDTO> fetchDatasets(Collection<String> datasetNames) {
    List<DatasetConfigDTO> datasets = this.datasetDAO.findByPredicate(Predicate.IN("dataset", datasetNames.toArray()));

    Map<String, DatasetConfigDTO> output = new HashMap<>();
    for (DatasetConfigDTO dataset : datasets) {
      if (dataset != null) {
        output.put(dataset.getDataset(), dataset);
      }
    }
    return output;
  }

  @Override
  public DetectionPipeline loadPipeline(DetectionConfigDTO config, long start, long end) throws Exception {
    return this.loader.from(this, config, start, end);
  }

  private static Predicate AND(Collection<Predicate> predicates) {
    return Predicate.AND(predicates.toArray(new Predicate[predicates.size()]));
  }

  private static long makeTimeout(long deadline) {
    long diff = deadline - System.currentTimeMillis();
    return diff > 0 ? diff : 0;
  }
}
