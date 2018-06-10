package com.linkedin.thirdeye.datasource;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.cache.DatasetConfigCacheLoader;
import com.linkedin.thirdeye.datasource.cache.DatasetListCache;
import com.linkedin.thirdeye.datasource.cache.DatasetMaxDataTimeCacheLoader;
import com.linkedin.thirdeye.datasource.cache.DimensionFiltersCacheLoader;
import com.linkedin.thirdeye.datasource.cache.MetricConfigCacheLoader;
import com.linkedin.thirdeye.datasource.cache.MetricDataset;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThirdEyeCacheRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeCacheRegistry.class);
  private static final ThirdEyeCacheRegistry INSTANCE = new ThirdEyeCacheRegistry();

  // DAO to ThirdEye's data and meta-data storage.
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static DatasetConfigManager datasetConfigDAO;
  private static MetricConfigManager metricConfigDAO;
  private static AnomalyFunctionManager anomalyFunctionDAO;

  // TODO: Rename QueryCache to a name like DataSourceCache.
  private QueryCache queryCache;

  // Meta-data caches
  private LoadingCache<String, DatasetConfigDTO> datasetConfigCache;
  private LoadingCache<MetricDataset, MetricConfigDTO> metricConfigCache;
  private LoadingCache<String, Long> datasetMaxDataTimeCache;
  private LoadingCache<String, String> dimensionFiltersCache;
  private DatasetListCache datasetsCache;

  public static ThirdEyeCacheRegistry getInstance() {
    return INSTANCE;
  }

  /**
   * Initializes data sources and caches.
   *
   * @param thirdeyeConfig ThirdEye's configurations.
   */
  public static void initializeCaches(ThirdEyeConfiguration thirdeyeConfig) throws Exception {
    initDataSources(thirdeyeConfig);
    initMetaDataCaches(thirdeyeConfig);
    initPeriodicCacheRefresh();
  }

  /**
   * Initializes data sources and caches without starting auto-refreshing procedure. This method is useful for running
   * a lightweight ThirdEye that doesn't cache tons of data beforehand.
   *
   * @param thirdeyeConfig ThirdEye's configurations.
   */
  public static void initializeCachesWithoutRefreshing(ThirdEyeConfiguration thirdeyeConfig) throws Exception {
    initDataSources(thirdeyeConfig);
    initMetaDataCaches(thirdeyeConfig);
  }

  /**
   * Initializes the adaptor to data sources such as Pinot, MySQL, etc.
   */
  private static void initDataSources(ThirdEyeConfiguration thirdeyeConfig) {
    try {
      // Initialize adaptors to time series databases.
      URL dataSourcesUrl = thirdeyeConfig.getDataSourcesAsUrl();
      DataSources dataSources = DataSourcesLoader.fromDataSourcesUrl(dataSourcesUrl);
      if (dataSources == null) {
        throw new IllegalStateException("Could not create data sources from path " + dataSourcesUrl);
      }
      // Query Cache
      Map<String, ThirdEyeDataSource> thirdEyeDataSourcesMap = DataSourcesLoader.getDataSourceMap(dataSources);
      QueryCache queryCache = new QueryCache(thirdEyeDataSourcesMap, Executors.newCachedThreadPool());
      ThirdEyeCacheRegistry.getInstance().registerQueryCache(queryCache);

      // Initialize connection to ThirdEye's anomaly and meta-data storage.
      datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
      metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
      anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    } catch (Exception e) {
     LOGGER.info("Caught exception while initializing caches", e);
    }
  }

  /**
   * Initialize the cache for meta data. This method has to be invoked after data sources are connected.
   */
  private static void initMetaDataCaches(ThirdEyeConfiguration thirdeyeConfig) {
    ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();
    QueryCache queryCache = cacheRegistry.getQueryCache();
    Preconditions.checkNotNull(queryCache,
        "Data sources are not initialzed. Please invoke initDataSources() before this method.");

    // DatasetConfig cache
    // TODO deprecate. read from database directly
    LoadingCache<String, DatasetConfigDTO> datasetConfigCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(new DatasetConfigCacheLoader(datasetConfigDAO));
    cacheRegistry.registerDatasetConfigCache(datasetConfigCache);

    // MetricConfig cache
    // TODO deprecate. read from database directly
    LoadingCache<MetricDataset, MetricConfigDTO> metricConfigCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(new MetricConfigCacheLoader(metricConfigDAO));
    cacheRegistry.registerMetricConfigCache(metricConfigCache);

    // DatasetMaxDataTime Cache
    LoadingCache<String, Long> datasetMaxDataTimeCache = CacheBuilder.newBuilder()
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(new DatasetMaxDataTimeCacheLoader(queryCache, datasetConfigDAO));
    cacheRegistry.registerDatasetMaxDataTimeCache(datasetMaxDataTimeCache);

    // Dimension Filter cache
    LoadingCache<String, String> dimensionFiltersCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build(new DimensionFiltersCacheLoader(queryCache, datasetConfigDAO));
    cacheRegistry.registerDimensionFiltersCache(dimensionFiltersCache);

    // Datasets list cache
    // TODO deprecate. read from database directly
    DatasetListCache datasetsCache = new DatasetListCache(anomalyFunctionDAO, datasetConfigDAO, thirdeyeConfig);
    cacheRegistry.registerDatasetsCache(datasetsCache);
  }

  private static void initPeriodicCacheRefresh() {
    // TODO deprecate this
    ScheduledExecutorService hourlyService = Executors.newSingleThreadScheduledExecutor();
    hourlyService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          LOGGER.info("Refreshing datasets list cache");
          ThirdEyeCacheRegistry.getInstance().getDatasetsCache().loadDatasets();
        } catch (Exception e) {
          LOGGER.error("Exception while refreshing datasets list", e);
        }
      }
    }, 1, 1, TimeUnit.HOURS);
  }

  public LoadingCache<String, Long> getDatasetMaxDataTimeCache() {
    return datasetMaxDataTimeCache;
  }

  public void registerDatasetMaxDataTimeCache(LoadingCache<String, Long> datasetMaxDataTimeCache) {
    this.datasetMaxDataTimeCache = datasetMaxDataTimeCache;
  }

  public DatasetListCache getDatasetsCache() {
    return datasetsCache;
  }

  public void registerDatasetsCache(DatasetListCache collectionsCache) {
    this.datasetsCache = collectionsCache;
  }

  public LoadingCache<String, String> getDimensionFiltersCache() {
    return dimensionFiltersCache;
  }

  public void registerDimensionFiltersCache(LoadingCache<String, String> dimensionFiltersCache) {
    this.dimensionFiltersCache = dimensionFiltersCache;
  }

  public QueryCache getQueryCache() {
    return queryCache;
  }

  public void registerQueryCache(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  public LoadingCache<String, DatasetConfigDTO> getDatasetConfigCache() {
    return datasetConfigCache;
  }

  public void registerDatasetConfigCache(LoadingCache<String, DatasetConfigDTO> datasetConfigCache) {
    this.datasetConfigCache = datasetConfigCache;
  }

  public LoadingCache<MetricDataset, MetricConfigDTO> getMetricConfigCache() {
    return metricConfigCache;
  }

  public void registerMetricConfigCache(LoadingCache<MetricDataset, MetricConfigDTO> metricConfigCache) {
    this.metricConfigCache = metricConfigCache;
  }
}
