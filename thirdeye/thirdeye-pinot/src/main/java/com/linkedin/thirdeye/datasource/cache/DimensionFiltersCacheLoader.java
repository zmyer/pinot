package com.linkedin.thirdeye.datasource.cache;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;

public class DimensionFiltersCacheLoader extends CacheLoader<String, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionFiltersCacheLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private QueryCache queryCache;
  private DatasetConfigManager datasetConfigDAO;

  public DimensionFiltersCacheLoader(QueryCache queryCache, DatasetConfigManager datasetConfigDAO) {
    this.queryCache = queryCache;
    this.datasetConfigDAO = datasetConfigDAO;
  }

  /**
   * Fetched dimension filters for this dataset from the right data source
   * {@inheritDoc}
   * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
   */
  @Override
  public String load(String dataset) throws Exception {
    LOGGER.info("Loading from dimension filters cache {}", dataset);
    String dimensionFiltersJson = null;
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
    String dataSourceName = datasetConfig.getDataSource();
    try {
      ThirdEyeDataSource dataSource = queryCache.getDataSource(dataSourceName);
      Map<String, List<String>> dimensionFilters = dataSource.getDimensionFilters(dataset);
      dimensionFiltersJson = OBJECT_MAPPER.writeValueAsString(dimensionFilters);
    } catch (Exception e) {
      LOGGER.error("Exception in getting max date time for {} from data source {}", dataset, dataSourceName, e);
    }
    return dimensionFiltersJson;
  }
}

