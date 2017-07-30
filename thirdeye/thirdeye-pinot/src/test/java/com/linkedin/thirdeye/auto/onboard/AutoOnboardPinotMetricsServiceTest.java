package com.linkedin.thirdeye.auto.onboard;

import com.google.common.collect.Sets;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.thirdeye.auto.onboard.AutoOnboardPinotDataSource;
import com.linkedin.thirdeye.datalayer.bao.AbstractManagerTestBase;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AutoOnboardPinotMetricsServiceTest  extends AbstractManagerTestBase {

  private AutoOnboardPinotDataSource testAutoLoadPinotMetricsService;
  private String dataset = "test-collection";
  private Schema schema;

  @BeforeMethod
  void beforeMethod() throws Exception {
    super.init();
    testAutoLoadPinotMetricsService = new AutoOnboardPinotDataSource(null, null);
    schema = Schema.fromInputSteam(ClassLoader.getSystemResourceAsStream("sample-pinot-schema.json"));
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, null);
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    super.cleanup();
  }

  @Test
  public void testAddNewDataset() throws Exception {
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
    Assert.assertEquals(datasetConfig.getDataset(), dataset);
    Assert.assertEquals(datasetConfig.getDimensions(), schema.getDimensionNames());
    Assert.assertEquals(datasetConfig.getTimeColumn(), schema.getTimeColumnName());
    TimeGranularitySpec timeGranularitySpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();
    Assert.assertEquals(datasetConfig.bucketTimeGranularity().getUnit(), timeGranularitySpec.getTimeType());
    Assert.assertEquals(datasetConfig.bucketTimeGranularity().getSize(), timeGranularitySpec.getTimeUnitSize());
    Assert.assertEquals(datasetConfig.getTimeFormat(), timeGranularitySpec.getTimeFormat());
    Assert.assertEquals(datasetConfig.getTimezone(), "UTC");
    Assert.assertEquals(datasetConfig.getExpectedDelay().getUnit(), TimeUnit.HOURS);

    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    List<String> schemaMetricNames = schema.getMetricNames();
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(metricConfigs.size(), schemaMetricNames.size());
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Assert.assertTrue(schemaMetricNames.contains(metricConfig.getName()));
      metricIds.add(metricConfig.getId());
    }

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.
        findByName(DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + dataset);
    Assert.assertEquals(dashboardConfig.getMetricIds(), metricIds);
  }

  @Test
  public void testRefreshDataset() throws Exception {
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("newDimension", DataType.STRING, true);
    schema.addField(dimensionFieldSpec);
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, datasetConfig);
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    DatasetConfigDTO newDatasetConfig1 = datasetConfigDAO.findByDataset(dataset);
    Assert.assertEquals(newDatasetConfig1.getDataset(), dataset);
    Assert.assertEquals(Sets.newHashSet(newDatasetConfig1.getDimensions()), Sets.newHashSet(schema.getDimensionNames()));

    MetricFieldSpec metricFieldSpec = new MetricFieldSpec("newMetric", DataType.LONG);
    schema.addField(metricFieldSpec);
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, newDatasetConfig1);

    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    List<String> schemaMetricNames = schema.getMetricNames();
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(metricConfigs.size(), schemaMetricNames.size());
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Assert.assertTrue(schemaMetricNames.contains(metricConfig.getName()));
      metricIds.add(metricConfig.getId());
    }

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.
        findByName(DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + dataset);
    Assert.assertEquals(dashboardConfig.getMetricIds(), metricIds);
  }
}
