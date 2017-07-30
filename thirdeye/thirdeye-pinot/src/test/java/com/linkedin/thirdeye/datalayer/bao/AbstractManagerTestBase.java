package com.linkedin.thirdeye.datalayer.bao;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.TestDBResources;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.ScriptRunner;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.ConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.datalayer.util.PersistenceConfig;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlphaBetaAlertFilter;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.joda.time.DateTime;

public abstract class AbstractManagerTestBase {
  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected RawAnomalyResultManager rawAnomalyResultDAO;
  protected JobManager jobDAO;
  protected TaskManager taskDAO;
  protected MergedAnomalyResultManager mergedAnomalyResultDAO;
  protected DatasetConfigManager datasetConfigDAO;
  protected MetricConfigManager metricConfigDAO;
  protected DashboardConfigManager dashboardConfigDAO;
  protected OverrideConfigManager overrideConfigDAO;
  protected AlertConfigManager alertConfigDAO;
  protected DataCompletenessConfigManager dataCompletenessConfigDAO;
  protected EventManager eventDAO;
  protected DetectionStatusManager detectionStatusDAO;
  protected AutotuneConfigManager autotuneConfigDAO;
  protected ClassificationConfigManager classificationConfigDAO;
  protected EntityToEntityMappingManager entityToEntityMappingDAO;
  protected GroupedAnomalyResultsManager groupedAnomalyResultsDAO;
  protected OnboardDatasetMetricManager onboardDatasetMetricDAO;
  protected ConfigManager configDAO;
  protected ApplicationManager applicationDAO;

  //  protected TestDBResources testDBResources;
  protected DAORegistry daoRegistry;
  DataSource ds;
  String dbUrlId;

  protected void init() {
    try {
      URL url = TestDBResources.class.getResource("/persistence-local.yml");
      File configFile = new File(url.toURI());
      PersistenceConfig configuration = DaoProviderUtil.createConfiguration(configFile);
      initializeDs(configuration);

      DaoProviderUtil.init(ds);

      daoRegistry = DAORegistry.getInstance();
      anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
      rawAnomalyResultDAO = daoRegistry.getRawAnomalyResultDAO();
      jobDAO = daoRegistry.getJobDAO();
      taskDAO = daoRegistry.getTaskDAO();
      mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
      datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
      metricConfigDAO = daoRegistry.getMetricConfigDAO();
      dashboardConfigDAO = daoRegistry.getDashboardConfigDAO();
      overrideConfigDAO = daoRegistry.getOverrideConfigDAO();
      alertConfigDAO = daoRegistry.getAlertConfigDAO();
      dataCompletenessConfigDAO = daoRegistry.getDataCompletenessConfigDAO();
      eventDAO = daoRegistry.getEventDAO();
      anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
      detectionStatusDAO = daoRegistry.getDetectionStatusDAO();
      autotuneConfigDAO = daoRegistry.getAutotuneConfigDAO();
      classificationConfigDAO = daoRegistry.getClassificationConfigDAO();
      entityToEntityMappingDAO = daoRegistry.getEntityToEntityMappingDAO();
      groupedAnomalyResultsDAO = daoRegistry.getGroupedAnomalyResultsDAO();
      onboardDatasetMetricDAO = daoRegistry.getOnboardDatasetMetricDAO();
      configDAO = daoRegistry.getConfigDAO();
      applicationDAO = daoRegistry.getApplicationDAO();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void cleanup() {
    try {
      cleanUpJDBC();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void initializeDs(PersistenceConfig configuration) throws Exception {
    ds = new DataSource();
    dbUrlId =
        configuration.getDatabaseConfiguration().getUrl() + System.currentTimeMillis() + "" + Math
            .random();
    ds.setUrl(dbUrlId);
    System.out.println("Creating db with connection url : " + ds.getUrl());
    ds.setPassword(configuration.getDatabaseConfiguration().getPassword());
    ds.setUsername(configuration.getDatabaseConfiguration().getUser());
    ds.setDriverClassName(configuration.getDatabaseConfiguration().getProperties()
        .get("hibernate.connection.driver_class"));

    // pool size configurations
    ds.setMaxActive(200);
    ds.setMinIdle(10);
    ds.setInitialSize(10);

    // when returning connection to pool
    ds.setTestOnReturn(true);
    ds.setRollbackOnReturn(true);

    // Timeout before an abandoned(in use) connection can be removed.
    ds.setRemoveAbandonedTimeout(600_000);
    ds.setRemoveAbandoned(true);

    Connection conn = ds.getConnection();
    // create schema
    URL createSchemaUrl = getClass().getResource("/schema/create-schema.sql");
    ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
    scriptRunner.setDelimiter(";", true);
    scriptRunner.runScript(new FileReader(createSchemaUrl.getFile()));
  }

  private void cleanUpJDBC() throws Exception {
    System.out.println("Cleaning database: start");
    try (Connection conn = ds.getConnection()) {
      URL deleteSchemaUrl = getClass().getResource("/schema/drop-tables.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
      scriptRunner.runScript(new FileReader(deleteSchemaUrl.getFile()));
    }
    new File(dbUrlId).delete();
    System.out.println("Cleaning database: done!");
  }

  protected AnomalyFunctionDTO getTestFunctionSpec(String metricName, String collection) {
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setFunctionName("integration test function 1");
    functionSpec.setType("WEEK_OVER_WEEK_RULE");
    functionSpec.setTopicMetric(metricName);
    functionSpec.setMetrics(Arrays.asList(metricName));
    functionSpec.setCollection(collection);
    functionSpec.setMetricFunction(MetricAggFunction.SUM);
    functionSpec.setCron("0/10 * * * * ?");
    functionSpec.setBucketSize(1);
    functionSpec.setBucketUnit(TimeUnit.HOURS);
    functionSpec.setWindowDelay(3);
    functionSpec.setWindowDelayUnit(TimeUnit.HOURS);
    functionSpec.setWindowSize(1);
    functionSpec.setWindowUnit(TimeUnit.DAYS);
    functionSpec.setProperties("baseline=w/w;changeThreshold=0.001");
    functionSpec.setIsActive(true);
    functionSpec.setRequiresCompletenessCheck(false);
    return functionSpec;
  }

  protected AnomalyFunctionDTO getTestFunctionAlphaBetaAlertFilterSpec(String metricName, String collection){
    AnomalyFunctionDTO functionSpec = getTestFunctionSpec(metricName, collection);
    Map<String, String> alphaBetaAlertFilter = new HashMap<>();
    alphaBetaAlertFilter.put("type", "alpha_beta");
    alphaBetaAlertFilter.put(AlphaBetaAlertFilter.ALPHA, "1");
    alphaBetaAlertFilter.put(AlphaBetaAlertFilter.BETA, "1");
    alphaBetaAlertFilter.put(AlphaBetaAlertFilter.THRESHOLD, "0.5");
    functionSpec.setAlertFilter(alphaBetaAlertFilter);
    return functionSpec;
  }

  protected AlertConfigDTO getTestAlertConfiguration(String name) {
    AlertConfigDTO alertConfigDTO = new AlertConfigDTO();
    alertConfigDTO.setName(name);
    alertConfigDTO.setActive(true);
    alertConfigDTO.setFromAddress("te@linkedin.com");
    alertConfigDTO.setRecipients("anomaly@linedin.com");
    alertConfigDTO.setCronExpression("0/10 * * * * ?");
    AlertConfigBean.EmailConfig emailConfig = new AlertConfigBean.EmailConfig();
    emailConfig.setAnomalyWatermark(0l);
    alertConfigDTO.setEmailConfig(emailConfig);
    AlertConfigBean.ReportConfigCollection reportConfigCollection =
        new AlertConfigBean.ReportConfigCollection();
    reportConfigCollection.setEnabled(true);
    alertConfigDTO.setReportConfigCollection(reportConfigCollection);
    return alertConfigDTO;
  }

  protected ClassificationConfigDTO getTestGroupingConfiguration(List<Long> mainFunctionIdList) {
    ClassificationConfigDTO configDTO = new ClassificationConfigDTO();
    configDTO.setName("classificationJob");
    configDTO.setMainFunctionIdList(mainFunctionIdList);
    configDTO.setAuxFunctionIdList(mainFunctionIdList);
    configDTO.setActive(true);
    return configDTO;
  }

  protected RawAnomalyResultDTO getAnomalyResult() {
    RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
    anomalyResult.setScore(1.1);
    anomalyResult.setStartTime(System.currentTimeMillis());
    anomalyResult.setEndTime(System.currentTimeMillis());
    anomalyResult.setWeight(10.1);
    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("dimensionName", "dimensionValue");
    anomalyResult.setDimensions(dimensionMap);
    anomalyResult.setCreationTimeUtc(System.currentTimeMillis());
    return anomalyResult;
  }

  JobDTO getTestJobSpec() {
    JobDTO jobSpec = new JobDTO();
    jobSpec.setJobName("Test_Anomaly_Job");
    jobSpec.setStatus(JobConstants.JobStatus.SCHEDULED);
    jobSpec.setTaskType(TaskConstants.TaskType.ANOMALY_DETECTION);
    jobSpec.setScheduleStartTime(System.currentTimeMillis());
    jobSpec.setWindowStartTime(new DateTime().minusHours(20).getMillis());
    jobSpec.setWindowEndTime(new DateTime().minusHours(10).getMillis());
    jobSpec.setConfigId(100);
    return jobSpec;
  }

  protected DatasetConfigDTO getTestDatasetConfig(String collection) {
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(collection);
    datasetConfigDTO.setDimensions(Lists.newArrayList("country", "browser", "environment"));
    datasetConfigDTO.setTimeColumn("time");
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setTimeUnit(TimeUnit.HOURS);
    datasetConfigDTO.setActive(true);
    datasetConfigDTO.setRequiresCompletenessCheck(false);
    return datasetConfigDTO;
  }

  protected MetricConfigDTO getTestMetricConfig(String collection, String metric, Long id) {
    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    if (id != null) {
      metricConfigDTO.setId(id);
    }
    metricConfigDTO.setDataset(collection);
    metricConfigDTO.setDatatype(MetricType.LONG);
    metricConfigDTO.setName(metric);
    metricConfigDTO.setAlias(ThirdEyeUtils.constructMetricAlias(collection, metric));
    return metricConfigDTO;
  }

  protected OverrideConfigDTO getTestOverrideConfigForTimeSeries(DateTime now) {
    OverrideConfigDTO overrideConfigDTO = new OverrideConfigDTO();
    overrideConfigDTO.setStartTime(now.minusHours(8).getMillis());
    overrideConfigDTO.setEndTime(now.plusHours(8).getMillis());
    overrideConfigDTO.setTargetEntity(OverrideConfigHelper.ENTITY_TIME_SERIES);
    overrideConfigDTO.setActive(true);

    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(ScalingFactor.SCALING_FACTOR, "1.2");
    overrideConfigDTO.setOverrideProperties(overrideProperties);

    Map<String, List<String>> overrideTarget = new HashMap<>();
    overrideTarget
        .put(OverrideConfigHelper.TARGET_COLLECTION, Arrays.asList("collection1", "collection2"));
    overrideTarget.put(OverrideConfigHelper.EXCLUDED_COLLECTION, Arrays.asList("collection3"));
    overrideConfigDTO.setTargetLevel(overrideTarget);

    return overrideConfigDTO;
  }

  protected DataCompletenessConfigDTO getTestDataCompletenessConfig(String dataset,
      long dateToCheckInMS, String dateToCheckInSDF, boolean dataComplete) {
    DataCompletenessConfigDTO dataCompletenessConfigDTO = new DataCompletenessConfigDTO();
    dataCompletenessConfigDTO.setDataset(dataset);
    dataCompletenessConfigDTO.setDateToCheckInMS(dateToCheckInMS);
    dataCompletenessConfigDTO.setDateToCheckInSDF(dateToCheckInSDF);
    dataCompletenessConfigDTO.setDataComplete(dataComplete);
    dataCompletenessConfigDTO.setCountStar(2000);
    dataCompletenessConfigDTO.setPercentComplete(79);
    dataCompletenessConfigDTO.setNumAttempts(3);
    return dataCompletenessConfigDTO;
  }

  protected DetectionStatusDTO getTestDetectionStatus(String dataset, long dateToCheckInMS,
      String dateToCheckInSDF, boolean detectionRun, long functionId) {
    DetectionStatusDTO detectionStatusDTO = new DetectionStatusDTO();
    detectionStatusDTO.setDataset(dataset);
    detectionStatusDTO.setFunctionId(functionId);
    detectionStatusDTO.setDateToCheckInMS(dateToCheckInMS);
    detectionStatusDTO.setDateToCheckInSDF(dateToCheckInSDF);
    detectionStatusDTO.setDetectionRun(detectionRun);
    return detectionStatusDTO;
  }

  protected AutotuneConfigDTO getTestAutotuneConfig(long functionId, long start, long end) {
    AutotuneConfigDTO autotuneConfigDTO = new AutotuneConfigDTO();
    autotuneConfigDTO.setFunctionId(functionId);
    autotuneConfigDTO.setStartTime(start);
    autotuneConfigDTO.setEndTime(end);
    autotuneConfigDTO.setPerformanceEvaluationMethod(PerformanceEvaluationMethod.ANOMALY_PERCENTAGE);
    autotuneConfigDTO.setLastUpdateTimestamp(DateTime.now().getMillis());
    Map<String, String> config = new HashMap<>();
    config.put("ConfigKey", "ConfigValue");
    autotuneConfigDTO.setConfiguration(config);
    Map<String, Double> performance = new HashMap<>();
    performance.put(autotuneConfigDTO.getPerformanceEvaluationMethod().name(), 0.5);
    autotuneConfigDTO.setPerformance(performance);
    return autotuneConfigDTO;
  }

  protected ClassificationConfigDTO getTestClassificationConfig(String name, List<Long> mainFunctionIdList,
      List<Long> functionIds) {
    ClassificationConfigDTO classificationConfigDTO = new ClassificationConfigDTO();
    classificationConfigDTO.setName(name);
    classificationConfigDTO.setMainFunctionIdList(mainFunctionIdList);
    classificationConfigDTO.setAuxFunctionIdList(functionIds);
    classificationConfigDTO.setActive(true);
    return classificationConfigDTO;
  }

  protected EntityToEntityMappingDTO getTestEntityToEntityMapping(String fromURN, String toURN, String mappingType) {
    EntityToEntityMappingDTO dto = new EntityToEntityMappingDTO();
    dto.setFromURN(fromURN);
    dto.setToURN(toURN);
    dto.setMappingType(mappingType);
    dto.setScore(1);
    return dto;
  }

  protected OnboardDatasetMetricDTO getTestOnboardConfig(String datasetName, String metricName, String dataSource) {
    OnboardDatasetMetricDTO dto = new OnboardDatasetMetricDTO();
    dto.setDatasetName(datasetName);
    dto.setMetricName(metricName);
    dto.setDataSource(dataSource);
    return dto;
  }

  protected ConfigDTO getTestConfig(String namespace, String name, Object value) {
    ConfigDTO dto = new ConfigDTO();
    dto.setNamespace(namespace);
    dto.setName(name);
    dto.setValue(value);
    return dto;
  }
}
