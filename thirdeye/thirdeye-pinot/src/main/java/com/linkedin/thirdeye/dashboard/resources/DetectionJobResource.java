package com.linkedin.thirdeye.dashboard.resources;

import static com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType.ALERT_FILTER_LOGISITC_AUTO_TUNE;
import static com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType.INITIATE_ALERT_FILTER_LOGISTIC_AUTO_TUNE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import com.linkedin.thirdeye.anomaly.detection.lib.FunctionReplayRunnable;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutoTune;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluateHelper;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.email.filter.PrecisionRecallEvaluator;
import com.linkedin.thirdeye.util.SeverityComputationUtil;


@Path("/detection-job")
@Produces(MediaType.APPLICATION_JSON)
public class DetectionJobResource {
  private final DetectionJobScheduler detectionJobScheduler;
  private final AnomalyFunctionManager anomalyFunctionSpecDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final RawAnomalyResultManager rawAnomalyResultDAO;
  private final AutotuneConfigManager autotuneConfigDAO;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private final AlertFilterAutotuneFactory alertFilterAutotuneFactory;
  private final AlertFilterFactory alertFilterFactory;

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobResource.class);

  public DetectionJobResource(DetectionJobScheduler detectionJobScheduler, AlertFilterFactory alertFilterFactory, AlertFilterAutotuneFactory alertFilterAutotuneFactory) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.anomalyFunctionSpecDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.rawAnomalyResultDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
    this.autotuneConfigDAO = DAO_REGISTRY.getAutotuneConfigDAO();
    this.alertFilterAutotuneFactory = alertFilterAutotuneFactory;
    this.alertFilterFactory = alertFilterFactory;
  }


  // Toggle Function Activation is redundant to endpoints defined in AnomalyResource
  @Deprecated
  @POST
  @Path("/{id}")
  public Response enable(@PathParam("id") Long id) throws Exception {
    toggleActive(id, true);
    return Response.ok().build();
  }

  @Deprecated
  @DELETE
  @Path("/{id}")
  public Response disable(@PathParam("id") Long id) throws Exception {
    toggleActive(id, false);
    return Response.ok().build();
  }

  @Deprecated
  private void toggleActive(Long id, boolean state) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new NullArgumentException("Function spec not found");
    }
    anomalyFunctionSpec.setIsActive(state);
    anomalyFunctionSpecDAO.update(anomalyFunctionSpec);
  }

  // endpoints to modify to aonmaly detection function
  // show remove to anomalyResource
  @POST
  @Path("/requiresCompletenessCheck/enable/{id}")
  public Response enableRequiresCompletenessCheck(@PathParam("id") Long id) throws Exception {
    toggleRequiresCompletenessCheck(id, true);
    return Response.ok().build();
  }

  @POST
  @Path("/requiresCompletenessCheck/disable/{id}")
  public Response disableRequiresCompletenessCheck(@PathParam("id") Long id) throws Exception {
    toggleRequiresCompletenessCheck(id, false);
    return Response.ok().build();
  }

  private void toggleRequiresCompletenessCheck(Long id, boolean state) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if(anomalyFunctionSpec == null) {
      throw new NullArgumentException("Function spec not found");
    }
    anomalyFunctionSpec.setRequiresCompletenessCheck(state);
    anomalyFunctionSpecDAO.update(anomalyFunctionSpec);
  }

  @POST
  @Path("/{id}/ad-hoc")
  public Response adHoc(@PathParam("id") Long id, @QueryParam("start") String startTimeIso,
                        @QueryParam("end") String endTimeIso) throws Exception {
    Long startTime = null;
    Long endTime = null;
    if (StringUtils.isBlank(startTimeIso) || StringUtils.isBlank(endTimeIso)) {
      throw new IllegalStateException("startTimeIso and endTimeIso must not be null");
    }
    startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
    detectionJobScheduler.runAdhocAnomalyFunction(id, startTime, endTime);
    return Response.ok().build();
  }

  /**
   * Returns the weight of the metric at the given window. The calculation of baseline (history) data is specified by
   * seasonal period (in days) and season count. Seasonal period is the difference of duration from one window to the
   * other. For instance, to use the data that is one week before current window, set seasonal period to 7. The season
   * count specify how many seasons of history data to retrieve. If there are more than 1 season, then the baseline is
   * the average of all seasons.
   *
   * Examples of the configuration of baseline:
   * 1. Week-Over-Week: seasonalPeriodInDays = 7, seasonCount = 1
   * 2. Week-Over-4-Weeks-Mean: seasonalPeriodInDays = 7, seasonCount = 4
   * 3. Month-Over-Month: seasonalPeriodInDays = 30, seasonCount = 1
   *
   * @param collectionName the collection to which the metric belong
   * @param metricName the metric name
   * @param startTimeIso start time of current window, inclusive
   * @param endTimeIso end time of current window, exclusive
   * @param seasonalPeriodInDays the difference of duration between the start time of each window
   * @param seasonCount the number of history windows
   *
   * @return the weight of the metric at the given window
   * @throws Exception
   */
  @POST
  @Path("/anomaly-weight")
  public Response computeSeverity(@NotNull @QueryParam("collection") String collectionName,
      @NotNull @QueryParam("metric") String metricName,
      @NotNull @QueryParam("start") String startTimeIso, @NotNull @QueryParam("end") String endTimeIso,
      @QueryParam("period") String seasonalPeriodInDays, @QueryParam("seasonCount") String seasonCount)
      throws Exception {
    DateTime startTime = null;
    DateTime endTime = null;
    if (StringUtils.isNotBlank(startTimeIso)) {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    }
    if (StringUtils.isNotBlank(endTimeIso)) {
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
    }
    long currentWindowStart = startTime.getMillis();
    long currentWindowEnd = endTime.getMillis();

    // Default is using one week data priors current values for calculating weight
    long seasonalPeriodMillis = TimeUnit.DAYS.toMillis(7);
    if (StringUtils.isNotBlank(seasonalPeriodInDays)) {
      seasonalPeriodMillis = TimeUnit.DAYS.toMillis(Integer.parseInt(seasonalPeriodInDays));
    }
    int seasonCountInt = 1;
    if (StringUtils.isNotBlank(seasonCount)) {
      seasonCountInt = Integer.parseInt(seasonCount);
    }

    SeverityComputationUtil util = new SeverityComputationUtil(collectionName, metricName);
    Map<String, Object> severity =
        util.computeSeverity(currentWindowStart, currentWindowEnd, seasonalPeriodMillis, seasonCountInt);

    return Response.ok(severity.toString(), MediaType.TEXT_PLAIN_TYPE).build();
  }

  /**
   * Breaks down the given range into consecutive monitoring windows as per function definition
   * Regenerates anomalies for each window separately
   *
   * As the anomaly result regeneration is a heavy job, we move the function from Dashboard to worker
   * @param id an anomaly function id
   * @param startTimeIso The start time of the monitoring window (in ISO Format), ex: 2016-5-23T00:00:00Z
   * @param endTimeIso The start time of the monitoring window (in ISO Format)
   * @param isForceBackfill false to resume backfill from the latest left off
   * @param speedup
   *      whether this backfill should speedup with 7-day window. The assumption is that the functions are using
   *      WoW-based algorithm, or Seasonal Data Model.
   * @return HTTP response of this request with a job execution id
   * @throws Exception
   */
  @POST
  @Path("/{id}/replay")
  public Response generateAnomaliesInRange(@PathParam("id") @NotNull final long id,
      @QueryParam("start") @NotNull String startTimeIso,
      @QueryParam("end") @NotNull String endTimeIso,
      @QueryParam("force") @DefaultValue("false") String isForceBackfill,
      @QueryParam("speedup") @DefaultValue("false") final Boolean speedup) throws Exception {
    Response response = generateAnomaliesInRangeForFunctions(Long.toString(id), startTimeIso, endTimeIso,
        isForceBackfill, speedup);
    // As there is only one function id, simplify the response message to a single job execution id
    Map<Long, Long> entity = (Map<Long, Long>) response.getEntity();
    return Response.status(response.getStatus()).entity(entity.values()).build();
  }

  /**
   * Breaks down the given range into consecutive monitoring windows as per function definition
   * Regenerates anomalies for each window separately
   *
   * Different from the previous replay function, this replay function takes multiple function ids, and is able to send
   * out alerts to user once the replay is done.
   *
   * Enable replay on inactive function, but still keep the original function status after replay
   * If the anomaly function has historical anomalies, will only remove anomalies within the replay period, making replay capable to take historical information
   *
   *  As the anomaly result regeneration is a heavy job, we move the function from Dashboard to worker
   * @param ids a string containing multiple anomaly function ids, separated by comma (e.g. f1,f2,f3)
   * @param startTimeIso The start time of the monitoring window (in ISO Format), ex: 2016-5-23T00:00:00Z
   * @param endTimeIso The start time of the monitoring window (in ISO Format)
   * @param isForceBackfill false to resume backfill from the latest left off
   * @param speedup
   *      whether this backfill should speedup with 7-day window. The assumption is that the functions are using
   *      WoW-based algorithm, or Seasonal Data Model.
   * @return HTTP response of this request with a map from function id to its job execution id
   * @throws Exception
   */
  @POST
  @Path("/replay")
  public Response generateAnomaliesInRangeForFunctions(@QueryParam("ids") @NotNull String ids,
      @QueryParam("start") @NotNull String startTimeIso,
      @QueryParam("end") @NotNull String endTimeIso,
      @QueryParam("force") @DefaultValue("false") String isForceBackfill,
      @QueryParam("speedup") @DefaultValue("false") final Boolean speedup) throws Exception {
    final boolean forceBackfill = Boolean.valueOf(isForceBackfill);
    final List<Long> functionIdList = new ArrayList<>();
    final Map<Long, Long> detectionJobIdMap = new HashMap<>();
    for (String functionId : ids.split(",")) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(Long.valueOf(functionId));
      if (anomalyFunction != null) {
        functionIdList.add(Long.valueOf(functionId));
      } else {
        LOG.warn("[Backfill] Unable to load function id {}", functionId);
      }
    }
    if (functionIdList.isEmpty()) {
      return Response.noContent().build();
    }

    // Check if the timestamps are available
    DateTime startTime = null;
    DateTime endTime = null;
    if (startTimeIso == null || startTimeIso.isEmpty()) {
      LOG.error("[Backfill] Monitoring start time is not found");
      throw new IllegalArgumentException(String.format("[Backfill] Monitoring start time is not found"));
    }
    if (endTimeIso == null || endTimeIso.isEmpty()) {
      LOG.error("[Backfill] Monitoring end time is not found");
      throw new IllegalArgumentException(String.format("[Backfill] Monitoring end time is not found"));
    }

    startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);

    if (startTime.isAfter(endTime)) {
      LOG.error("[Backfill] Monitoring start time is after monitoring end time");
      throw new IllegalArgumentException(String.format(
          "[Backfill] Monitoring start time is after monitoring end time"));
    }
    if (endTime.isAfterNow()) {
      endTime = DateTime.now();
      LOG.warn("[Backfill] End time is in the future. Force to now.");
    }

    final Map<Long, Integer> originalWindowSize = new HashMap<>();
    final Map<Long, TimeUnit> originalWindowUnit = new HashMap<>();
    final Map<Long, String> originalCron = new HashMap<>();
    saveFunctionWindow(functionIdList, originalWindowSize, originalWindowUnit, originalCron);

    // Update speed-up window and cron
    if (speedup) {
      for (long functionId : functionIdList) {
        anomalyFunctionSpeedup(functionId);
      }
    }

    // Run backfill : for each function, set to be active to enable runBackfill,
    //                remove existing anomalies if there is any already within the replay window (to avoid duplicated anomaly results)
    //                set the original activation status back after backfill
    for (long functionId : functionIdList) {
      // Activate anomaly function if it's inactive
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      Boolean isActive = anomalyFunction.getIsActive();
      if (!isActive) {
        anomalyFunction.setActive(true);
        anomalyFunctionDAO.update(anomalyFunction);
      }
      // remove existing anomalies within same replay window
      OnboardResource onboardResource = new OnboardResource();
      onboardResource.deleteExistingAnomalies(functionId, startTime.getMillis(), endTime.getMillis());
      // run backfill
      long detectionJobId = detectionJobScheduler.runBackfill(functionId, startTime, endTime, forceBackfill);
      // Put back activation status
      anomalyFunction.setActive(isActive);
      anomalyFunctionDAO.update(anomalyFunction);
      detectionJobIdMap.put(functionId, detectionJobId);
    }

    new Thread(new Runnable() {
      @Override
      public void run() {
        for(long detectionJobId : detectionJobIdMap.values()) {
          detectionJobScheduler.waitForJobDone(detectionJobId);
        }
        // Revert window setup
        revertFunctionWindow(functionIdList, originalWindowSize, originalWindowUnit, originalCron);
      }
    }).run();

    return Response.ok(detectionJobIdMap).build();
  }

  /**
   * Under current infrastructure, we are not able to determine whether and how we accelerate the backfill.
   * Currently, the requirement for speedup is to increase the window up to 1 week.
   * For WoW-based models, if we enlarge the window to 7 days, it can significantly increase the backfill speed.
   * Now, the hard-coded code is a contemporary solution to this problem. It can be fixed under new infra.
   *
   * TODO Data model provide information on how the function can speed up, and user determines if they wnat to speed up
   * TODO the replay
   * @param functionId
   */
  private void anomalyFunctionSpeedup (long functionId) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    anomalyFunction.setWindowSize(170);
    anomalyFunction.setWindowUnit(TimeUnit.HOURS);
    anomalyFunction.setCron("0 0 0 ? * MON *");
    anomalyFunctionDAO.update(anomalyFunction);
  }

  private void saveFunctionWindow(List<Long> functionIdList, Map<Long, Integer> windowSize,
      Map<Long, TimeUnit> windowUnit, Map<Long, String> cron) {
    for (long functionId : functionIdList) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      windowSize.put(functionId, anomalyFunction.getWindowSize());
      windowUnit.put(functionId, anomalyFunction.getWindowUnit());
      cron.put(functionId, anomalyFunction.getCron());
    }
  }

  private void revertFunctionWindow(List<Long> functionIdList, Map<Long, Integer> windowSize,
      Map<Long, TimeUnit> windowUnit, Map<Long, String> cron) {
    for (long functionId : functionIdList) {
      AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
      if (windowSize.containsKey(functionId)) {
        anomalyFunction.setWindowSize(windowSize.get(functionId));
      }
      if (windowUnit.containsKey(functionId)) {
        anomalyFunction.setWindowUnit(windowUnit.get(functionId));
      }
      if (cron.containsKey(functionId)) {
        anomalyFunction.setCron(cron.get(functionId));
      }
      anomalyFunctionDAO.update(anomalyFunction);
    }
  }

  @POST
  @Path("/{id}/offlineAnalysis")
  public Response generateAnomaliesInTrainingData(@PathParam("id") @NotNull long id,
      @QueryParam("time") String analysisTimeIso) throws Exception {

    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    if (anomalyFunction == null) {
      return Response.noContent().build();
    }

    DateTime analysisTime = DateTime.now();
    if (StringUtils.isNotEmpty(analysisTimeIso)) {
      analysisTime = ISODateTimeFormat.dateTimeParser().parseDateTime(analysisTimeIso);
    }

    Long jobId = detectionJobScheduler.runOfflineAnalysis(id, analysisTime);
    List<Long> anomalyIds = new ArrayList<>();
    if (jobId == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity("Anomaly function " + Long.toString(id)
          + " is inactive. Or thread is interrupted.").build();
    } else {
      JobStatus jobStatus = detectionJobScheduler.waitForJobDone(jobId);
      if(jobStatus.equals(JobStatus.FAILED)) {
        return Response.status(Response.Status.NO_CONTENT).entity("Detection job failed").build();
      } else {
        List<MergedAnomalyResultDTO> mergedAnomalies = mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(
            0, analysisTime.getMillis(), id, true);
        for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
          anomalyIds.add(mergedAnomaly.getId());
        }
      }
    }

    return Response.ok(anomalyIds).build();
  }

  /**
   * Given a list of holiday starts and holiday ends, return merged anomalies with holidays removed
   * @param functionId: the functionId to fetch merged anomalies with holidays removed
   * @param startTime: start time in milliseconds of merged anomalies
   * @param endTime: end time of in milliseconds merged anomalies
   * @param holidayStarts: holidayStarts in ISO Format ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds: holidayEnds in in ISO Format ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @return a list of merged anomalies with holidays removed
   */
  public static List<MergedAnomalyResultDTO> getMergedAnomaliesRemoveHolidays(long functionId, long startTime, long endTime, String holidayStarts, String holidayEnds) {
    StringTokenizer starts = new StringTokenizer(holidayStarts, ",");
    StringTokenizer ends = new StringTokenizer(holidayEnds, ",");
    MergedAnomalyResultManager anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    List<MergedAnomalyResultDTO> totalAnomalies = anomalyMergedResultDAO.findByStartTimeInRangeAndFunctionId(startTime, endTime, functionId, true);
    int origSize = totalAnomalies.size();
    long start;
    long end;
    while (starts.hasMoreElements() && ends.hasMoreElements()) {
      start = ISODateTimeFormat.dateTimeParser().parseDateTime(starts.nextToken()).getMillis();
      end = ISODateTimeFormat.dateTimeParser().parseDateTime(ends.nextToken()).getMillis();
      List<MergedAnomalyResultDTO> holidayMergedAnomalies = anomalyMergedResultDAO.findByStartTimeInRangeAndFunctionId(start, end, functionId, true);
      totalAnomalies.removeAll(holidayMergedAnomalies);
    }
    if(starts.hasMoreElements() || ends.hasMoreElements()) {
      LOG.warn("Input holiday starts and ends length not equal!");
    }
    LOG.info("Removed {} merged anomalies", origSize - totalAnomalies.size());
    return totalAnomalies;
  }


  /**
   *
   * @param id anomaly function id
   * @param startTimeIso start time of anomalies to tune alert filter in ISO format ex: 2016-5-23T00:00:00Z
   * @param endTimeIso end time of anomalies to tune alert filter in ISO format ex: 2016-5-23T00:00:00Z
   * @param autoTuneType the type of auto tune to invoke (default is "AUTOTUNE")
   * @param holidayStarts: holidayStarts in ISO Format, ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds: holidayEnds in in ISO Format, ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @return HTTP response of request: string of alert filter
   */
  @POST
  @Path("/autotune/filter/{functionId}")
  public Response tuneAlertFilter(@PathParam("functionId") long id,
      @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso,
      @QueryParam("autoTuneType") @DefaultValue("AUTOTUNE") String autoTuneType,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();

    // get anomalies by function id, start time and end time
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(id);
    List<MergedAnomalyResultDTO> anomalyResultDTOS = getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);
    AutotuneConfigDTO target = new AutotuneConfigDTO();

    // create alert filter and evaluator
    AlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    PrecisionRecallEvaluator evaluator = new PrecisionRecallEvaluator(alertFilter, null);

    // create alert filter auto tune
    AlertFilterAutoTune alertFilterAutotune = alertFilterAutotuneFactory.fromSpec(autoTuneType);
    LOG.info("initiated alertFilterAutoTune of Type {}", alertFilterAutotune.getClass().toString());
    long autotuneId = -1;
    double currPrecision = 0;
    double currRecall = 0;
    try {
      //evaluate current alert filter (calculate current precision and recall)
      evaluator.init(anomalyResultDTOS);
      LOG.info("AlertFilter of Type {}, has been evaluated with precision: {}, recall: {}", alertFilter.getClass().toString(), evaluator.getPrecision(), evaluator.getRecall());
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }
    currPrecision = evaluator.getWeightedPrecision();
    currRecall = evaluator.getRecall();
    try {
      // get tuned alert filter
      Map<String, String> tunedAlertFilter = alertFilterAutotune.tuneAlertFilter(anomalyResultDTOS, currPrecision, currRecall);
      LOG.info("tuned AlertFilter");

      // if alert filter auto tune has updated alert filter, write to autotune_config_index, and get the autotuneId
      // otherwise do nothing and return alert filter
      if (alertFilterAutotune.isUpdated()) {
        target.setFunctionId(id);
        target.setAutotuneMethod(ALERT_FILTER_LOGISITC_AUTO_TUNE);
        target.setConfiguration(tunedAlertFilter);
        target.setPerformanceEvaluationMethod(PerformanceEvaluationMethod.PRECISION_AND_RECALL);
        autotuneId = DAO_REGISTRY.getAutotuneConfigDAO().save(target);
        LOG.info("Model has been updated");
      } else {
        LOG.info("Model hasn't been updated because tuned model cannot beat original model");
      }
    } catch (Exception e) {
      LOG.warn("AutoTune throws exception due to: {}", e.getMessage());
    }
    return Response.ok(autotuneId).build();
  }

  /**
   * Endpoint to check if merged anomalies given a time period have at least one positive label
   * @param id functionId to test anomalies
   * @param startTimeIso start time to check anomaly history labels in ISO format ex: 2016-5-23T00:00:00Z
   * @param endTimeIso end time to check anomaly history labels in ISO format ex: 2016-5-23T00:00:00Z
   * @param holidayStarts optional: holidayStarts in ISO format as string, ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds optional:holidayEnds in ISO format as string, ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @return true if the list of merged anomalies has at least one positive label, false otherwise
   */
  @POST
  @Path("/initautotune/checkhaslabel/{functionId}")
  public Response checkAnomaliesHasLabel(@PathParam("functionId") long id,
      @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {
    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
    List<MergedAnomalyResultDTO> anomalyResultDTOS = getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);
    return Response.ok(AnomalyUtils.checkHasLabels(anomalyResultDTOS)).build();
  }

  /**
   * End point to trigger initiate alert filter auto tune
   * @param id functionId to initiate alert filter auto tune
   * @param startTimeIso: training data starts time ex: 2016-5-23T00:00:00Z
   * @param endTimeIso: training data ends time ex: 2016-5-23T00:00:00Z
   * @param autoTuneType: By default is "AUTOTUNE"
   * @param nExpectedAnomalies: number of expected anomalies to recommend users to label
   * @param holidayStarts optional: holidayStarts in ISO format: start1,start2,...
   * @param holidayEnds optional:holidayEnds in ISO format: end1,end2,...
   * @return true if alert filter has successfully being initiated, false otherwise
   */
  @POST
  @Path("/initautotune/filter/{functionId}")
  public Response initiateAlertFilterAutoTune(@PathParam("functionId") long id,
      @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso,
      @QueryParam("autoTuneType") @DefaultValue("AUTOTUNE") String autoTuneType, @QueryParam("nExpected") int nExpectedAnomalies,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();

    // get anomalies by function id, start time and end time
    List<MergedAnomalyResultDTO> anomalyResultDTOS = getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);

    //initiate AutoTuneConfigDTO
    AutotuneConfigDTO target = new AutotuneConfigDTO();

    // create alert filter auto tune
    AlertFilterAutoTune alertFilterAutotune = alertFilterAutotuneFactory.fromSpec(autoTuneType);
    long autotuneId = -1;
    try {
      Map<String, String> tunedAlertFilter = alertFilterAutotune.initiateAutoTune(anomalyResultDTOS, nExpectedAnomalies);

      // if achieved the initial alert filter
      if (alertFilterAutotune.isUpdated()) {
        target.setFunctionId(id);
        target.setConfiguration(tunedAlertFilter);
        target.setAutotuneMethod(INITIATE_ALERT_FILTER_LOGISTIC_AUTO_TUNE);
        autotuneId = DAO_REGISTRY.getAutotuneConfigDAO().save(target);
      } else {
        LOG.info("Failed init alert filter since model hasn't been updated");
      }
    } catch (Exception e) {
      LOG.warn("Failed to achieve init alert filter: {}", e.getMessage());
    }
    return Response.ok(autotuneId).build();
  }


  /**
   * The endpoint to evaluate alert filter
   * @param id: function ID
   * @param startTimeIso: startTime of merged anomaly ex: 2016-5-23T00:00:00Z
   * @param endTimeIso: endTime of merged anomaly ex: 2016-5-23T00:00:00Z
   * @return feedback summary, precision and recall as json object
   * @throws Exception when data has no positive label or model has no positive prediction
   */
  @POST
  @Path("/eval/filter/{functionId}")
  public Response evaluateAlertFilterByFunctionId(@PathParam("functionId") long id,
      @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();

    // get anomalies by function id, start time and end time`
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(id);
    List<MergedAnomalyResultDTO> anomalyResultDTOS =
        getMergedAnomaliesRemoveHolidays(id, startTime, endTime, holidayStarts, holidayEnds);

    // create alert filter and evaluator
    AlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    //evaluate current alert filter (calculate current precision and recall)
    PrecisionRecallEvaluator evaluator = new PrecisionRecallEvaluator(alertFilter, anomalyResultDTOS);

    LOG.info("AlertFilter of Type {}, has been evaluated with precision: {}, recall:{}", alertFilter.getClass().toString(),
        evaluator.getWeightedPrecision(), evaluator.getRecall());

    return Response.ok(evaluator.toProperties().toString()).build();
  }

  /**
   * To evaluate alert filte directly by autotune Id using autotune_config_index table
   * This is to leverage the intermediate step before updating tuned alert filter configurations
   * @param id: autotune Id
   * @param startTimeIso: merged anomalies start time. ex: 2016-5-23T00:00:00Z
   * @param endTimeIso: merged anomalies end time  ex: 2016-5-23T00:00:00Z
   * @param holidayStarts: holiday starts time to remove merged anomalies in ISO format. ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds: holiday ends time to remove merged anomlaies in ISO format. ex: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @return HTTP response of evaluation results
   */
  @POST
  @Path("/eval/autotune/{autotuneId}")
  public Response evaluateAlertFilterByAutoTuneId(@PathParam("autotuneId") long id,
      @QueryParam("start") String startTimeIso, @QueryParam("end") String endTimeIso,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    long startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
    long endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();


    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    long functionId = target.getFunctionId();
    List<MergedAnomalyResultDTO> anomalyResultDTOS =
        getMergedAnomaliesRemoveHolidays(functionId, startTime, endTime, holidayStarts, holidayEnds);

    Map<String, String> alertFilterParams = target.getConfiguration();
    AlertFilter alertFilter = alertFilterFactory.fromSpec(alertFilterParams);
    PrecisionRecallEvaluator evaluator = new PrecisionRecallEvaluator(alertFilter, anomalyResultDTOS);

    return Response.ok(evaluator.toProperties().toString()).build();
  }


  /**
   * Perform anomaly function autotune:
   *  - run backfill on all possible combinations of tuning parameters
   *  - keep all the parameter combinations which lie in the goal range
   *  - return list of parameter combinations along with their performance evaluation
   * @param functionId
   * the id of the target anomaly function
   * @param  replayTimeIso
   * the end time of the anomaly function replay in ISO format, e.g. 2017-02-27T00:00:00.000Z
   * @param replayDuration
   * the duration of the replay ahead of the replayStartTimeIso
   * @param durationUnit
   * the time unit of the duration, DAYS, HOURS, MINUTES and so on
   * @param speedup
   * whether we speedup the replay process
   * @param tuningJSON
   * the json object includes all tuning fields and list of parameters
   * ex: {"baselineLift": [0.9, 0.95, 1, 1.05, 1.1], "baselineSeasonalPeriod": [2, 3, 4]}
   * @param goal
   * the expected performance assigned by user
   * @param includeOrigin
   * to include the performance of original setup into comparison
   * If we perform offline analysis before hand, we don't get the correct performance about the current configuration
   * setup. Therefore, we need to exclude the performance from the comparison.
   * @return
   * A response containing all satisfied properties with their evaluation result
   */
  @Deprecated
  @POST
  @Path("replay/function/{id}")
  public Response anomalyFunctionReplay(@PathParam("id") @NotNull long functionId,
      @QueryParam("time") String replayTimeIso, @QueryParam("duration") @DefaultValue("30") int replayDuration,
      @QueryParam("durationUnit") @DefaultValue("DAYS") String durationUnit,
      @QueryParam("speedup") @DefaultValue("true") boolean speedup,
      @QueryParam("tune") @DefaultValue("{\"pValueThreshold\":[0.05, 0.01]}") String tuningJSON,
      @QueryParam("goal") @DefaultValue("0.05") double goal,
      @QueryParam("includeOriginal") @DefaultValue("true") boolean includeOrigin,
      @QueryParam("evalMethod") @DefaultValue("ANOMALY_PERCENTAGE") String performanceEvaluationMethod) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if (anomalyFunction == null) {
      LOG.warn("Unable to find anomaly function {}", functionId);
      return Response.status(Response.Status.BAD_REQUEST).entity("Cannot find function").build();
    }
    DateTime replayStart = null;
    DateTime replayEnd = null;
    try {
      TimeUnit timeUnit = TimeUnit.valueOf(durationUnit.toUpperCase());

      TimeGranularity timeGranularity = new TimeGranularity(replayDuration, timeUnit);
      replayEnd = DateTime.now();
      if (StringUtils.isNotEmpty(replayTimeIso)) {
        replayEnd = ISODateTimeFormat.dateTimeParser().parseDateTime(replayTimeIso);
      }
      replayStart = replayEnd.minus(timeGranularity.toPeriod());
    } catch (Exception e) {
      throw new WebApplicationException("Unable to parse strings, " + replayTimeIso
          + ", in ISO DateTime format", e);
    }

    // List all tuning parameter sets
    List<Map<String, String>> tuningParameters = null;
    try {
      tuningParameters = listAllTuningParameters(new JSONObject(tuningJSON));
    } catch(JSONException e) {
      LOG.error("Unable to parse json string: {}", tuningJSON, e );
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    if (tuningParameters.size() == 0) { // no tuning combinations
      LOG.warn("No tuning parameter is found in json string {}", tuningJSON);
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    AutotuneMethodType autotuneMethodType = AutotuneMethodType.EXHAUSTIVE;
    PerformanceEvaluationMethod performanceEvalMethod = PerformanceEvaluationMethod.valueOf(performanceEvaluationMethod.toUpperCase());

    Map<String, Double> originalPerformance = new HashMap<>();
    originalPerformance.put(performanceEvalMethod.name(),
        PerformanceEvaluateHelper.getPerformanceEvaluator(performanceEvalMethod, functionId, functionId,
            new Interval(replayStart.getMillis(), replayEnd.getMillis()), mergedAnomalyResultDAO).evaluate());

    // select the functionAutotuneConfigDTO in DB
    //TODO: override existing autotune results by a method "autotuneConfigDAO.udpate()"
    AutotuneConfigDTO targetDTO = null;
    List<AutotuneConfigDTO> functionAutoTuneConfigDTOList =
        autotuneConfigDAO.findAllByFuctionIdAndWindow(functionId,replayStart.getMillis(), replayEnd.getMillis());
    for (AutotuneConfigDTO configDTO : functionAutoTuneConfigDTOList) {
      if(configDTO.getAutotuneMethod().equals(autotuneMethodType) &&
          configDTO.getPerformanceEvaluationMethod().equals(performanceEvalMethod) &&
          configDTO.getStartTime() == replayStart.getMillis() && configDTO.getEndTime() == replayEnd.getMillis() &&
          configDTO.getGoal() == goal) {
        targetDTO = configDTO;
        break;
      }
    }

    if (targetDTO == null) {  // Cannot find existing dto
      targetDTO = new AutotuneConfigDTO();
      targetDTO.setFunctionId(functionId);
      targetDTO.setAutotuneMethod(autotuneMethodType);
      targetDTO.setPerformanceEvaluationMethod(performanceEvalMethod);
      targetDTO.setStartTime(replayStart.getMillis());
      targetDTO.setEndTime(replayEnd.getMillis());
      targetDTO.setGoal(goal);
      autotuneConfigDAO.save(targetDTO);
    }

    // clear message;
    targetDTO.setMessage("");
    if (includeOrigin) {
      targetDTO.setPerformance(originalPerformance);
    } else {
      targetDTO.setPerformance(Collections.EMPTY_MAP);
    }
    autotuneConfigDAO.update(targetDTO);

    // Setup threads and start to run
    for(Map<String, String> config : tuningParameters) {
      LOG.info("Running backfill replay with parameter configuration: {}" + config.toString());
      FunctionReplayRunnable backfillRunnable = new FunctionReplayRunnable(detectionJobScheduler, anomalyFunctionDAO,
          mergedAnomalyResultDAO, rawAnomalyResultDAO, autotuneConfigDAO);
      backfillRunnable.setTuningFunctionId(functionId);
      backfillRunnable.setFunctionAutotuneConfigId(targetDTO.getId());
      backfillRunnable.setReplayStart(replayStart);
      backfillRunnable.setReplayEnd(replayEnd);
      backfillRunnable.setForceBackfill(true);
      backfillRunnable.setGoal(goal);
      backfillRunnable.setSpeedUp(speedup);
      backfillRunnable.setPerformanceEvaluationMethod(performanceEvalMethod);
      backfillRunnable.setAutotuneMethodType(autotuneMethodType);
      backfillRunnable.setTuningParameter(config);

      new Thread(backfillRunnable).start();
    }

    return Response.ok(targetDTO.getId()).build();
  }

  /**
   * Parse the jsonobject and list all the possible configuration combinations
   * @param tuningJSON the input json string from user
   * @return full list of all the possible configurations
   * @throws JSONException
   */
  private List<Map<String, String>> listAllTuningParameters(JSONObject tuningJSON) throws JSONException {
    List<Map<String, String>> tuningParameters = new ArrayList<>();
    Iterator<String> jsonFieldIterator = tuningJSON.keys();
    Map<String, List<String>> fieldToParams = new HashMap<>();

    int numPermutations = 1;
    while (jsonFieldIterator.hasNext()) {
      String field = jsonFieldIterator.next();

      if (field != null && !field.isEmpty()) {
        // JsonArray to String List
        List<String> params = new ArrayList<>();
        JSONArray paramArray = tuningJSON.getJSONArray(field);
        if (paramArray.length() == 0) {
          continue;
        }
        for (int i = 0; i < paramArray.length(); i++) {
          params.add(paramArray.get(i).toString());
        }
        numPermutations *= params.size();
        fieldToParams.put(field, params);
      }
    }

    if (fieldToParams.size() == 0) { // No possible tuning parameters
      return tuningParameters;
    }
    List<String> fieldList = new ArrayList<>(fieldToParams.keySet());
    for (int i = 0; i < numPermutations; i++) {
      Map<String, String> combination = new HashMap<>();
      int index = i;
      for (String field : fieldList) {
        List<String> params = fieldToParams.get(field);
        combination.put(field, params.get(index % params.size()));
        index /= params.size();
      }
      tuningParameters.add(combination);
    }

    return tuningParameters;
  }


  /**
   * Single function Reply to generate anomalies given a time range
   * Given anomaly function Id, or auto tuned Id, start time, end time, it clones a function with same configurations and replays from start time to end time
   * Replay function with input auto tuned configurations and save the cloned function
   * @param functionId functionId to be replayed
   * @param autotuneId autotuneId that has auto tuned configurations as well as original functionId. If autotuneId is provided, the replay will apply auto tuned configurations to the auto tuned function
   *                   If both functionId and autotuneId are provided, use autotuneId as principal
   * Either functionId or autotuneId should be not null to provide function information, if functionId is not aligned with autotuneId's function, use all function information from autotuneId
   * @param replayStartTimeIso replay start time in ISO format, e.g. 2017-02-27T00:00:00.000Z, replay start time inclusive
   * @param replayEndTimeIso replay end time, e.g. 2017-02-27T00:00:00.000Z, replay end time exclusive
   * @param speedUp boolean to determine should we speed up the replay process (by maximizing detection window size)
   * @return cloned function Id
   */
  @Deprecated
  @POST
  @Path("/replay/singlefunction")
  public Response replayAnomalyFunctionByFunctionId(@QueryParam("functionId") Long functionId,
      @QueryParam("autotuneId") Long autotuneId,
      @QueryParam("start") @NotNull String replayStartTimeIso,
      @QueryParam("end") @NotNull String replayEndTimeIso,
      @QueryParam("speedUp") @DefaultValue("true") boolean speedUp) {

    if (functionId == null && autotuneId == null) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    DateTime replayStart;
    DateTime replayEnd;
    try {
      replayStart = ISODateTimeFormat.dateTimeParser().parseDateTime(replayStartTimeIso);
      replayEnd = ISODateTimeFormat.dateTimeParser().parseDateTime(replayEndTimeIso);
    } catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST).entity("Input start and end time illegal! ").build();
    }

    FunctionReplayRunnable functionReplayRunnable;
    AutotuneConfigDTO target = null;
    if (autotuneId != null) {
      target = DAO_REGISTRY.getAutotuneConfigDAO().findById(autotuneId);
      functionReplayRunnable = new FunctionReplayRunnable(detectionJobScheduler, anomalyFunctionDAO, mergedAnomalyResultDAO, rawAnomalyResultDAO, target.getConfiguration(), target.getFunctionId(), replayStart, replayEnd, false);
    } else {
      functionReplayRunnable = new FunctionReplayRunnable(detectionJobScheduler, anomalyFunctionDAO, mergedAnomalyResultDAO, rawAnomalyResultDAO, new HashMap<String, String>(), functionId, replayStart, replayEnd, false);
    }
    functionReplayRunnable.setSpeedUp(speedUp);
    functionReplayRunnable.run();

    Map<String, String> responseMessages = new HashMap<>();
    responseMessages.put("cloneFunctionId", String.valueOf(functionReplayRunnable.getLastClonedFunctionId()));
    if (target != null && functionId != null && functionId != target.getFunctionId()) {
      responseMessages.put("Warning", "Input function Id does not consistent with autotune Id's function, use auto tune Id's information instead.");
    }
    return Response.ok(responseMessages).build();
  }


  /**
   * Given alert filter autotune Id, update to function spec
   * @param id alert filte autotune id
   * @return function Id being updated
   */
  @POST
  @Path("/update/filter/{autotuneId}")
  public Response updateAlertFilterToFunctionSpecByAutoTuneId(@PathParam("autotuneId") long id) {
    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    long functionId = target.getFunctionId();
    AnomalyFunctionManager anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(functionId);
    anomalyFunctionSpec.setAlertFilter(target.getConfiguration());
    anomalyFunctionDAO.update(anomalyFunctionSpec);
    return Response.ok(functionId).build();
  }

  /**
   * Extract alert filter boundary given auto tune Id
   * @param id autotune Id
   * @return alert filter boundary in json format
   */
  @POST
  @Path("/eval/autotuneboundary/{autotuneId}")
  public Response getAlertFilterBoundaryByAutoTuneId(@PathParam("autotuneId") long id) {
    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    return Response.ok(target.getConfiguration()).build();
  }

  /**
   * Extract alert filter training data
   * @param id alert filter autotune id
   * @param startTimeIso: alert filter trainig data start time in ISO format: e.g. 2017-02-27T00:00:00.000Z
   * @param endTimeIso: alert filter training data end time in ISO format
   * @param holidayStarts holiday starts time in ISO format to remove merged anomalies: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @param holidayEnds holiday ends time in ISO format to remove merged anomalies: 2016-5-23T00:00:00Z,2016-6-23T00:00:00Z,...
   * @return training data in json format
   */
  @POST
  @Path("/eval/autotunemetadata/{autotuneId}")
  public Response getAlertFilterMetaDataByAutoTuneId(@PathParam("autotuneId") long id,
      @QueryParam("start") String startTimeIso, @QueryParam("end") String endTimeIso,
      @QueryParam("holidayStarts") @DefaultValue("") String holidayStarts,
      @QueryParam("holidayEnds") @DefaultValue("") String holidayEnds) {

    long startTime;
    long endTime;
    try {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
    } catch (Exception e) {
      throw new WebApplicationException("Unable to parse strings, " + startTimeIso + " and " + endTimeIso
          + ", in ISO DateTime format", e);
    }

    AutotuneConfigDTO target = DAO_REGISTRY.getAutotuneConfigDAO().findById(id);
    long functionId = target.getFunctionId();
    List<MergedAnomalyResultDTO> anomalyResultDTOS =
        getMergedAnomaliesRemoveHolidays(functionId, startTime, endTime, holidayStarts, holidayEnds);
    List<AnomalyUtils.MetaDataNode> metaData = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly: anomalyResultDTOS) {
      metaData.add(new AnomalyUtils.MetaDataNode(anomaly));
    }
    return Response.ok(metaData).build();
  }
}
