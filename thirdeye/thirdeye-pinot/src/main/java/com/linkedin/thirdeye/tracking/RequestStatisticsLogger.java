package com.linkedin.thirdeye.tracking;

import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import com.linkedin.thirdeye.api.TimeGranularity;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RequestStatisticsLogger implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RequestStatisticsLogger.class);

  private ScheduledExecutorService scheduledExecutorService;
  private TimeGranularity runFrequency;

  public RequestStatisticsLogger(TimeGranularity runFrequency) {
    this.runFrequency = runFrequency;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void run() {
    try {
      long timestamp = System.nanoTime();
      RequestStatistics stats = ThirdeyeMetricsUtil.getRequestLog().getStatistics(timestamp);
      ThirdeyeMetricsUtil.getRequestLog().truncate(timestamp);

      RequestStatisticsFormatter formatter = new RequestStatisticsFormatter();
      LOG.info("Recent request performance statistics:\n{}", formatter.format(stats));
    } catch (Exception e) {
      LOG.error("Could not generate statistics", e);
    }
  }

  public void start() {
    LOG.info("starting logger");
    this.scheduledExecutorService.scheduleWithFixedDelay(this,
        this.runFrequency.getSize(), this.runFrequency.getSize(), this.runFrequency.getUnit());
  }

  public void shutdown() {
    LOG.info("stopping logger");
    this.scheduledExecutorService.shutdown();
  }
}
