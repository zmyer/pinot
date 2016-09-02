package com.linkedin.thirdeye.datalayer.dao;

import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.AbstractBaseTest;
import com.linkedin.thirdeye.datalayer.entity.AnomalyFunction;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAnomalyFunctionDAO extends AbstractBaseTest {

  private Long anomalyFunctionId;
  private static String collection = "my dataset";
  private static String metricName = "__counts";

  @Test
  public void testCreate() {
    anomalyFunctionId = anomalyFunctionDAO.save(getTestAnomalyFunction(metricName, collection));
    Assert.assertNotNull(anomalyFunctionId);

    // test fetch all
    List<AnomalyFunction> functions = anomalyFunctionDAO.findAll();
    Assert.assertEquals(functions.size(), 1);
  }

  private AnomalyFunction getTestAnomalyFunction(String metricName2, String collection2) {
    AnomalyFunction function = new AnomalyFunction();
    function.setMetricFunction(MetricAggFunction.SUM);
    function.setMetricId(1L);
    function.setMetric(metricName);
    function.setBucketSize(5);
    function.setCollection(collection);
    function.setBucketUnit(TimeUnit.MINUTES);
    function.setCron("0 0/5 * * * ?");
    function.setName("my awesome test function");
    function.setType("USER_RULE");
    function.setWindowDelay(1);
    function.setWindowDelayUnit(TimeUnit.HOURS);
    function.setWindowSize(10);
    function.setWindowUnit(TimeUnit.HOURS);
    return function;
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAllByCollection() {
    List<AnomalyFunction> functions = anomalyFunctionDAO.findAllByCollection(collection);
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testFindAllByCollection"})
  public void testDistinctMetricsByCollection() {
    List<String> metrics = anomalyFunctionDAO.findDistinctMetricsByCollection(collection);
    Assert.assertEquals(metrics.get(0), metricName);
  }

  @Test(dependsOnMethods = {"testDistinctMetricsByCollection"})
  public void testUpdate() {
    AnomalyFunction spec = anomalyFunctionDAO.findById(anomalyFunctionId);
    Assert.assertNotNull(spec);
    Assert.assertEquals(spec.getMetricFunction(), MetricAggFunction.SUM);
    spec.setMetricFunction(MetricAggFunction.COUNT);
    anomalyFunctionDAO.save(spec);
    AnomalyFunction specReturned = anomalyFunctionDAO.findById(anomalyFunctionId);
    Assert.assertEquals(specReturned.getMetricFunction(), MetricAggFunction.COUNT);
  }

  @Test(dependsOnMethods = {"testUpdate"})
  public void testDelete() {
    anomalyFunctionDAO.deleteById(anomalyFunctionId);
    AnomalyFunction spec = anomalyFunctionDAO.findById(anomalyFunctionId);
    Assert.assertNull(spec);
  }
}
