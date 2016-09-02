package com.linkedin.thirdeye.datalayer.dao;

import com.linkedin.thirdeye.datalayer.entity.AnomalyFunction;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class AnomalyFunctionDAO extends AbstractBaseDAO<AnomalyFunction> {

  public AnomalyFunctionDAO() {
    super(AnomalyFunction.class);
  }

  public List<AnomalyFunction> findAllByCollection(String collection) {
    return super.findByParams(ImmutableMap.of("collection", collection));
  }

  public List<String> findDistinctMetricsByCollection(String collection) {
    Set<String> uniqueMetricSet = new TreeSet<>();
    List<AnomalyFunction> findByParams =
        super.findByParams(ImmutableMap.of("collection", collection));
    for (AnomalyFunction AnomalyFunction : findByParams) {
      uniqueMetricSet.add(AnomalyFunction.getMetric());
    }
    return new ArrayList<>(uniqueMetricSet);
  }

  public List<AnomalyFunction> findAllActiveFunctions() {
    return super.findByParams(ImmutableMap.of("isActive", true));
  }

}
