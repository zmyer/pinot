package com.linkedin.thirdeye.detection.alert.filter;

import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.detection.DetectionTestUtils.*;


public class LegacyAlertFilterTest {
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final List<Long> PROP_ID_VALUE = Arrays.asList(1001L, 1002L);
  private static final String PROP_LEGACY_ALERT_FILTER_CONFIG = "legacyAlertFilterConfig";
  private static final String PROP_LEGACY_ALERT_CONFIG = "legacyAlertConfig";
  private static final String PROP_LEGACY_ALERT_FILTER_CLASS_NAME = "legacyAlertFilterClassName";
  private static final String RECIPIENTS_VALUES = "test@example.com,mytest@example.org";

  private List<MergedAnomalyResultDTO> detectedAnomalies;
  private LegacyAlertFilter legacyAlertFilter;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    this.detectedAnomalies = new ArrayList<>();
    this.detectedAnomalies.add(makeAnomaly(1001L, 1500, 2000));
    this.detectedAnomalies.add(makeAnomaly(1001L, 0, 1000));
    this.detectedAnomalies.add(makeAnomaly(1002L, 0, 1000));
    this.detectedAnomalies.add(makeAnomaly(1002L, 1100, 1500));
    this.detectedAnomalies.add(makeAnomaly(1002L, 3333, 9999));
    this.detectedAnomalies.add(makeAnomaly(1003L, 1100, 1500));

    DataProvider mockDataProvider = new MockDataProvider().setAnomalies(this.detectedAnomalies);

    DetectionAlertConfigDTO detectionAlertConfig = new DetectionAlertConfigDTO();
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_DETECTION_CONFIG_IDS, PROP_ID_VALUE);
    Map<String, Object> alertConfig = new HashMap<>();
    alertConfig.put("recipients", RECIPIENTS_VALUES);
    properties.put(PROP_LEGACY_ALERT_CONFIG, alertConfig);
    properties.put(PROP_LEGACY_ALERT_FILTER_CLASS_NAME, "com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter");
    properties.put(PROP_LEGACY_ALERT_FILTER_CONFIG, "");
    detectionAlertConfig.setProperties(properties);

    detectionAlertConfig.setVectorClocks(new HashMap<Long, Long>());

    this.legacyAlertFilter = new LegacyAlertFilter(mockDataProvider, detectionAlertConfig, 2500L);
  }

  @Test
  public void testRun() throws Exception {
    DetectionAlertFilterResult result = this.legacyAlertFilter.run();
    Assert.assertEquals(result.getResult().get(new HashSet<>(Arrays.asList("test@example.com", "mytest@example.org"))),
        new HashSet<>(this.detectedAnomalies.subList(0, 4)));
  }
}
