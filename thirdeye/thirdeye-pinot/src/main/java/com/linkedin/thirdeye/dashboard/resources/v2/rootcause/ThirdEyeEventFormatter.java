package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;
import com.linkedin.thirdeye.rootcause.impl.ThirdEyeEventEntity;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;


public class ThirdEyeEventFormatter extends RootCauseEventEntityFormatter {
  @Override
  public boolean applies(EventEntity entity) {
    return entity instanceof ThirdEyeEventEntity;
  }

  @Override
  public RootCauseEventEntity format(EventEntity entity) {
    ThirdEyeEventEntity thirdEyeEventEntity = (ThirdEyeEventEntity) entity;
    EventDTO eventDto = thirdEyeEventEntity.getDto();

    Multimap<String, String> attributes = ArrayListMultimap.create();

    StringBuilder dimensions = new StringBuilder();
    for(Map.Entry<String, List<String>> entry : eventDto.getTargetDimensionMap().entrySet()) {
      String dimName = entry.getKey();
      dimensions.append(" (");
      dimensions.append(StringUtils.join(entry.getValue(), ","));
      dimensions.append(")");

      attributes.putAll(dimName, entry.getValue());
    }

    String label = String.format("%s%s", eventDto.getName(), dimensions.toString());
    String link = "";

    if (thirdEyeEventEntity.getEventType().equals("holiday")){
      link = String.format("https://www.google.com/search?q=%s", eventDto.getName());
    }

    RootCauseEventEntity out = makeRootCauseEventEntity(entity, label, link, eventDto.getStartTime(), eventDto.getEndTime(), "");
    out.setAttributes(attributes);

    return out;
  }
}
