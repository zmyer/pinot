/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.transform.function;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.query.transform.TransformFunction;

public class TimeConverterFunction {

  private static final DateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
  private static final String TIME_PATTERN = "timepattern";

  static {
    ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public static class ToHoursSinceEpochFromDaysSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 24;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMinutesSinceEpochFromDaysSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 1440;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToSecondsSinceEpochFromDaysSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 86400;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMillisecondsSinceEpochFromDaysSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 86400000L;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToDaysSinceEpochFromHoursSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 24;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMinutesSinceEpochFromHoursSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 60;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToSecondsSinceEpochFromHoursSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 3600;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMillisecondsSinceEpochFromHoursSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 3600000L;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToDaysSinceEpochFromMinutesSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 1440;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToHoursSinceEpochFromMinutesSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 60;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToSecondsSinceEpochFromMinutesSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 60;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMillisecondsSinceEpochFromMinutesSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 60000L;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToDaysSinceEpochFromSecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 86400;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToHoursSinceEpochFromSecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 3600;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMinutesSinceEpochFromSecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 60;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMillisecondsSinceEpochFromSecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() * 1000L;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToDaysSinceEpochFromMillisecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 86400000L;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToHoursSinceEpochFromMillisecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 3600000;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMinutesSinceEpochFromMillisecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 60000;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToSecondsSinceEpochFromMillisecondsSinceEpochConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      return Double.valueOf(input).longValue() / 1000;
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToDaysSinceEpochFromIsoDateTimeConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      try {
        return ISO_DATE_FORMAT.parse(input).getTime() / 86400000;
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToHoursSinceEpochFromIsoDateTimeConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      try {
        return ISO_DATE_FORMAT.parse(input).getTime() / 3600000;
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMinutesSinceEpochFromIsoDateTimeConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      try {
        return ISO_DATE_FORMAT.parse(input).getTime() / 60000;
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToSecondsSinceEpochFromIsoDateTimeConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      try {
        return ISO_DATE_FORMAT.parse(input).getTime() / 1000;
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToMillisecondsSinceEpochFromIsoDateTimeConverter implements TransformFunction<String, Long> {
    @Override
    public Long transform(String input) {
      try {
        return ISO_DATE_FORMAT.parse(input).getTime();
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }

    @Override
    public void init(Map<String, String> params) {

    }
  }

  public static class ToDaysSinceEpochFromGivenTimePatternConverter implements TransformFunction<String, Long> {
    private DateFormat df;

    @Override
    public void init(Map<String, String> params) {
      Preconditions.checkNotNull(params, String.format("Params to init %s is null!", this.getClass().getName()));
      Preconditions.checkNotNull(params.get(TIME_PATTERN),
          String.format("Param TimePattern to init %s is null!", this.getClass().getName()));
      df = new SimpleDateFormat(params.get(TIME_PATTERN));
    }

    @Override
    public Long transform(String input) {
      try {
        return df.parse(input).getTime() / 86400000;
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }
  }

  public static class ToHoursSinceEpochFromGivenTimePatternConverter implements TransformFunction<String, Long> {
    private DateFormat df;

    @Override
    public void init(Map<String, String> params) {
      Preconditions.checkNotNull(params, String.format("Params to init %s is null!", this.getClass().getName()));
      Preconditions.checkNotNull(params.get(TIME_PATTERN),
          String.format("Param TimePattern to init %s is null!", this.getClass().getName()));
      df = new SimpleDateFormat(params.get(TIME_PATTERN));
    }

    @Override
    public Long transform(String input) {
      try {
        return df.parse(input).getTime() / 3600000;
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }
  }

  public static class ToMinutesSinceEpochFromGivenTimePatternConverter implements TransformFunction<String, Long> {
    private DateFormat df;

    @Override
    public void init(Map<String, String> params) {
      Preconditions.checkNotNull(params, String.format("Params to init %s is null!", this.getClass().getName()));
      Preconditions.checkNotNull(params.get(TIME_PATTERN),
          String.format("Param TimePattern to init %s is null!", this.getClass().getName()));
      df = new SimpleDateFormat(params.get(TIME_PATTERN));
    }

    @Override
    public Long transform(String input) {
      try {
        return df.parse(input).getTime() / 60000;
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }

  }

  public static class ToSecondsSinceEpochFromGivenTimePatternConverter implements TransformFunction<String, Long> {
    private DateFormat df;

    @Override
    public void init(Map<String, String> params) {
      Preconditions.checkNotNull(params, String.format("Params to init %s is null!", this.getClass().getName()));
      Preconditions.checkNotNull(params.get(TIME_PATTERN),
          String.format("Param TimePattern to init %s is null!", this.getClass().getName()));
      df = new SimpleDateFormat(params.get(TIME_PATTERN));
    }

    @Override
    public Long transform(String input) {
      try {
        return df.parse(input).getTime() / 1000;
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }

  }

  public static class ToMillisecondsSinceEpochFromGivenTimePatternConverter implements TransformFunction<String, Long> {

    private DateFormat df;

    @Override
    public void init(Map<String, String> params) {
      Preconditions.checkNotNull(params, String.format("Params to init %s is null!", this.getClass().getName()));
      Preconditions.checkNotNull(params.get(TIME_PATTERN),
          String.format("Param TimePattern to init %s is null!", this.getClass().getName()));
      df = new SimpleDateFormat(params.get(TIME_PATTERN));
    }

    @Override
    public Long transform(String input) {
      try {
        return df.parse(input).getTime();
      } catch (Exception e) {
        return Long.MIN_VALUE;
      }
    }

  }

  public static void main(String[] args) throws Exception {
    String ts = "2016-07-25T11:28:05+00:00";
    Date parsedDate = ISO_DATE_FORMAT.parse(ts);
    System.out.println(parsedDate);
    System.out.println(parsedDate.getTime());
  }
}
