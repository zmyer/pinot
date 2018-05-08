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
package com.linkedin.pinot.common.data;

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.EnumUtils;
import org.joda.time.DurationFieldType;
import org.joda.time.chrono.ISOChronology;


public class DateTimeFormatUnitSpec {

  /**
   * Time unit enum with range from MILLISECONDS to YEARS
   */
  public enum DateTimeTransformUnit {
    MILLISECONDS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return millisSinceEpoch;
      }
    }, SECONDS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return TimeUnit.MILLISECONDS.toSeconds(millisSinceEpoch);
      }
    }, MINUTES {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return TimeUnit.MILLISECONDS.toMinutes(millisSinceEpoch);
      }
    }, HOURS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return TimeUnit.MILLISECONDS.toHours(millisSinceEpoch);
      }
    }, DAYS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return TimeUnit.MILLISECONDS.toDays(millisSinceEpoch);
      }
    }, WEEKS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return DurationFieldType.weeks().getField(ISOChronology.getInstanceUTC()).getDifference(millisSinceEpoch, 0L);
      }
    }, MONTHS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return DurationFieldType.months().getField(ISOChronology.getInstanceUTC()).getDifference(millisSinceEpoch, 0L);
      }
    }, YEARS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return DurationFieldType.years().getField(ISOChronology.getInstanceUTC()).getDifference(millisSinceEpoch, 0L);
      }
    };

    /**
     * Convert the given millisecond since epoch into the desired time unit.
     *
     * @param millisSinceEpoch Millisecond since epoch
     * @return Time since epoch of desired time unit
     */
    public abstract long fromMillis(long millisSinceEpoch);
  }

  private TimeUnit _timeUnit = null;
  private DateTimeTransformUnit _dateTimeTransformUnit = null;

  public DateTimeFormatUnitSpec(String unit) {
    if (!isValidUnitSpec(unit)) {
      throw new IllegalArgumentException("Unit must belong to enum TimeUnit or DateTimeTransformUnit");
    }
    if (EnumUtils.isValidEnum(TimeUnit.class, unit)) {
      _timeUnit = TimeUnit.valueOf(unit);
    }
    if (EnumUtils.isValidEnum(DateTimeTransformUnit.class, unit)) {
      _dateTimeTransformUnit = DateTimeTransformUnit.valueOf(unit);
    }
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  public DateTimeTransformUnit getDateTimeTransformUnit() {
    return _dateTimeTransformUnit;
  }

  public static boolean isValidUnitSpec(String unit) {
    if (EnumUtils.isValidEnum(TimeUnit.class, unit) || EnumUtils.isValidEnum(DateTimeTransformUnit.class, unit)) {
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    DateTimeFormatUnitSpec that = (DateTimeFormatUnitSpec) o;

    return EqualityUtils.isEqual(_timeUnit, that._timeUnit) && EqualityUtils.isEqual(_dateTimeTransformUnit,
        that._dateTimeTransformUnit);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_timeUnit);
    result = EqualityUtils.hashCodeOf(result, _dateTimeTransformUnit);
    return result;
  }
}
