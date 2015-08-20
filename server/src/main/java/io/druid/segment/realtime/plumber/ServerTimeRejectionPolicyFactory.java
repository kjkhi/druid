/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.realtime.plumber;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class ServerTimeRejectionPolicyFactory implements RejectionPolicyFactory
{
  private static DateTimeFormatter bf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.forID("Asia/Shanghai"));

  /**
   * 把当前系统时间转换为北京时区
   * @param timeMillis
   * @return
   */
  private long getBeijingTimeMillis(long timeMillis){
    return DateTime.parse(bf.print(timeMillis), ISODateTimeFormat.dateTime()).getMillis();
  }

  @Override
  public RejectionPolicy create(final Period windowPeriod)
  {
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    return new RejectionPolicy()
    {
      @Override
      public DateTime getCurrMaxTime()
      {
        return new DateTime();
      }

      @Override
      public boolean accept(long timestamp)
      {
        // 把当前系统时间转换为北京时区
//        long now = System.currentTimeMillis();
        long now = getBeijingTimeMillis(System.currentTimeMillis());

        boolean notTooOld = timestamp >= (now - windowMillis);
        boolean notTooYoung = timestamp <= (now + windowMillis);

        return notTooOld && notTooYoung;
      }

      @Override
      public String toString()
      {
        return String.format ("serverTime-%s", windowPeriod);
      }
    };
  }
}
