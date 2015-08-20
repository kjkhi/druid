package io.druid.timezone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by jaykelin on 2015/8/19.
 */
public class TimeZoneTest {

    @Test
    public void test(){
        String s = "13fbdcd6f4f7d400";
        long l = Long.parseLong(s, 16)/1000000;
        long a = System.currentTimeMillis();
        DateTimeFormatter bf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.forID("Asia/Shanghai"));
        DateTimeFormatter utcf = ISODateTimeFormat.dateTime();

        System.out.println(a);
        System.out.println(new DateTime(a).getMillis());
        System.out.print(utcf.print(getBeijingTimeMillis(a)));

    }


    private long getBeijingTimeMillis(long timeMillis){
        DateTimeFormatter bf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.forID("Asia/Shanghai"));
        DateTimeFormatter utcf = ISODateTimeFormat.dateTime();
        return DateTime.parse(bf.print(timeMillis), utcf).getMillis();
    }
}
