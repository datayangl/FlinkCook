package com.code.ly.sql.util;

import org.apache.flink.api.common.time.Time;

import java.util.concurrent.TimeUnit;

public class TimeUtil {

    public static Time parseTime(String s) {
        try {
            TimeUnit timeUnit = null;

            String[] arry = s.split(" ", -1);
            CheckConditions.checkArgument(arry.length == 1 || arry.length == 2, "the format of time duration is a single number like '5' or a number with suffix like '5 s' ");

            if (arry.length == 1) {
                timeUnit = TimeUnit.SECONDS;
            }

            if (arry.length == 2) {
                String timeFormat = arry[1];
                boolean isFormatSupported = timeFormat.endsWith("ms") || timeFormat.endsWith("s") || timeFormat.endsWith("min") || timeFormat.endsWith("h") || timeFormat.endsWith("hour") || timeFormat.endsWith("day");
                CheckConditions.checkArgument(isFormatSupported, "the suffix of time duration format is not supported, which could be 'ms s min hour day'");

                switch (timeFormat) {
                    case "ms":
                        timeUnit = TimeUnit.MILLISECONDS;
                        break;
                    case "s":
                        timeUnit = TimeUnit.SECONDS;
                        break;
                    case "min":
                        timeUnit = TimeUnit.MINUTES;
                        break;
                    case "h":
                        timeUnit = TimeUnit.HOURS;
                        break;
                    case "hour":
                        timeUnit = TimeUnit.HOURS;
                        break;
                    case "day":
                        timeUnit = TimeUnit.DAYS;
                        break;
                    default:
                        throw new IllegalArgumentException("unsupported time duration suffix:" + timeFormat);
                }

            }
            long timeSize = Long.parseLong(arry[0]);

            return Time.of(timeSize, timeUnit);
        } catch (Exception e) {
            throw new IllegalArgumentException("wrong time duration format:" + s);
        }
    }
}
