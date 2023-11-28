package com.pan.flink.utils;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author panjb
 */
public class TimeHelper {

    private TimeHelper() {
    }

    public static LocalDateTime toLocalDateTime(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
    }

    public static LocalDateTime toLocalDateTime(String time) {
        return toLocalDateTime(toTimestamp(time));
    }

    /**
     * 时间字符串转时间戳
     * @param time 时间字符串， 格式：yyyy-MM-dd HH:mm:ss
     * @return 时间戳
     */
    public static long toTimestamp(String time) {
        return Timestamp.valueOf(time).getTime();
    }

    public static String format(long time, String pattern) {
        return DateTimeFormatter.ofPattern(pattern).format(toLocalDateTime(time));
    }

}
