package org.example.gtfsynq.domain.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class GtfsFeedFormatter {

    public static LocalDate parseDate(String date) {
        if (date == null || date.isBlank()) {
            return null;
        }

        try {
            return LocalDate.parse(date, GTFS_DATE);
        } catch (Exception ignored) {
            return LocalDate.parse(date);
        }
    }

    public static LocalTime parseTime(String time) {
        return (time == null || time.isBlank())
            ? null
            : LocalTime.parse(time, GTFS_TIME);
    }

    public static Instant parseDateTime(Long dateTime) {
        return (dateTime == null) ? null : Instant.ofEpochSecond(dateTime);
    }

    public static String nullableString(boolean present, String value) {
        return present ? value : null;
    }

    public static Integer nullableInteger(boolean present, int value) {
        return present ? value : null;
    }

    public static LocalDate nullableDate(boolean present, String value) {
        return present ? parseDate(value) : null;
    }

    public static LocalTime nullableTime(boolean present, String value) {
        return present ? parseTime(value) : null;
    }

    public static Instant nullableInstant(boolean present, Long value) {
        return present ? parseDateTime(value) : null;
    }

    public static String buildKey(String... parts) {
        return String.join(":", parts);
    }

    private static final DateTimeFormatter GTFS_DATE =
        DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter GTFS_TIME =
        DateTimeFormatter.ofPattern("HH:mm:ss");
}
