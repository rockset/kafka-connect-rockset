package rockset.parser;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class LogicalConverters {
  interface LogicalTypeConverter {
    Object convertLogicalType(Object value);
  }

  static class TimeConverter implements LogicalTypeConverter {

    @Override
    public Object convertLogicalType(Object value) {
      Integer timestampMillis = (Integer) value;
      return convertToRocksetTime(timestampMillis);
    }

    private Map<String, Object> convertToRocksetTime(Integer timestampMillis) {
      Map<String, Object> timeObj = new HashMap<>();
      timeObj.put("__rockset_type", "time");
      timeObj.put("value", millisToTime(timestampMillis));
      return timeObj;
    }

    private String millisToTime(int timemillis) {
      Instant instant = Instant.ofEpochMilli(timemillis);
      ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
      DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss:SSSSSS");
      return timeFormatter.format(zdt);
    }
  }

  static class DateConverter implements LogicalTypeConverter {
    @Override
    public Object convertLogicalType(Object value) {
      Integer daysSinceEpoch = (Integer) value;
      return convertToRocksetDate(daysSinceEpoch);
    }

    public Map<String, Object> convertToRocksetDate(Integer daysSinceEpoch) {
      Map<String, Object> dateObj = new HashMap<>();
      dateObj.put("__rockset_type", "date");
      dateObj.put("value", daysToDate(daysSinceEpoch));
      return dateObj;
    }

    private String daysToDate(int daysSinceEpoch) {
      Instant instant = Instant.ofEpochSecond(daysSinceEpoch * 86400);
      LocalDate date = instant.atZone(ZoneOffset.UTC).toLocalDate();
      DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("YYYY/MM/dd");
      return dateFormatter.format(date);
    }
  }

  static class TimestampConverter implements LogicalTypeConverter {
    @Override
    public Object convertLogicalType(Object value) {
      long timestampMillis = new Long(value.toString());
      return convertToRocksetTimestamp(timestampMillis);
    }

    private Map<String, Object> convertToRocksetTimestamp(long timestampMillis) {
      Map<String, Object> timestampObj = new HashMap<>();
      // rockset expects timestamps in microseconds
      timestampObj.put("__rockset_type", "timestamp");
      timestampObj.put("value", timestampMillis * 1000);
      return timestampObj;
    }
  }
}
