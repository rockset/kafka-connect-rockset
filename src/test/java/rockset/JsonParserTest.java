package rockset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class JsonParserTest {

  @Test
  public void testBasicJsonObject() {
    Map<String, Object> obj = ImmutableMap.of("foo", "bar", "foo2", "bar2");
    SinkRecord sr = new SinkRecord("test-topic", 1, null, null, null, serialize(obj), 0);
    assertEquals(obj, parseSingleValue(sr));
  }

  @Test
  public void testListTypeOneObject() {
    List<Map<String, Object>> obj = ImmutableList.of(
        ImmutableMap.of("foo", "bar", "foo2", "bar2"));
    SinkRecord sr = new SinkRecord("test-topic", 1, null, null, null, serialize(obj), 0);
    assertEquals(obj.get(0), parseSingleValue(sr));
  }

  @Test
  public void testListTypeMultipleObjects() {
    List<Map<String, Object>> obj = ImmutableList.of(
        ImmutableMap.of("foo", "bar", "foo2", "bar2"),
        ImmutableMap.of("foo", "bar", "foo2", "bar2"),
        ImmutableMap.of("foo", "bar", "foo2", "bar2"));
    SinkRecord sr = new SinkRecord("test-topic", 1, null, null, null, serialize(obj), 0);
    assertEquals(obj, parseList(sr));
  }

  @Test
  public void testEmptyList() {
    List<Map<String, Object>> obj = ImmutableList.of();
    SinkRecord sr = new SinkRecord("test-topic", 1, null, null, null, serialize(obj), 0);
    assertThrows(RuntimeException.class, () -> parseSingleValue(sr));
  }

  private Object serialize(Object obj) {
    try {
      return new ObjectMapper().writeValueAsString(obj);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, Object> parseSingleValue(SinkRecord record) {
    return parseList(record).get(0);
  }

  public List<Map<String, Object>> parseList(SinkRecord record) {
    return new JsonParser().parseValue(record);
  }


}
