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
    assertEquals(obj, parseValue(sr));
  }

  @Test
  public void testListTypeOneObject() {
    List<Map<String, Object>> obj = ImmutableList.of(
        ImmutableMap.of("foo", "bar", "foo2", "bar2"));
    SinkRecord sr = new SinkRecord("test-topic", 1, null, null, null, serialize(obj), 0);
    assertEquals(obj.get(0), parseValue(sr));
  }

  @Test
  public void testListTypeMultipleObjects() {
    List<Map<String, Object>> obj = ImmutableList.of(
        ImmutableMap.of("foo", "bar", "foo2", "bar2"),
        ImmutableMap.of("foo", "bar", "foo2", "bar2"),
        ImmutableMap.of("foo", "bar", "foo2", "bar2"));
    SinkRecord sr = new SinkRecord("test-topic", 1, null, null, null, serialize(obj), 0);
    assertThrows(RuntimeException.class, () -> parseValue(sr));
  }

  @Test
  public void testEmptyList() {
    List<Map<String, Object>> obj = ImmutableList.of();
    SinkRecord sr = new SinkRecord("test-topic", 1, null, null, null, serialize(obj), 0);
    assertThrows(RuntimeException.class, () -> parseValue(sr));
  }

  private Object serialize(Object obj) {
    try {
      return new ObjectMapper().writeValueAsString(obj);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, Object> parseValue(SinkRecord record) {
    return new JsonParser().parseValue(record);
  }


}