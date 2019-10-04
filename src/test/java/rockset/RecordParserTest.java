package rockset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class RecordParserTest {
  private static final ObjectMapper mapper = new ObjectMapper();

  // When a key is avro type, but doesn't have a schema parseKey should fail
  @Test
  public void testAvroKeyNoSchema() {
    Schema keySchema = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA);

    Struct key = new Struct(keySchema)
        .put("id", 2L);

    Schema valueSchema = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA);

    Struct value = new Struct(valueSchema)
        .put("id", 2L)
        .put("name", "my-name");

    SinkRecord sr = makeSinkRecord(null, key, valueSchema, value);
    assertThrows(ConnectException.class, () -> new AvroParser().parseKey(sr));
  }

  // test possible non-avro types for keys
  @Test
  public void testSimpleKeys() {
    verifySimpleKey(1);
    verifySimpleKey("key");
    verifySimpleKey(0.1);
    verifySimpleKey(false);
    verifySimpleKey(true);
  }

  private void verifySimpleKey(Object key) {
    Schema valueSchema = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA);

    Struct value = new Struct(valueSchema)
        .put("id", 2L)
        .put("name", "my-name");

    SinkRecord sr = makeSinkRecord(null, key, valueSchema, value);

    Object parsedKey = new AvroParser().parseKey(sr);
    assertEquals(key, parsedKey);


    Object parsedValue = new AvroParser().parseValue(sr);
    Map<String, Object> expectedValue = ImmutableMap.of(
        "id", 2,
        "name", "my-name");
    assertEquals(expectedValue, toMap(parsedValue));
  }

  // test avro parsing without key
  @Test
  public void testNoKeyAvro() {
    Schema detailsSchema = SchemaBuilder.struct()
        .field("zip_code", Schema.INT64_SCHEMA)
        .field("city", Schema.STRING_SCHEMA);

    Schema valueSchema = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("details", detailsSchema)
        .field("name", Schema.STRING_SCHEMA);

    Struct details = new Struct(detailsSchema)
        .put("zip_code", 1111L)
        .put("city", "abcdef");

    Struct value = new Struct(valueSchema)
        .put("id", 1234L)
        .put("details", details)
        .put("name", "my-name");

    SinkRecord sr = makeSinkRecord(null, null, valueSchema, value);
    Object key = new AvroParser().parseKey(sr);
    assertNull(key);

    Object parsedValue = new AvroParser().parseValue(sr);
    Map<String, Object> expectedDetails = ImmutableMap.of(
        "zip_code", 1111,
        "city", "abcdef");
    Map<String, Object> expectedValue = ImmutableMap.of(
        "id", 1234,
        "details", expectedDetails,
        "name", "my-name");
    assertEquals(expectedValue, toMap(parsedValue));
  }

  @Test
  public void testAvroKey() {
    Schema detailsSchema = SchemaBuilder.struct()
        .field("zip_code", Schema.INT64_SCHEMA)
        .field("city", Schema.STRING_SCHEMA);

    Schema keySchema = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("details", detailsSchema);

    Schema valueSchema = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("details", detailsSchema)
        .field("name", Schema.STRING_SCHEMA);

    Struct details = new Struct(detailsSchema)
        .put("zip_code", 1111L)
        .put("city", "abcdef");

    Struct key = new Struct(keySchema)
        .put("id", 1234L)
        .put("details", details);

    Struct value = new Struct(valueSchema)
        .put("id", 1234L)
        .put("details", details)
        .put("name", "my-name");

    SinkRecord sinkRecord = makeSinkRecord(keySchema, key, valueSchema, value);

    Map<String, Object> expectedDetails = ImmutableMap.of(
        "zip_code", 1111,
        "city", "abcdef");

    {
      Object parsedKey = new AvroParser().parseKey(sinkRecord);
      Map<String, Object> expectedKey = ImmutableMap.of(
          "id", 1234,
          "details", expectedDetails);
      assertEquals(expectedKey, toMap(parsedKey));
    }

    {
      Object parsedValue = new AvroParser().parseValue(sinkRecord);
      Map<String, Object> expectedValue = ImmutableMap.of(
          "id", 1234,
          "details", expectedDetails,
          "name", "my-name");
      assertEquals(expectedValue, toMap(parsedValue));
    }
  }

  private SinkRecord makeSinkRecord(
      Schema keySchema, Object key, Schema valueSchema,
      Object value) {
    return new SinkRecord("topic", 0, keySchema, key, valueSchema, value, 1);
  }

  private Map<String,Object> toMap(Object key) {
    try {
      return mapper.readValue(key.toString(), new TypeReference<Map<String, Object>>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}