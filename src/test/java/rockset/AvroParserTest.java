package rockset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class AvroParserTest {

  // When a key is avro type, but doesn't have a schema parseKey should fail
  @Test
  public void testAvroKeyNoSchema() {
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA);

    Struct key = new Struct(keySchema).put("id", 2L);

    Schema valueSchema =
        SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).field("name", Schema.STRING_SCHEMA);

    Struct value = new Struct(valueSchema).put("id", 2L).put("name", "my-name");

    SinkRecord sr = makeSinkRecord(null, key, valueSchema, value);
    assertThrows(ConnectException.class, () -> new AvroParser().parseKey(sr));
  }

  // test possible non-avro types for keys
  @Test
  public void testSimpleKeys() throws IOException {
    verifySimpleKey(1);
    verifySimpleKey("key");
    verifySimpleKey(0.1);
    verifySimpleKey(false);
    verifySimpleKey(true);
  }

  @Test
  public void testAvroArraySchema1() throws IOException {
    final String schemaStr =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"KsqlDataSourceSchema\",\n"
            + "  \"namespace\": \"io.confluent.ksql.avro_schemas\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"user_id\",\n"
            + "      \"type\": [\n"
            + "        \"null\",\n"
            + "        {\n"
            + "          \"type\": \"array\",\n"
            + "          \"items\": [\n"
            + "            \"null\",\n"
            + "            \"string\"\n"
            + "          ]\n"
            + "        }\n"
            + "      ],\n"
            + "      \"default\": null\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaStr);
    Schema schema = new AvroData(1).toConnectSchema(avroSchema);

    AvroParser avroParser = new AvroParser();

    String value =
        "{\n"
            + "  \"user_id\": [\n"
            + "    \"1\",\n"
            + "    \"2\",\n"
            + "    \"3\"\n"
            + "  ]\n"
            + "}";

    Map<String, Object> map = avroParser.getMap(value);
    Map<String, Object> res = avroParser.convertLogicalTypesMap(schema, map);

    assertEquals(res.keySet().stream().iterator().next(), "user_id");
    assertEquals(
        new ObjectMapper().writeValueAsString(res.entrySet().stream().iterator().next().getValue()),
        "[\"1\",\"2\",\"3\"]");
  }

  @Test
  public void testAvroArraySchema2() {
    final String schemaStr =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"KsqlDataSourceSchema\",\n"
            + "  \"namespace\": \"io.confluent.ksql.avro_schemas\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"data\",\n"
            + "      \"type\": [\n"
            + "        \"null\",\n"
            + "        {\n"
            + "          \"type\": \"array\",\n"
            + "          \"items\": [\n"
            + "            {\n"
            + "              \"type\": \"record\",\n"
            + "              \"name\": \"KsqlDataSourceSchema_data\",\n"
            + "              \"fields\": [\n"
            + "                {\n"
            + "                  \"name\": \"key\",\n"
            + "                  \"type\": [\n"
            + "                    \"null\",\n"
            + "                    \"string\"\n"
            + "                  ],\n"
            + "                  \"default\": null\n"
            + "                },\n"
            + "                {\n"
            + "                  \"name\": \"value\",\n"
            + "                  \"type\": [\n"
            + "                    \"null\",\n"
            + "                    \"string\"\n"
            + "                  ],\n"
            + "                  \"default\": null\n"
            + "                }\n"
            + "              ],\n"
            + "              \"connect.internal.type\": \"MapEntry\"\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      ],\n"
            + "      \"default\": null\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaStr);
    Schema schema = new AvroData(1).toConnectSchema(avroSchema);

    AvroParser avroParser = new AvroParser();

    String value =
        "{\n"
            + "  \"data\": [\n"
            + "    {\n"
            + "      \"KsqlDataSourceSchema_data\": {\n"
            + "        \"key\": \"1\",\n"
            + "        \"value\": \"11\"\n"
            + "      }\n"
            + "    },\n"
            + "    {\n"
            + "      \"KsqlDataSourceSchema_data\": {\n"
            + "        \"key\": \"2\",\n"
            + "        \"value\": \"22\"\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    Map<String, Object> map = avroParser.getMap(value);
    Map<String, Object> res = avroParser.convertLogicalTypesMap(schema, map);

    assertEquals("data", res.keySet().stream().iterator().next());
    List<Map<String, Object>> values =
        (List<Map<String, Object>>) res.entrySet().stream().iterator().next().getValue();

    assertEquals(values.size(), 2);
    assertEquals(
        ((Map<String, String>) values.get(0).get("KsqlDataSourceSchema_data")).get("key"), "1");
    assertEquals(
        ((Map<String, String>) values.get(0).get("KsqlDataSourceSchema_data")).get("value"), "11");
    assertEquals(
        ((Map<String, String>) values.get(1).get("KsqlDataSourceSchema_data")).get("key"), "2");
    assertEquals(
        ((Map<String, String>) values.get(1).get("KsqlDataSourceSchema_data")).get("value"), "22");
  }

  @Test
  public void testAvroArraySchema3() throws IOException {
    Schema schema =
        SchemaBuilder.struct()
            .name("record")
            .field(
                "data",
                SchemaBuilder.array(SchemaBuilder.map(Schema.STRING_SCHEMA, Timestamp.SCHEMA)));

    String value =
        "{\n"
            + "  \"data\": [\n"
            + "    {\n"
            + "      \"1\": 1642784652\n"
            + "    },\n"
            + "    {\n"
            + "      \"2\": 1642784653\n"
            + "    },\n"
            + "    {\n"
            + "      \"3\": 1642784654\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    AvroParser avroParser = new AvroParser();
    Map<String, Object> map = avroParser.getMap(value);
    Map<String, Object> res = avroParser.convertLogicalTypesMap(schema, map);

    String expectedOutput =
        "{\n"
            + "  \"data\" : [ {\n"
            + "    \"1\" : {\n"
            + "      \"value\" : 1642784652000,\n"
            + "      \"__rockset_type\" : \"timestamp\"\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"2\" : {\n"
            + "      \"value\" : 1642784653000,\n"
            + "      \"__rockset_type\" : \"timestamp\"\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"3\" : {\n"
            + "      \"value\" : 1642784654000,\n"
            + "      \"__rockset_type\" : \"timestamp\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}";

    assertEquals(
        expectedOutput.replaceAll("[\\n\\t ]", ""), new ObjectMapper().writeValueAsString(res));
  }

  @Test
  public void testAvroArraySchema4() throws IOException {
    Schema schema =
        SchemaBuilder.struct()
            .name("record")
            .field(
                "data",
                SchemaBuilder.struct()
                    .field(
                        "foo",
                        SchemaBuilder.array(
                            SchemaBuilder.map(Schema.STRING_SCHEMA, Timestamp.SCHEMA))));

    String value =
        "{\n"
            + "  \"data\": {\n"
            + "    \"foo\": [\n"
            + "      {\n"
            + "        \"1\": 1642784652\n"
            + "      },\n"
            + "      {\n"
            + "        \"2\": 1642784653\n"
            + "      },\n"
            + "      {\n"
            + "        \"3\": 1642784654\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";

    AvroParser avroParser = new AvroParser();
    Map<String, Object> map = avroParser.getMap(value);
    Map<String, Object> res = avroParser.convertLogicalTypesMap(schema, map);

    String expectedOutput =
        "{\n"
            + "  \"data\" : {\n"
            + "    \"foo\" : [ {\n"
            + "      \"1\" : {\n"
            + "        \"value\" : 1642784652000,\n"
            + "        \"__rockset_type\" : \"timestamp\"\n"
            + "      }\n"
            + "    }, {\n"
            + "      \"2\" : {\n"
            + "        \"value\" : 1642784653000,\n"
            + "        \"__rockset_type\" : \"timestamp\"\n"
            + "      }\n"
            + "    }, {\n"
            + "      \"3\" : {\n"
            + "        \"value\" : 1642784654000,\n"
            + "        \"__rockset_type\" : \"timestamp\"\n"
            + "      }\n"
            + "    } ]\n"
            + "  }\n"
            + "}";

    assertEquals(
        expectedOutput.replaceAll("[\\n\\t ]", ""), new ObjectMapper().writeValueAsString(res));
  }

  private void verifySimpleKey(Object key) throws IOException {
    Schema valueSchema =
        SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).field("name", Schema.STRING_SCHEMA);

    Struct value = new Struct(valueSchema).put("id", 2L).put("name", "my-name");

    SinkRecord sr = makeSinkRecord(null, key, valueSchema, value);

    Object parsedKey = new AvroParser().parseKey(sr);
    assertEquals(key, parsedKey);

    Map<String, Object> expectedValue = ImmutableMap.of("id", 2, "name", "my-name");
    assertEquals(expectedValue, parseValue(sr));
  }

  // test avro parsing without key
  @Test
  public void testNoKeyAvro() throws IOException {
    Schema detailsSchema =
        SchemaBuilder.struct()
            .field("zip_code", Schema.INT64_SCHEMA)
            .field("city", Schema.STRING_SCHEMA);

    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("details", detailsSchema)
            .field("name", Schema.STRING_SCHEMA);

    Struct details = new Struct(detailsSchema).put("zip_code", 1111L).put("city", "abcdef");

    Struct value =
        new Struct(valueSchema).put("id", 1234L).put("details", details).put("name", "my-name");

    SinkRecord sr = makeSinkRecord(null, null, valueSchema, value);
    Object key = new AvroParser().parseKey(sr);
    assertNull(key);

    Map<String, Object> expectedDetails = ImmutableMap.of("zip_code", 1111, "city", "abcdef");
    Map<String, Object> expectedValue =
        ImmutableMap.of("id", 1234, "details", expectedDetails, "name", "my-name");
    assertEquals(expectedValue, parseValue(sr));
  }

  @Test
  public void testAvroKey() throws IOException {
    Schema detailsSchema =
        SchemaBuilder.struct()
            .field("zip_code", Schema.INT64_SCHEMA)
            .field("city", Schema.STRING_SCHEMA);

    Schema keySchema =
        SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).field("details", detailsSchema);

    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("details", detailsSchema)
            .field("name", Schema.STRING_SCHEMA);

    Struct details = new Struct(detailsSchema).put("zip_code", 1111L).put("city", "abcdef");

    Struct key = new Struct(keySchema).put("id", 1234L).put("details", details);

    Struct value =
        new Struct(valueSchema).put("id", 1234L).put("details", details).put("name", "my-name");

    SinkRecord sinkRecord = makeSinkRecord(keySchema, key, valueSchema, value);

    Map<String, Object> expectedDetails = ImmutableMap.of("zip_code", 1111, "city", "abcdef");

    {
      Object parsedKey = new AvroParser().parseKey(sinkRecord);
      Map<String, Object> expectedKey = ImmutableMap.of("id", 1234, "details", expectedDetails);
      assertEquals(expectedKey, parsedKey);
    }

    {
      Map<String, Object> expectedValue =
          ImmutableMap.of("id", 1234, "details", expectedDetails, "name", "my-name");
      assertEquals(expectedValue, parseValue(sinkRecord));
    }
  }

  @Test
  void testTime() {
    Schema timeSchema =
        SchemaBuilder.struct()
            .field("time1", Time.SCHEMA)
            .field("time2", Time.SCHEMA)
            .field("time3", Time.SCHEMA)
            .field("time4", Time.SCHEMA)
            .field("time5", Time.SCHEMA);

    // millis seconds since mid night
    Struct value =
        new Struct(timeSchema)
            .put("time1", Date.from(Instant.ofEpochMilli(0)))
            .put("time2", Date.from(Instant.ofEpochMilli(10)))
            .put("time3", Date.from(Instant.ofEpochMilli(123456)))
            .put("time4", Date.from(Instant.ofEpochMilli(12345678)))
            .put("time5", Date.from(Instant.ofEpochMilli(86400000)));

    Map<String, Object> expected =
        ImmutableMap.of(
            "time1", rocksetTimeType("00:00:00:000000"),
            "time2", rocksetTimeType("00:00:00:010000"),
            "time3", rocksetTimeType("00:02:03:456000"),
            "time4", rocksetTimeType("03:25:45:678000"),
            "time5", rocksetTimeType("00:00:00:000000"));

    SinkRecord sinkRecord = makeSinkRecord(null, null, timeSchema, value);
    assertEquals(expected, parseValue(sinkRecord));
  }

  @Test
  void testDate() {
    Schema dateSchema =
        SchemaBuilder.struct()
            .field("date1", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("date2", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("date3", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("date4", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("date5", org.apache.kafka.connect.data.Date.SCHEMA);

    Struct value =
        new Struct(dateSchema)
            .put("date1", Date.from(Instant.ofEpochSecond(86400 * 0)))
            .put("date2", Date.from(Instant.ofEpochSecond(86400 * 1)))
            .put("date3", Date.from(Instant.ofEpochSecond(86400 * 100)))
            .put("date4", Date.from(Instant.ofEpochSecond(86400 * 12345)))
            .put("date5", Date.from(Instant.ofEpochSecond(86400 * 17000)));

    Map<String, Object> expected =
        ImmutableMap.of(
            "date1", rocksetDateType("1970/01/01"),
            "date2", rocksetDateType("1970/01/02"),
            "date3", rocksetDateType("1970/04/11"),
            "date4", rocksetDateType("2003/10/20"),
            "date5", rocksetDateType("2016/07/18"));

    SinkRecord sinkRecord = makeSinkRecord(null, null, dateSchema, value);
    System.out.println(parseValue(sinkRecord));
    assertEquals(expected, parseValue(sinkRecord));
  }

  @Test
  void testTimestamp() {
    Schema detailsSchema =
        SchemaBuilder.struct()
            .field("timestamp1", Timestamp.SCHEMA)
            .field("timestamp2", Timestamp.SCHEMA)
            .field("timestamp3", Timestamp.SCHEMA)
            .field("timestamp4", Timestamp.SCHEMA)
            .field("timestamp5", Timestamp.SCHEMA);

    Struct value =
        new Struct(detailsSchema)
            .put("timestamp1", new Date(0L))
            .put("timestamp2", new Date(1000L))
            .put("timestamp3", new Date(10000000L))
            .put("timestamp4", new Date(1234567890123L))
            .put("timestamp5", new Date(1583966531712L));

    Map<String, Object> expected =
        ImmutableMap.of(
            "timestamp1", rocksetTimestampType(0L),
            "timestamp2", rocksetTimestampType(1000L),
            "timestamp3", rocksetTimestampType(10000000L),
            "timestamp4", rocksetTimestampType(1234567890123L),
            "timestamp5", rocksetTimestampType(1583966531712L));

    SinkRecord sinkRecord = makeSinkRecord(null, null, detailsSchema, value);
    assertEquals(expected, parseValue(sinkRecord));
  }

  @Test
  void testMultipleLogicalTypes() {
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("time", Time.SCHEMA)
            .field("date", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("timestamp", Timestamp.SCHEMA);

    Struct value =
        new Struct(valueSchema)
            .put("time", Date.from(Instant.ofEpochMilli(3723000))) // 1 hour, 2 mins, 3 seconds
            .put("date", Date.from(Instant.ofEpochSecond(86400)))
            .put("timestamp", new Date(1234L));

    Map<String, Object> expected =
        ImmutableMap.of(
            "time", rocksetTimeType("01:02:03:000000"),
            "timestamp", rocksetTimestampType(1234L),
            "date", rocksetDateType("1970/01/02"));

    SinkRecord sinkRecord = makeSinkRecord(null, null, valueSchema, value);
    assertEquals(expected, parseValue(sinkRecord));
  }

  @Test
  void testNestedStructLogicalTypes() {
    Schema nestedSchema1 = SchemaBuilder.struct().field("time", Time.SCHEMA);
    Schema nestedSchema2 =
        SchemaBuilder.struct().field("date", org.apache.kafka.connect.data.Date.SCHEMA);
    Schema nestedSchema3 = SchemaBuilder.struct().field("timestamp", Timestamp.SCHEMA);
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("nested1", nestedSchema1)
            .field("nested2", nestedSchema2)
            .field("nested3", nestedSchema3);

    Struct nestedValue1 =
        new Struct(nestedSchema1)
            .put("time", Date.from(Instant.ofEpochMilli(3723000))); // 1 hour, 2 mins, 3 seconds
    Struct nestedValue2 =
        new Struct(nestedSchema2).put("date", Date.from(Instant.ofEpochSecond(86400)));
    Struct nestedValue3 = new Struct(nestedSchema3).put("timestamp", new Date(1234L));
    Struct value =
        new Struct(valueSchema)
            .put("nested1", nestedValue1)
            .put("nested2", nestedValue2)
            .put("nested3", nestedValue3);

    Map<String, Object> expectedValue =
        ImmutableMap.of(
            "nested1", ImmutableMap.of("time", rocksetTimeType("01:02:03:000000")),
            "nested2", ImmutableMap.of("date", rocksetDateType("1970/01/02")),
            "nested3", ImmutableMap.of("timestamp", rocksetTimestampType(1234L)));

    SinkRecord sr = makeSinkRecord(null, null, valueSchema, value);
    assertEquals(expectedValue, parseValue(sr));
  }

  @Test
  void testNestedMapLogical() {
    Schema nestedSchema1 = SchemaBuilder.map(Schema.STRING_SCHEMA, Time.SCHEMA).build();
    Schema nestedSchema2 =
        SchemaBuilder.map(Schema.STRING_SCHEMA, org.apache.kafka.connect.data.Date.SCHEMA).build();
    Schema nestedSchema3 = SchemaBuilder.map(Schema.STRING_SCHEMA, Timestamp.SCHEMA).build();

    Schema valueSchema =
        SchemaBuilder.struct()
            .field("nested1", nestedSchema1)
            .field("nested2", nestedSchema2)
            .field("nested3", nestedSchema3);

    Struct value =
        new Struct(valueSchema)
            .put("nested1", ImmutableMap.of("time", Date.from(Instant.ofEpochMilli(3723000))))
            .put("nested2", ImmutableMap.of("date", Date.from(Instant.ofEpochSecond(86400))))
            .put("nested3", ImmutableMap.of("timestamp", new Date(1234L)));

    Map<String, Object> expectedValue =
        ImmutableMap.of(
            "nested1", ImmutableMap.of("time", rocksetTimeType("01:02:03:000000")),
            "nested2", ImmutableMap.of("date", rocksetDateType("1970/01/02")),
            "nested3", ImmutableMap.of("timestamp", rocksetTimestampType(1234L)));

    SinkRecord sr = makeSinkRecord(null, null, valueSchema, value);
    assertEquals(expectedValue, parseValue(sr));
  }

  @Test
  void testArrayLogicalTypes() {
    Schema arraySchema1 = SchemaBuilder.array(Time.SCHEMA).build();
    Schema arraySchema2 = SchemaBuilder.array(org.apache.kafka.connect.data.Date.SCHEMA).build();
    Schema arraySchema3 = SchemaBuilder.array(Timestamp.SCHEMA).build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("times", arraySchema1)
            .field("dates", arraySchema2)
            .field("timestamps", arraySchema3);

    Struct value =
        new Struct(valueSchema)
            .put(
                "times",
                ImmutableList.of(
                    Date.from(Instant.ofEpochMilli(3723000)),
                    Date.from(Instant.ofEpochMilli(3723000)),
                    Date.from(Instant.ofEpochMilli(3723000))))
            .put(
                "dates",
                ImmutableList.of(
                    Date.from(Instant.ofEpochSecond(86400)),
                    Date.from(Instant.ofEpochSecond(86400)),
                    Date.from(Instant.ofEpochSecond(86400))))
            .put("timestamps", ImmutableList.of(new Date(1234L), new Date(1234L), new Date(1234L)));

    Map<String, Object> expectedValue =
        ImmutableMap.of(
            "times",
                ImmutableList.of(
                    rocksetTimeType("01:02:03:000000"),
                    rocksetTimeType("01:02:03:000000"),
                    rocksetTimeType("01:02:03:000000")),
            "dates",
                ImmutableList.of(
                    rocksetDateType("1970/01/02"),
                    rocksetDateType("1970/01/02"),
                    rocksetDateType("1970/01/02")),
            "timestamps",
                ImmutableList.of(
                    rocksetTimestampType(1234L),
                    rocksetTimestampType(1234L),
                    rocksetTimestampType(1234L)));

    SinkRecord sr = makeSinkRecord(null, null, valueSchema, value);
    assertEquals(expectedValue, parseValue(sr));
  }

  @Test
  void testNestedArrayLogicalTypes() {
    Schema nestedSchema1 =
        SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Time.SCHEMA)).build();
    Schema nestedSchema2 =
        SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                SchemaBuilder.array(org.apache.kafka.connect.data.Date.SCHEMA))
            .build();
    Schema nestedSchema3 =
        SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Timestamp.SCHEMA)).build();

    Schema valueSchema =
        SchemaBuilder.struct()
            .field("nested1", nestedSchema1)
            .field("nested2", nestedSchema2)
            .field("nested3", nestedSchema3);

    Struct value =
        new Struct(valueSchema)
            .put(
                "nested1",
                ImmutableMap.of(
                    "times",
                    ImmutableList.of(
                        Date.from(Instant.ofEpochMilli(3723000)),
                        Date.from(Instant.ofEpochMilli(3723000)),
                        Date.from(Instant.ofEpochMilli(3723000)))))
            .put(
                "nested2",
                ImmutableMap.of(
                    "dates",
                    ImmutableList.of(
                        Date.from(Instant.ofEpochSecond(86400)),
                        Date.from(Instant.ofEpochSecond(86400)),
                        Date.from(Instant.ofEpochSecond(86400)))))
            .put(
                "nested3",
                ImmutableMap.of(
                    "timestamps",
                    ImmutableList.of(new Date(1234L), new Date(1234L), new Date(1234L))));

    Map<String, Object> expectedValue =
        ImmutableMap.of(
            "nested1",
                ImmutableMap.of(
                    "times",
                    ImmutableList.of(
                        rocksetTimeType("01:02:03:000000"),
                        rocksetTimeType("01:02:03:000000"),
                        rocksetTimeType("01:02:03:000000"))),
            "nested2",
                ImmutableMap.of(
                    "dates",
                    ImmutableList.of(
                        rocksetDateType("1970/01/02"),
                        rocksetDateType("1970/01/02"),
                        rocksetDateType("1970/01/02"))),
            "nested3",
                ImmutableMap.of(
                    "timestamps",
                    ImmutableList.of(
                        rocksetTimestampType(1234L),
                        rocksetTimestampType(1234L),
                        rocksetTimestampType(1234L))));

    SinkRecord sr = makeSinkRecord(null, null, valueSchema, value);
    assertEquals(expectedValue, parseValue(sr));
  }

  @Test
  public void testNullTypes() throws IOException {
    final String schemaStr =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"KsqlDataSourceSchema\",\n"
            + "  \"namespace\": \"io.confluent.ksql.avro_schemas\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"array_field\",\n"
            + "      \"type\": [\n"
            + "        \"null\",\n"
            + "        {\n"
            + "          \"type\": \"array\",\n"
            + "          \"items\": [\n"
            + "            \"null\",\n"
            + "            \"string\"\n"
            + "          ]\n"
            + "        }\n"
            + "      ],\n"
            + "      \"default\": null\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"map_field\",\n"
            + "      \"type\": [\n"
            + "        \"null\",\n"
            + "        {\n"
            + "          \"type\": \"map\",\n"
            + "          \"values\": \"int\"\n"
            + "        }\n"
            + "      ],\n"
            + "      \"default\": null\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaStr);
    Schema schema = new AvroData(1).toConnectSchema(avroSchema);

    AvroParser avroParser = new AvroParser();

    String value = "{\n" + "  \"array_field\": null,\n" + "  \"map_field\": null\n" + "}";

    Map<String, Object> map = avroParser.getMap(value);
    Map<String, Object> res = avroParser.convertLogicalTypesMap(schema, map);

    assertEquals(res.keySet(), ImmutableSet.of("array_field", "map_field"));

    List<Object> expectedValues = new ArrayList<Object>();
    expectedValues.add(null);
    expectedValues.add(null);
    assertEquals(new ArrayList<Object>(res.values()), expectedValues);
  }

  @Test
  public void testUnexpectedField() throws IOException {
    final String schemaStr =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"KsqlDataSourceSchema\",\n"
            + "  \"namespace\": \"io.confluent.ksql.avro_schemas\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"map_field\",\n"
            + "      \"type\": [\n"
            + "        \"null\"\n"
            + "      ],\n"
            + "      \"default\": null\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaStr);
    Schema schema = new AvroData(1).toConnectSchema(avroSchema);

    AvroParser avroParser = new AvroParser();

    String value = "{\n" + "  \"nondefinedfield\": 3\n" + "}";

    Map<String, Object> map = avroParser.getMap(value);
    assertThrows(
        DataException.class,
        () -> avroParser.convertLogicalTypesMap(schema, map),
        "found non-declared field: nondefinedfield");
  }

  private ImmutableMap<String, Object> rocksetTimestampType(long timeMs) {
    return ImmutableMap.of("__rockset_type", "timestamp", "value", timeMs * 1000);
  }

  private ImmutableMap<String, Object> rocksetDateType(Object date) {
    return ImmutableMap.of("__rockset_type", "date", "value", date);
  }

  private ImmutableMap<String, Object> rocksetTimeType(Object time) {
    return ImmutableMap.of("__rockset_type", "time", "value", time);
  }

  private Map<String, Object> parseValue(SinkRecord sinkRecord) {
    return new AvroParser().parseValue(sinkRecord);
  }

  private SinkRecord makeSinkRecord(
      Schema keySchema, Object key, Schema valueSchema, Object value) {
    return new SinkRecord("topic", 0, keySchema, key, valueSchema, value, 1);
  }
}
