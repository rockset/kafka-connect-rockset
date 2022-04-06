package rockset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import rockset.LogicalConverters.DateConverter;
import rockset.LogicalConverters.LogicalTypeConverter;
import rockset.LogicalConverters.TimeConverter;
import rockset.LogicalConverters.TimestampConverter;

class AvroParser implements RecordParser {
  private static final Map<String, LogicalTypeConverter> LOGICAL_TYPE_CONVERTERS =
      ImmutableMap.of(
          Time.SCHEMA.name(), new TimeConverter(),
          Date.SCHEMA.name(), new DateConverter(),
          Timestamp.SCHEMA.name(), new TimestampConverter()
      );

  @Override
  public List<Map<String, Object>> parseValue(SinkRecord record) {
    if (record.value() == null) {
      return Collections.emptyList();
    }
    AvroData avroData = new AvroData(1); // arg is  cacheSize
    Object val = avroData.fromConnectData(record.valueSchema(), record.value());
    if (val instanceof NonRecordContainer) {
      val = ((NonRecordContainer) val).getValue();
    }
    if (val instanceof Record) {
      Map<String, Object> map = getMap(val);
      return Collections.singletonList(convertLogicalTypesMap(record.valueSchema(), map));
    }

    return Collections.singletonList(getMap(val));
  }

  private boolean isLogicalType(Schema schema) {
    return LOGICAL_TYPE_CONVERTERS.containsKey(schema.name());
  }

  private Object convertType(Schema schema, Object o) {
    if (isLogicalType(schema)) {
      return convertLogicalType(schema, o);
    }

    Type type = schema.type();

    switch (type) {
      case STRUCT:
      case MAP:
        return convertLogicalTypesMap(schema, (Map<String, Object>)o);

      case ARRAY:
        return convertLogicalTypesArray(schema, (List<Object>)o);
    }

    // cld be a scalar type, use as-is
    return o;
  }

  public List<Object> convertLogicalTypesArray(Schema schema, List<Object> arr) {
    List<Object> res = new ArrayList<>();

    for (Object o : arr) {
      res.add(convertType(schema.valueSchema(), o));
    }

    return res;
  }

  public Map<String, Object> convertLogicalTypesMap(Schema valueSchema, Map<String, Object> map) {
    for (Entry<String, Object> e : map.entrySet()) {
      Schema schema = getSchemaForField(valueSchema, e.getKey());
      if (schema == null) {
        continue;
      }

      e.setValue(convertType(schema, e.getValue()));
    }

    return map;
  }

  public Object convertLogicalType(Schema schema, Object value) {
    String schemaName = schema.name();
    return LOGICAL_TYPE_CONVERTERS.get(schemaName).convertLogicalType(value);
  }

  private Schema getSchemaForField(Schema schema, String key) {
    if (schema.type() == Type.STRUCT) {
      for (Field f : schema.fields()) {
        if (f.name().equals(key)) {
          return f.schema();
        }
      }
    }

    return schema.valueSchema();
  }

  public Map<String, Object> getMap(Object val) {
    try {
      return new ObjectMapper().readValue(val.toString(), new TypeReference<Map<String, Object>>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
