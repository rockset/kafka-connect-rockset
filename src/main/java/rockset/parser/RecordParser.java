package rockset.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

public interface RecordParser {

  Map<String, Object> parseValue(SinkRecord record);

  /**
   * Parse key from a sink record. If key is struct type convert to Java Map type else return what
   * is in the key
   */
  default Object parseKey(SinkRecord record) throws IOException {
    if (record.key() instanceof Struct) {
      AvroData keyData = new AvroData(1);
      Object key = keyData.fromConnectData(record.keySchema(), record.key());
      // For struct types convert to a Java Map object
      return toMap(key);
    }
    return record.key();
  }

  static Object toMap(Object key) throws IOException {
    return new ObjectMapper()
        .readValue(key.toString(), new TypeReference<Map<String, Object>>() {});
  }
}
