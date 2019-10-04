package rockset;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;


public interface RecordParser {

  Object parseValue(SinkRecord record);

  /**
   * Parse key from a sink record.
   * If key is struct type convert AVRO to JSON
   * else return what is in the key
   */
  default Object parseKey(SinkRecord record) {
    if (record.key() instanceof Struct) {
      AvroData keyData = new AvroData(1);
      return keyData.fromConnectData(record.keySchema(), record.key());
    }
    return record.key();
  }
}

class AvroParser implements RecordParser {
  @Override
  public Object parseValue(SinkRecord record) {
    AvroData avroData = new AvroData(1); // arg is  cacheSize
    Object val = avroData.fromConnectData(record.valueSchema(), record.value());
    if (val instanceof NonRecordContainer) {
      val = ((NonRecordContainer) val).getValue();
    }

    return val;
  }
}

class JsonParser implements RecordParser {
  @Override
  public Object parseValue(SinkRecord record) {
    return record.value();
  }
}
