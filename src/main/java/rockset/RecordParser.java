package rockset;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.kafka.connect.sink.SinkRecord;


public interface RecordParser {
  Object parse(SinkRecord record);
}

class AvroParser implements RecordParser {
  @Override
  public Object parse(SinkRecord record) {
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
  public Object parse(SinkRecord record) {
    return record.value();
  }
}


