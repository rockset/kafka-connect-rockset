package rockset;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

public interface RocksetWrapper {
  boolean addDoc(String workspace, String collection, String topic,
                 Collection<SinkRecord> sr, RecordParser recordParser, int batchSize);
}
