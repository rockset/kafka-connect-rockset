package rockset;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

public interface RocksetWrapper {

  // returns on success, throws RetriableException for retriable errors
  // throws ConnectException for unhandled errors
  void addDoc(String topic, Collection<SinkRecord> sr, RecordParser recordParser, int batchSize);
}
