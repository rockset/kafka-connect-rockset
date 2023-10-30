package rockset;

import java.util.Collection;
import org.apache.kafka.connect.sink.SinkRecord;
import rockset.parser.RecordParser;

public interface RequestWrapper {

  // returns on success, throws RetriableException for retriable errors
  // throws ConnectException for unhandled errors
  void addDoc(String topic, Collection<SinkRecord> sr, RecordParser recordParser, int batchSize);
}
