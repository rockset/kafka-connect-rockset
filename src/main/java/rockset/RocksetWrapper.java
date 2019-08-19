package rockset;

import org.apache.kafka.connect.sink.SinkRecord;

public interface RocksetWrapper {
  boolean addDoc(String workspace, String collection, String json, SinkRecord sr);
}
