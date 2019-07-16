package rockset;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

public class RocksetSinkUtils {

  public static String createId(SinkRecord sr) {
    if (sr.key() != null) {
      if (sr.key() instanceof String) {
        return String.valueOf(sr.key());
      } else {
        // only supports string keys
        throw new ConnectException(String.format("Only keys of type String are supported, " +
            "key is of type %s", sr.key().getClass()));
      }
    } else {
      return sr.topic() + "+" + sr.kafkaPartition() + "+" + sr.kafkaOffset();
    }
  }
}
