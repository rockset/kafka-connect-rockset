package rockset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

public class RocksetSinkTask extends SinkTask {
  private RocksetClientWrapper rc;
  private static Logger log = LoggerFactory.getLogger(RocksetSinkTask.class);

  RocksetConnectorConfig config;
  @Override
  public void start(Map<String, String> settings) {
    this.config = new RocksetConnectorConfig(settings);
    this.rc = new RocksetClientWrapper(
        this.config.getRocksetApikey(),
        this.config.getRocksetApiServerUrl());
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord sr : records) {
      log.warn(sr.value().toString());
      String collection = this.config.getRocksetCollection();
      this.rc.addDoc(collection, sr.value().toString(), true);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
