package rockset;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private ExecutorService executorService;
  private static Logger log = LoggerFactory.getLogger(RocksetSinkTask.class);

  RocksetConnectorConfig config;
  @Override
  public void start(Map<String, String> settings) {
    this.config = new RocksetConnectorConfig(settings);
    this.rc = new RocksetClientWrapper(
        this.config.getRocksetApikey(),
        this.config.getRocksetApiServerUrl());
    this.executorService = Executors.newFixedThreadPool(this.config.getRocksetTaskThreads());
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    String collection = this.config.getRocksetCollection();
    for (SinkRecord sr : records) {
      executorService.submit(() -> this.rc.addDoc(collection, sr.value().toString()));
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
