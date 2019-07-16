package rockset;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

public class RocksetSinkTask extends SinkTask {
  private RocksetClientWrapper rc;
  private ExecutorService executorService;
  RocksetConnectorConfig config;

  public static final int RETRIES_COUNT = 5;
  public static final int INITIAL_DELAY = 250;

  @Override
  public void start(Map<String, String> settings) {
    this.config = new RocksetConnectorConfig(settings);
    this.rc = new RocksetClientWrapper(
        this.config.getRocksetApikey(),
        this.config.getRocksetApiServerUrl());
    this.executorService = Executors.newFixedThreadPool(this.config.getRocksetTaskThreads());
  }

  // used for testing
  public void start(Map<String, String> settings, RocksetClientWrapper rc, ExecutorService executorService) {
    this.config = new RocksetConnectorConfig(settings);
    this.rc = rc;
    this.executorService = executorService;
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    String collection = this.config.getRocksetCollection();
    String workspace = this.config.getRocksetWorkspace();
    String format = this.config.getFormat();
    handleRecords(records, format, workspace, collection);
  }

  private void handleRecords(Collection<SinkRecord> records, String format,
                             String workspace, String collection) {
    switch (format) {
      case "json":
        for (SinkRecord sr : records) {
          executorService.execute(() -> addWithRetries(workspace, collection,
              sr.value().toString(), sr));
        }
        break;
      case "avro":
        AvroData avroData = new AvroData(1000); // arg is cacheSize
        for (SinkRecord sr : records) {
          executorService.execute(() -> {
            Object val = avroData.fromConnectData(sr.valueSchema(), sr.value());
            if (val instanceof NonRecordContainer) {
              val = ((NonRecordContainer) val).getValue();
            }
            addWithRetries(workspace, collection, val.toString(), sr);
          });
        }
        break;
      default:
        throw new ConnectException(String.format("Format %s not supported", format));
    }
  }

  private void addWithRetries(String workspace, String collection, String doc, SinkRecord sr) {
    boolean success = this.rc.addDoc(workspace, collection, doc, sr);
    int retries = 0;
    int delay = INITIAL_DELAY;
    while (!success && retries < RETRIES_COUNT) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      success = this.rc.addDoc(workspace, collection, doc, sr);
      retries += 1;
      delay *= 2;
    }
    if (!success) {
      throw new ConnectException(String.format("Add document request timed out " +
          "for document with _id %s, collection %s, and workspace %s",
          RocksetSinkUtils.createId(sr), collection, workspace));
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not Required
  }

  @Override
  public void stop() {
    // Not Required
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
