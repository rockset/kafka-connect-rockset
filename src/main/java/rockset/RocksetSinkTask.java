package rockset;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rockset.parser.AvroParser;
import rockset.parser.JsonParser;
import rockset.parser.RecordParser;
import rockset.utils.BlockingExecutor;

public class RocksetSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(RocksetSinkTask.class);

  private RequestWrapper requestWrapper;

  private BlockingExecutor executorService;

  private RocksetConnectorConfig config;
  private RecordParser recordParser;

  private RecordParser getRecordParser(String format) {
    switch (format.toLowerCase()) {
      case "json":
        return new JsonParser();
      case "avro":
        return new AvroParser();
      default:
        throw new ConnectException(String.format("Format %s not supported", format));
    }
  }

  @Override
  public void start(Map<String, String> settings) {
    log.info("Starting Rockset Kafka Connect Plugin");
    this.config = new RocksetConnectorConfig(settings);
    OkHttpClient httpClient =
        new OkHttpClient.Builder()
            .connectTimeout(1, TimeUnit.MINUTES)
            .writeTimeout(1, TimeUnit.MINUTES)
            .readTimeout(1, TimeUnit.MINUTES)
            .build();
    this.requestWrapper = new RocksetRequestWrapper(config, httpClient);
    this.executorService =
        new BlockingExecutor(
            config.getRocksetTaskThreads(),
            Executors.newScheduledThreadPool(config.getRocksetTaskThreads()));
    this.recordParser = getRecordParser(config.getFormat());
  }

  @TestOnly
  public void start(
      Map<String, String> settings,
      RequestWrapper requestWrapper,
      BlockingExecutor executorService) {
    this.config = new RocksetConnectorConfig(settings);
    this.requestWrapper = requestWrapper;
    this.executorService = executorService;
    this.recordParser = getRecordParser(config.getFormat());
  }

  @Override
  public void stop() {
    log.info("Stopping Rockset Kafka Connect Plugin, waiting for active tasks to complete");
    if (executorService != null) {
      executorService.shutdownNow();
    }
    log.info("Stopped Rockset Kafka Connect Plugin");
  }

  // open() will be called for a partition before any put() is called for it
  @Override
  public void open(Collection<TopicPartition> partitions) {
    log.debug("Opening {} partitions: {}", partitions.size(), partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    log.debug("Closing {} partitions: {}", partitions.size(), partitions);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    Map<TopicPartition, Future<?>> futures =
        records.stream()
            .collect(Collectors.groupingBy(r -> new TopicPartition(r.topic(), r.kafkaPartition())))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        executorService.submit(
                            () ->
                                  requestWrapper.addDoc(
                                    e.getKey().topic(),
                                    e.getValue(),
                                    recordParser,
                                    config.getRocksetBatchSize()))));
    futures.forEach(
        (tp, f) -> {
          try {
            f.get();
          } catch (Exception e) {
            // Cancel all tasks still executing since this put() will be retried anyway
            futures.values().forEach(g -> g.cancel(true));
            // Wait for all cancelled tasks to complete before returning since otherwise a
            // long-running task from this put() could continue into the next call of put()
            futures
                .values()
                .forEach(
                    g -> {
                      try {
                        g.get();
                      } catch (Exception ignored) {
                      }
                    });
            // Now that all threads have completed, we can handle the original exception
            if (isRetriableException(e)) {
              if (context != null) {
                context.timeout(config.getRetryBackoffMs());
              }
              throw new RetriableException(
                  String.format(
                      "Encountered a retriable exception for topic %s partition %s",
                      tp.topic(), tp.partition()),
                  e);
            }
            throw new ConnectException(
                String.format(
                    "Encountered an unexpected exception for topic %s partition %s",
                    tp.topic(), tp.partition()),
                e);
          }
        });
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not needed
  }

  private boolean isRetriableException(Throwable e) {
    return (e.getCause() != null && e.getCause() instanceof RetriableException);
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
