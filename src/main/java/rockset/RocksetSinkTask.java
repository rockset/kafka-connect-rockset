package rockset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rockset.utils.BlockingExecutor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

public class RocksetSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(RocksetSinkTask.class);
  private RocksetWrapper rw;
  private BlockingExecutor executorService;
  private Map<TopicPartition, List<Future>> futureMap;
  private RocksetConnectorConfig config;
  private RecordParser recordParser;

  public static final int BATCH_SIZE = 1000;
  public static final int RETRIES_COUNT = 5;
  public static final int INITIAL_DELAY = 250;
  public static final double JITTER_FACTOR = 0.2;

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
    this.config = new RocksetConnectorConfig(settings);
    this.rw = RocksetClientFactory.getRocksetWrapper(config);

    int numThreads = this.config.getRocksetTaskThreads();
    this.executorService = new BlockingExecutor(numThreads,
        Executors.newFixedThreadPool(numThreads));
    this.futureMap = new HashMap<>();
    this.recordParser = getRecordParser(config.getFormat());

  }

  public void start(Map<String, String> settings, RocksetWrapper rw,
                    ExecutorService executorService) {
    this.config = new RocksetConnectorConfig(settings);
    this.rw = rw;
    this.executorService = new BlockingExecutor(config.getRocksetTaskThreads(), executorService);
    this.futureMap = new HashMap<>();
    this.recordParser = getRecordParser(config.getFormat());
    log.info("Starting Rockset Kafka Connect Plugin");
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.size() == 0) {
      log.debug("zero records in put call, returning");
      return;
    }

    submitForProcessing(records);
  }

  private Map<TopicPartition, Collection<SinkRecord>> partitionRecordsByTopic(
      Collection<SinkRecord> records) {
    Map<TopicPartition, Collection<SinkRecord>> topicPartitionedRecords = new HashMap<>();
    for (SinkRecord record: records) {
      TopicPartition key = new TopicPartition(record.topic(), record.kafkaPartition());
      topicPartitionedRecords.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
    }

    return topicPartitionedRecords;
  }

  private void submitForProcessing(Collection<SinkRecord> records) {
    partitionRecordsByTopic(records).forEach((toppar, recordBatch) -> {
      try {
        checkForFailures(toppar, false);
        futureMap.computeIfAbsent(toppar, k -> new ArrayList<>())
            .add(executorService.submit(() -> addWithRetries(toppar.topic(), recordBatch)));
      } catch (InterruptedException e) {
        throw new ConnectException("Failed to put records", e);
      }
    });
  }

  private boolean isRetriableException(Throwable e) {
   return (e.getCause() != null && e.getCause() instanceof RetriableException);
  }

  private void checkForFailures(TopicPartition tp, boolean wait) {
    if (futureMap.get(tp) == null) {
      return;
    }

    List<Future> futures = futureMap.get(tp);
    Iterator<Future> futureIterator = futures.iterator();
    while (futureIterator.hasNext()) {
      Future future = futureIterator.next();
      // this is blocking only if wait is true
      if (wait || future.isDone()) {
        try {
          future.get();
        } catch (Exception e) {
          if (isRetriableException(e)) {
            throw new RetriableException(
                String.format("Unable to write document for topic: %s, partition: %s, in Rockset,"
                    + " should retry, cause: %s", tp.topic(), tp.partition(), e.getMessage()), e);
          }

          throw new RuntimeException(
              String.format("Unable to write document for topic: %s, partition: %s, in Rockset,"
                  + " cause: %s", tp.topic(), tp.partition(), e.getMessage()), e);
        } finally {
          futureIterator.remove();
        }
      }
    }
  }

  // TODO improve this logic
  private void addWithRetries(String topic, Collection<SinkRecord> records) {
    log.debug("Adding %s records to Rockset for topic: %s", records.size(), topic);
    boolean success = this.rw.addDoc(topic, records, recordParser, BATCH_SIZE);
    int retries = 0;
    int delay = INITIAL_DELAY;
    while (!success && retries < RETRIES_COUNT) {
      log.debug("Retrying adding %s docs to Rockset for topic: %s", records.size(), topic);
      try {
        Thread.sleep((long) (delay * (1 + JITTER_FACTOR * ThreadLocalRandom.current()
            .nextDouble(-1, 1))));
      }
      catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      // addDoc throws ConnectException if it's not Internal Error
      success = this.rw.addDoc(topic, records, recordParser, BATCH_SIZE);
      retries += 1;
      delay *= 2;
    }
    if (!success) {
      throw new RetriableException(String.format("Add document request timed out "
          + " for topic: %s", topic));
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    for (Map.Entry<TopicPartition, OffsetAndMetadata> toe : map.entrySet()) {
      log.debug("Flusing for topic: %s, partition: %s", toe.getKey(), toe.getValue());
      checkForFailures(toe.getKey(), true);
    }
  }

  @Override
  public void stop() {
    log.info("Stopping Rockset Kafka Connect Plugin, waiting for active tasks to complete");
    executorService.shutdown();
    log.info("Stopped Rockset Kafka Connect Plugin");
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
