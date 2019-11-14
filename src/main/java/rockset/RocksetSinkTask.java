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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import rockset.utils.RetriableTask;

public class RocksetSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(RocksetSinkTask.class);
  private RocksetWrapper rw;

  // taskExecutorService is responsible to run the task of sending data
  // to Rockset. If the task fails, it submits it to retryExecutorService
  // to be submitted back to taskExecutorService after a delay.
  private BlockingExecutor taskExecutorService;

  // retryExecutorService scheduled tasks to be retried after a delay and
  // submits it to taskExecutorService. If the retryExecutorService is full
  // it will fail the task immediately (retrying is best effort)
  // make sure this has more threads than the task executor always
  private ExecutorService retryExecutorService;

  private Map<TopicPartition, List<RetriableTask>> futureMap;
  private RocksetConnectorConfig config;
  private RecordParser recordParser;

  private static final int BATCH_SIZE = 1000;

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
    this.taskExecutorService = new BlockingExecutor(numThreads,
        Executors.newFixedThreadPool(numThreads));

    this.retryExecutorService = new ThreadPoolExecutor(
        numThreads * 2, numThreads * 2,
        1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(numThreads * 10));

    this.futureMap = new HashMap<>();
    this.recordParser = getRecordParser(config.getFormat());
  }

  public void start(Map<String, String> settings, RocksetWrapper rw,
                    ExecutorService executorService,
                    ExecutorService retryExecutorService) {
    this.config = new RocksetConnectorConfig(settings);
    this.rw = rw;
    this.taskExecutorService = new BlockingExecutor(config.getRocksetTaskThreads(),
        executorService);
    this.retryExecutorService = retryExecutorService;
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
        RetriableTask task = new RetriableTask(taskExecutorService, retryExecutorService,
            () -> submitTask(toppar.topic(), recordBatch));

        // this should only block if all the threads are busy
        taskExecutorService.submit(task);

        futureMap.computeIfAbsent(toppar, k -> new ArrayList<>()).add(task);
      } catch (InterruptedException e) {
        throw new ConnectException("Failed to put records", e);
      }
    });
  }

  private boolean isRetriableException(Throwable e) {
   return (e.getCause() != null && e.getCause() instanceof RetriableException);
  }

  private void checkForFailures(TopicPartition tp) {
    if (futureMap.get(tp) == null) {
      return;
    }

    List<RetriableTask> futures = futureMap.get(tp);
    Iterator<RetriableTask> futureIterator = futures.iterator();
    while (futureIterator.hasNext()) {
      Future future = futureIterator.next();
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

  private void submitTask(String topic, Collection<SinkRecord> records) {
    log.debug("Adding {} records to Rockset for topic: {}", records.size(), topic);
    this.rw.addDoc(topic, records, recordParser, BATCH_SIZE);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    map.forEach((toppar, offsetAndMetadata) -> {
      log.debug("Flushing for topic: {}, partition: {}, offset: {}, metadata: {}",
          toppar.topic(), toppar.partition(), offsetAndMetadata.offset(), offsetAndMetadata.metadata());
      checkForFailures(toppar);
    });
  }

  @Override
  public void stop() {
    log.info("Stopping Rockset Kafka Connect Plugin, waiting for active tasks to complete");
    if (taskExecutorService != null) {
      taskExecutorService.shutdownNow();
    }
    log.info("Stopped Rockset Kafka Connect Plugin");
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
