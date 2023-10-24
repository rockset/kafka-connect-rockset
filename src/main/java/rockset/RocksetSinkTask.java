package rockset;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.google.common.base.Preconditions;
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
import rockset.utils.RetriableTask;

public class RocksetSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(RocksetSinkTask.class);
  private RequestWrapper rw;

  // taskExecutorService is responsible to run the task of sending data
  // to Rockset. If the task fails, it submits it to retryExecutorService
  // to be submitted back to taskExecutorService after a delay.
  private BlockingExecutor taskExecutorService;

  // retryExecutorService scheduled tasks to be retried after a delay and
  // submits it to taskExecutorService. If the retryExecutorService is full
  // it will fail the task immediately (retrying is best effort).
  // Make sure this has more threads than the task executor thread pool always
  // otherwise it can lead to deadlock.
  private ExecutorService retryExecutorService;

  private Map<TopicPartition, List<RetriableTask>> futureMap;
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
    this.rw = new RocksetRequestWrapper(config, httpClient);

    int numThreads = this.config.getRocksetTaskThreads();
    this.taskExecutorService =
        new BlockingExecutor(numThreads, Executors.newFixedThreadPool(numThreads));

    this.retryExecutorService =
        new ThreadPoolExecutor(
            numThreads * 2,
            numThreads * 2,
            1,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(numThreads * 10));

    this.futureMap = new HashMap<>();
    this.recordParser = getRecordParser(config.getFormat());
  }

  @TestOnly
  public void start(
      Map<String, String> settings,
      RequestWrapper rw,
      ExecutorService executorService,
      ExecutorService retryExecutorService) {
    this.config = new RocksetConnectorConfig(settings);
    this.rw = rw;
    this.taskExecutorService =
        new BlockingExecutor(config.getRocksetTaskThreads(), executorService);
    this.retryExecutorService = retryExecutorService;
    this.futureMap = new HashMap<>();
    this.recordParser = getRecordParser(config.getFormat());
  }

  @Override
  public void stop() {
    log.info("Stopping Rockset Kafka Connect Plugin, waiting for active tasks to complete");
    if (taskExecutorService != null) {
      taskExecutorService.shutdownNow();
    }
    log.info("Stopped Rockset Kafka Connect Plugin");
  }

  // open() will be called for a partition before any put() is called for it
  @Override
  public void open(Collection<TopicPartition> partitions) {
    log.debug(String.format("Opening %d partitions: %s", partitions.size(), partitions));
    partitions.forEach(
        tp -> {
          Preconditions.checkState(!futureMap.containsKey(tp));
          futureMap.put(tp, new ArrayList<>());
        });
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    log.debug(String.format("Closing %d partitions: %s", partitions.size(), partitions));
    partitions.forEach(
        tp -> {
          Preconditions.checkState(futureMap.containsKey(tp));
          futureMap.remove(tp);
        });
  }

  // put() doesn't need to block until the writes complete, that is what flush() is for
  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.size() == 0) {
      log.debug("zero records in put call, returning");
      return;
    }

    groupRecordsByTopicPartition(records)
        .forEach(
            (tp, recordBatch) -> {
              try {
                RetriableTask task =
                    new RetriableTask(
                        taskExecutorService,
                        retryExecutorService,
                        () -> addDocs(tp.topic(), recordBatch));

                // this should only block if all the threads are busy
                taskExecutorService.submit(task);

                Preconditions.checkState(futureMap.containsKey(tp));
                futureMap.get(tp).add(task);
              } catch (InterruptedException e) {
                throw new ConnectException("Failed to put records", e);
              }
            });
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    map.forEach(
        (tp, offsetAndMetadata) -> {
          log.debug(
              "Flushing for topic: {}, partition: {}, offset: {}, metadata: {}",
              tp.topic(),
              tp.partition(),
              offsetAndMetadata.offset(),
              offsetAndMetadata.metadata());
          checkForFailures(tp);
        });
  }

  private Map<TopicPartition, Collection<SinkRecord>> groupRecordsByTopicPartition(
      Collection<SinkRecord> records) {
    Map<TopicPartition, Collection<SinkRecord>> topicPartitionedRecords = new HashMap<>();
    for (SinkRecord record : records) {
      TopicPartition key = new TopicPartition(record.topic(), record.kafkaPartition());
      topicPartitionedRecords.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
    }

    return topicPartitionedRecords;
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
      Future<Void> future = futureIterator.next();
      try {
        future.get();
      } catch (Exception e) {
        if (isRetriableException(e)) {
          throw new RetriableException(
              String.format(
                  "Unable to write document for topic: %s, partition: %s, in Rockset,"
                      + " should retry, cause: %s",
                  tp.topic(), tp.partition(), e.getMessage()),
              e);
        }

        throw new RuntimeException(
            String.format(
                "Unable to write document for topic: %s, partition: %s, in Rockset," + " cause: %s",
                tp.topic(), tp.partition(), e.getMessage()),
            e);
      } finally {
        futureIterator.remove();
      }
    }
  }

  private void addDocs(String topic, Collection<SinkRecord> records) {
    log.debug("Adding {} records to Rockset for topic: {}", records.size(), topic);
    this.rw.addDoc(topic, records, recordParser, this.config.getRocksetBatchSize());
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
