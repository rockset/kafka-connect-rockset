package rockset;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import rockset.utils.BlockingExecutor;

public class RocksetSinkTaskTest {
  private static final Logger log = LoggerFactory.getLogger(RocksetSinkTaskTest.class);

  private void addDoc(String topic, Map<String, String> settings, Collection<SinkRecord> records) {
    RocksetRequestWrapper rr = Mockito.mock(RocksetRequestWrapper.class);
    BlockingExecutor executorService = new BlockingExecutor(1, Executors.newScheduledThreadPool(1));

    TopicPartition tp = new TopicPartition(topic, 1);
    Collection<TopicPartition> assignedPartitions = Collections.singleton(tp);
    Map<TopicPartition, OffsetAndMetadata> flushPartitions =
        Collections.singletonMap(tp, new OffsetAndMetadata(1L));

    RocksetSinkTask rst = new RocksetSinkTask();
    rst.start(settings, rr, executorService);
    rst.open(assignedPartitions);
    rst.put(records);
    rst.flush(flushPartitions);
    rst.close(assignedPartitions);
    rst.stop();

    Mockito.verify(rr).addDoc(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
  }

  @Test
  public void testPutJson() {
    String topic = "testPutJson";
    SinkRecord sr = new SinkRecord(topic, 1, null, "key", null, "{\"name\":\"johnny\"}", 0);
    Collection<SinkRecord> records = new ArrayList<>();
    records.add(sr);

    Map<String, String> settings = new HashMap<>();
    settings.put("format", "JSON");

    addDoc(topic, settings, records);
  }

  @Test
  public void testPutAvro() {
    String topic = "testPutAvro";
    Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    Struct record = new Struct(schema).put("name", "johnny");
    SinkRecord sr = new SinkRecord(topic, 1, null, "key", schema, record, 0);
    Collection<SinkRecord> records = new ArrayList<>();
    records.add(sr);

    Map<String, String> settings = new HashMap<>();
    settings.put("format", "avro");

    addDoc(topic, settings, records);
  }

  /** Verify that puts throw an exception, not a succeeding flush */
  @DataProvider(name = "bothBooleans")
  public static Object[][] bothBooleans() {
    return new Object[][] {{true}, {false}};
  }

  // Check that exceptions are thrown by put() and not flush(). If exceptions are delayed until
  // flush() then multiple preceding put() calls can have their writes interleaved incorrectly
  @Test(dataProvider = "bothBooleans")
  public void testException(boolean retriable) {
    String topic = "testException";
    int partition = 1;
    SinkRecord sr = new SinkRecord(topic, partition, null, "key", null, "{\"name\":\"johnny\"}", 0);
    Collection<SinkRecord> records = new ArrayList<>();
    records.add(sr);

    Map<String, String> settings = new HashMap<>();

    RocksetRequestWrapper rc = Mockito.mock(RocksetRequestWrapper.class);
    BlockingExecutor executorService = new BlockingExecutor(1, Executors.newScheduledThreadPool(1));

    RocksetSinkTask rst = new RocksetSinkTask();
    rst.start(settings, rc, executorService);
    rst.open(Collections.singleton(new TopicPartition(topic, partition)));

    // Do a put that simulates throwing Retryable exception from apiserver
    Mockito.doThrow(retriable ? new RetriableException("retry") : new ConnectException("error"))
        .when(rc)
        .addDoc(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
    Map<TopicPartition, OffsetAndMetadata> fmap = new HashMap<>();
    fmap.put(new TopicPartition(topic, partition), new OffsetAndMetadata(0));

    // It's important that the put() throws the retry and not flush() so that
    // no future put() calls for this topic-partition can happen until this one succeeds
    if (retriable) {
      assertThrows(RetriableException.class, () -> rst.put(records));
    } else {
      assertThrows(ConnectException.class, () -> rst.put(records));
    }

    // New puts should be successful.
    Mockito.doNothing()
        .when(rc)
        .addDoc(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
    rst.put(records);
    rst.flush(fmap);
    rst.close(Collections.singleton(new TopicPartition(topic, partition)));
    rst.stop();
  }

  // If any request fails during a call to put() then all other requests stemming from that same
  // put() should be cancelled and awaited before put() returns
  @Test
  public void testNoLeakedWrites() {
    String topic1 = "testNoLeakedWrites-1";
    SinkRecord sr1 = new SinkRecord(topic1, 1, null, "key", null, "{\"name\":\"johnny\"}", 0);
    String topic2 = "testNoLeakedWrites-2";
    SinkRecord sr2 = new SinkRecord(topic2, 1, null, "key", null, "{\"name\":\"johnny\"}", 0);

    Map<String, String> settings = new HashMap<>();

    RocksetRequestWrapper rc = Mockito.mock(RocksetRequestWrapper.class);
    BlockingExecutor executorService = new BlockingExecutor(2, Executors.newScheduledThreadPool(2));

    RocksetSinkTask rst = new RocksetSinkTask();
    rst.start(settings, rc, executorService);
    rst.open(Arrays.asList(new TopicPartition(topic1, 1), new TopicPartition(topic2, 1)));

    CountDownLatch readyLatch = new CountDownLatch(2);
    CountDownLatch topic1PutLatch = new CountDownLatch(1);
    CountDownLatch topic2PutLatch = new CountDownLatch(1);
    CountDownLatch topic2InterruptedLatch = new CountDownLatch(1);

    // Do a put that simulates throwing an exception for topic1 and expect topic2 gets interrupted
    Mockito.doAnswer(
            invocationOnMock -> {
              readyLatch.countDown();
              topic1PutLatch.await();
              throw new ConnectException("error");
            })
        .when(rc)
        .addDoc(Mockito.eq(topic1), Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.doAnswer(
            invocationOnMock -> {
              try {
                readyLatch.countDown();

                topic2PutLatch.await();
              } catch (InterruptedException e) {
                topic2InterruptedLatch.countDown();
              }
              return null;
            })
        .when(rc)
        .addDoc(Mockito.eq(topic2), Mockito.any(), Mockito.any(), Mockito.anyInt());

    // Need to run put() in a separate thread since it is blocking
    ExecutorService service = Executors.newFixedThreadPool(1);
    Future<?> result =
        service.submit(
            () -> {
              rst.put(Arrays.asList(sr1, sr2));
            });

    // Wait until both threads have entered addDocs()
    try {
      assertTrue(readyLatch.await(3, TimeUnit.SECONDS));
    } catch (InterruptedException unused) {
      Assertions.fail("Should not have been interrupted");
    }

    // Unblock the thread for topic1 which will throw an exception. The background
    // put() call above should catch this exception and cancel the thread for topic2
    topic1PutLatch.countDown();

    try {
      assertTrue(topic2InterruptedLatch.await(3, TimeUnit.SECONDS));
    } catch (InterruptedException unused) {
      Assertions.fail("Should not have been interrupted");
    }

    // At this point the put() should have completed since both threads are exited and
    // the result should be an exception
    boolean executionException = false;
    try {
      result.get(3, TimeUnit.SECONDS);
    } catch (ExecutionException unused) {
      // this should happen
      executionException = true;
    } catch (Exception unused) {
      Assertions.fail("Unexpected error");
    }
    assertTrue(executionException);
  }
}
