package rockset;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksetSinkTaskTest {
  private static final Logger log = LoggerFactory.getLogger(RocksetSinkTaskTest.class);

  private void addDoc(String topic, Map settings, Collection records) {
    RocksetRequestWrapper rr = Mockito.mock(RocksetRequestWrapper.class);
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();
    ExecutorService retryExecutorService = MoreExecutors.newDirectExecutorService();

    RocksetSinkTask rst = new RocksetSinkTask();
    rst.start(settings, rr, executorService, retryExecutorService);
    rst.open(Collections.singleton(new TopicPartition(topic, 1)));

    rst.put(records);

    Map<TopicPartition, OffsetAndMetadata> map = new HashMap();
    map.put(new TopicPartition(topic, 1), new OffsetAndMetadata(1L));
    rst.flush(map);
    rst.close(Collections.singleton(new TopicPartition(topic, 1)));

    Mockito.verify(rr).addDoc(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
  }

  @Test
  public void testPutJson() {
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", null, "{\"name\":\"johnny\"}", 0);
    Collection records = new ArrayList();
    records.add(sr);

    Map settings = new HashMap();
    settings.put("rockset.apikey", "5");
    settings.put("rockset.collection", "j");
    settings.put("format", "JSON");

    addDoc("testPut", settings, records);
  }

  @Test
  public void testPutAvro() {
    Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    Struct record = new Struct(schema).put("name", "johnny");
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", schema, record, 0);
    Collection records = new ArrayList();
    records.add(sr);

    Map settings = new HashMap();
    settings.put("rockset.apikey", "5");
    settings.put("rockset.collection", "j");
    settings.put("format", "avro");

    addDoc("testPut", settings, records);
  }

  /** Verify that puts do not throw exception, but a suceeding flush does */
  @Test
  public void testRetriesPut() {
    String topic = "testRetries";
    int partition = 1;
    SinkRecord sr = new SinkRecord(topic, partition, null, "key", null, "{\"name\":\"johnny\"}", 0);
    Collection records = new ArrayList();
    records.add(sr);

    Map settings = new HashMap();
    settings.put("rockset.apikey", "5");
    settings.put("rockset.collection", "j");

    RocksetRequestWrapper rc = Mockito.mock(RocksetRequestWrapper.class);
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();
    ExecutorService retryExecutorService = Executors.newFixedThreadPool(2);

    RocksetSinkTask rst = new RocksetSinkTask();
    rst.start(settings, rc, executorService, retryExecutorService);

    // Do a put that simulates throwing Retryable exception from apiserver
    // The put does not throw, but rather the succeeding flush throws the exception.
    Mockito.doThrow(new RetriableException("retry"))
        .when(rc)
        .addDoc(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
    rst.put(records);

    Map<TopicPartition, OffsetAndMetadata> fmap = new HashMap<>();
    fmap.put(new TopicPartition(topic, partition), new OffsetAndMetadata(0));
    assertThrows(RetriableException.class, () -> rst.flush(fmap));

    // The previous flush has thrown an exception and the exception is cleared.
    // New puts should be successful.
    Mockito.doNothing()
        .when(rc)
        .addDoc(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
    rst.put(records);
    rst.flush(fmap);
  }

  @Test
  public void testRetriesFlush() {
    SinkRecord sr = new SinkRecord("testRetries", 1, null, "key", null, "{\"name\":\"johnny\"}", 0);
    Collection records = new ArrayList();
    records.add(sr);

    Map settings = new HashMap();
    settings.put("rockset.apikey", "5");
    settings.put("rockset.collection", "j");

    RocksetRequestWrapper rc = Mockito.mock(RocksetRequestWrapper.class);
    Mockito.doThrow(new RetriableException("retry"))
        .when(rc)
        .addDoc(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());

    ExecutorService executorService = MoreExecutors.newDirectExecutorService();
    ExecutorService retryExecutorService = Executors.newFixedThreadPool(2);

    RocksetSinkTask rst = new RocksetSinkTask();
    rst.start(settings, rc, executorService, retryExecutorService);
    rst.put(records);

    Map<TopicPartition, OffsetAndMetadata> map = new HashMap();
    map.put(new TopicPartition("testRetries", 1), new OffsetAndMetadata(1L));
    assertThrows(RetriableException.class, () -> rst.flush(map));
  }
}
