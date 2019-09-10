package rockset;

import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ExecutorService;

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
import org.testng.annotations.BeforeTest;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RocksetSinkTaskTest {
  private static final Logger log = LoggerFactory.getLogger(RocksetSinkTaskTest.class);

  @BeforeTest
  public void setUp() {

  }

  @Test
  public void test() {

  }

  public void addDoc(Map settings, Collection records) {
    {
      RocksetClientWrapper rc = Mockito.mock(RocksetClientWrapper.class);
      Mockito.when(rc.addDoc(Mockito.any(), Mockito.any(), Mockito.any(),
          Mockito.any(), Mockito.any(), Mockito.anyInt()))
          .thenReturn(true);
      ExecutorService executorService = MoreExecutors.newDirectExecutorService();
      RocksetSinkTask rst = new RocksetSinkTask();
      rst.start(settings, rc, executorService);

      rst.put(records);
      Mockito.verify(rc)
          .addDoc(Mockito.anyString(), Mockito.anyString(), Mockito.any(),
              Mockito.any(), Mockito.any(), Mockito.anyInt());
    }

    {
      RocksetRequestWrapper rr = Mockito.mock(RocksetRequestWrapper.class);
      Mockito.when(rr.addDoc(Mockito.any(), Mockito.any(), Mockito.any(),
          Mockito.any(), Mockito.any(), Mockito.anyInt()))
          .thenReturn(true);
      ExecutorService executorService = MoreExecutors.newDirectExecutorService();
      RocksetSinkTask rst = new RocksetSinkTask();
      rst.start(settings, rr, executorService);

      rst.put(records);
      Mockito.verify(rr).addDoc(Mockito.anyString(), Mockito.anyString(), Mockito.any(),
          Mockito.any(), Mockito.any(), Mockito.anyInt());
    }
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

    addDoc(settings, records);
  }

  @Test
  public void testPutAvro() {
    Schema schema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct record = new Struct(schema)
        .put("name", "johnny");
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", schema, record, 0);
    Collection records = new ArrayList();
    records.add(sr);

    Map settings = new HashMap();
    settings.put("rockset.apikey", "5");
    settings.put("rockset.collection", "j");
    settings.put("format", "avro");

    addDoc(settings, records);
  }

  @Test
  public void testRetriesPut() {
    SinkRecord sr = new SinkRecord("testRetries", 1, null, "key", null, "{\"name\":\"johnny\"}", 0);
    Collection records = new ArrayList();
    records.add(sr);

    Map settings = new HashMap();
    settings.put("rockset.apikey", "5");
    settings.put("rockset.collection", "j");

    RocksetClientWrapper rc = Mockito.mock(RocksetClientWrapper.class);
    // second put should throw RetriableException
    Mockito.when(rc.addDoc(Mockito.any(), Mockito.any(), Mockito.any(),
        Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(false);
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();
    RocksetSinkTask rst = new RocksetSinkTask();
    rst.start(settings, rc, executorService);
    rst.put(records);
    assertThrows(RetriableException.class, () -> {
      rst.put(records);
    });
  }

  @Test
  public void testRetriesFlush() {
    SinkRecord sr = new SinkRecord("testRetries", 1, null, "key", null, "{\"name\":\"johnny\"}", 0);
    Collection records = new ArrayList();
    records.add(sr);

    Map settings = new HashMap();
    settings.put("rockset.apikey", "5");
    settings.put("rockset.collection", "j");

    RocksetClientWrapper rc = Mockito.mock(RocksetClientWrapper.class);
    Mockito.when(rc.addDoc(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
        Mockito.any(), Mockito.anyInt())).thenReturn(false);
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();
    RocksetSinkTask rst = new RocksetSinkTask();
    rst.start(settings, rc, executorService);
    rst.put(records);
    Map<TopicPartition, OffsetAndMetadata> map = new HashMap();
    map.put(new TopicPartition("testRetries", 1), new OffsetAndMetadata(1L));
    assertThrows(RetriableException.class, () -> {
      rst.flush(map);
    });
  }
}
