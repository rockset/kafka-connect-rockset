package rockset;

import com.rockset.client.RocksetClient;
import com.rockset.client.api.DocumentsApi;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RocksetClientWrapperTest {
  private static RocksetConnectorConfig rcc;

  @BeforeAll
  public static void setup() {
    Map<String, String> settings = new HashMap<>();
    settings.put("rockset.apikey", "dummy");
    settings.put("rockset.workspace", "kafka-workspace");
    settings.put("rockset.collection", "kafka-collection");
    rcc = new RocksetConnectorConfig(settings);
  }

  @Test
  public void testAddDocJson() {
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", null, "{\"name\": \"johnny\"}", 0);

    RocksetClientWrapper rcw = new RocksetClientWrapper(rcc,
            Mockito.mock(RocksetClient.class),
            Mockito.mock(DocumentsApi.class));
    rcw.addDoc("testPut", Arrays.asList(sr), new JsonParser(), 10);
  }

  @Test
  public void testAddDocAvro() {
    Schema schema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct record = new Struct(schema)
        .put("name", "johnny");
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", schema, record, 0);

    RocksetClientWrapper rcw = new RocksetClientWrapper(rcc,
            Mockito.mock(RocksetClient.class),
            Mockito.mock(DocumentsApi.class));

    rcw.addDoc("testPut", Collections.singletonList(sr), new AvroParser(), 10);
  }
}
