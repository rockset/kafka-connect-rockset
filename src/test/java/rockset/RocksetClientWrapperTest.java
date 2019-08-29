package rockset;

import com.rockset.client.RocksetClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testng.Assert;

import java.util.Arrays;

public class RocksetClientWrapperTest {
  @Test
  public void testAddDocJson() {
    String workspace = "commons";
    String collection = "foo";
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", null, "{\"name\": \"johnny\"}", 0);

    RocksetClientWrapper rcw = new RocksetClientWrapper(Mockito.mock(RocksetClient.class));
    Assert.assertTrue(rcw.addDoc(workspace, collection, Arrays.asList(sr), new JsonParser()));
  }

  @Test
  public void testAddDocAvro() {
    String workspace = "commons";
    String collection = "foo";

    Schema schema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct record = new Struct(schema)
        .put("name", "johnny");
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", schema, record, 0);

    RocksetClientWrapper rcw = new RocksetClientWrapper(Mockito.mock(RocksetClient.class));

    Assert.assertTrue(rcw.addDoc(workspace, collection, Arrays.asList(sr), new AvroParser()));
  }
}
