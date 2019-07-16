package rockset;

import com.rockset.client.RocksetClient;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class RocksetClientWrapperTest {
  @Test
  public void testAddDoc() {
    String workspace = "commons";
    String collection = "foo";
    String json = "{\"name\":\"johnny\"}";
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", null, "{'name':'johnny'}", 0);

    RocksetClientWrapper rcw = new RocksetClientWrapper(Mockito.mock(RocksetClient.class));
    assert(rcw.addDoc(workspace, collection, json, sr));
  }
}
