package rockset;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

public class RocksetRequestWrapperTest {

  private OkHttpClient getMockOkHttpClient() throws Exception {
    OkHttpClient client = Mockito.mock(OkHttpClient.class);
    Call call = Mockito.mock(Call.class);

    Response response = new Response.Builder()
        .request(new Request.Builder().url("http://dummy/url/").build())
        .body(Mockito.mock(ResponseBody.class))
        .code(200)
        .protocol(Protocol.HTTP_1_1)
        .message("Go Rockset!")
        .build();

    Mockito.when(client.newCall(Mockito.any())).thenReturn(call);
    Mockito.when(call.execute()).thenReturn(response);

    return client;
  }

  @Test
  public void testAddDoc() throws Exception {
    String workspace = "commons";
    String collection = "foo";
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", null, "{\"name\": \"johnny\"}", 0);

    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw =
        new RocksetRequestWrapper("integration_key", "https://api_server", client);
    assert(rrw.addDoc(workspace, collection, "testPut", Arrays.asList(sr), new JsonParser()));
  }

  @Test
  public void testAddDocAvro() throws Exception {
    String workspace = "commons";
    String collection = "foo";

    Schema schema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct record = new Struct(schema)
        .put("name", "johnny");
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", schema, record, 0);

    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw =
        new RocksetRequestWrapper("integration_key", "https://api_server", client);
    assert(rrw.addDoc(workspace, collection, "testPut", Arrays.asList(sr), new AvroParser()));
  }
}
