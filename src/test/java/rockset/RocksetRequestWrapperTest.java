package rockset;

import static org.junit.jupiter.api.Assertions.assertThrows;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RocksetRequestWrapperTest {
  private static RocksetConnectorConfig rcc;

  @BeforeAll
  public static void setup() {
    Map<String, String> settings = new HashMap<>();
    settings.put("rockset.integration.key", "kafka://5");
    rcc = new RocksetConnectorConfig(settings);
  }

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
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", null, "{\"name\": \"johnny\"}", 0, 15L, TimestampType.CREATE_TIME);

    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw =
        new RocksetRequestWrapper(rcc, client);
    rrw.addDoc("testPut", Arrays.asList(sr), new JsonParser(), 10);

    Mockito.verify(client, Mockito.times(1)).newCall(Mockito.any());
  }

  // Add a document with a null key using the JsonParser
  @Test
  public void testAddDocNullKey() throws Exception {
    Object key = null;
    SinkRecord sr = new SinkRecord("testPut", 1, null, key, null, "{\"name\": \"johnny\"}", 0);

    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw =
        new RocksetRequestWrapper(rcc, client);
    rrw.addDoc("testPut", Arrays.asList(sr), new JsonParser(), 10);

    Mockito.verify(client, Mockito.times(1)).newCall(Mockito.any());
  }

  // Add a document with a null Value using the JsonParser
  @Test
  public void testAddDocNullValue() throws Exception {
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", null, null, 0);

    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw =
        new RocksetRequestWrapper(rcc, client);
    rrw.addDoc("testPut", Arrays.asList(sr), new JsonParser(), 10);

    Mockito.verify(client, Mockito.times(1)).newCall(Mockito.any());
  }

  @Test
  public void testAddDocAvro() throws Exception {
    Schema schema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct record = new Struct(schema)
        .put("name", "johnny");
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", schema, record, 0);

    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw = new RocksetRequestWrapper(rcc, client);

    rrw.addDoc("testPut", Arrays.asList(sr), new AvroParser(), 10);

    Mockito.verify(client, Mockito.times(1)).newCall(Mockito.any());
  }

  @Test
  public void testAddDocBatch() throws Exception {
    SinkRecord sr1 = new SinkRecord("testPut1", 1, null, "key", null, "{\"name\": \"johnny\"}", 1);
    SinkRecord sr2 = new SinkRecord("testPut2", 1, null, "key", null, "{\"name\": \"johnny\"}", 2);

    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw =
        new RocksetRequestWrapper(rcc, client);

    rrw.addDoc("testPut", Arrays.asList(sr1, sr2), new JsonParser(), 1);

    Mockito.verify(client, Mockito.times(2)).newCall(Mockito.any());
  }

  @Test
  public void testAddDocRetry() throws Exception {
    SinkRecord sr1 = new SinkRecord("testPut1", 1, null, "key", null, "{\"name\": \"johnny\"}", 1);
    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw = new RocksetRequestWrapper(rcc, client);

    // configure client to throw on execute call
    Mockito.when(client.newCall(Mockito.any()).execute())
        .thenThrow(new SocketTimeoutException());

    // addDoc should throw retriable exception
    assertThrows(RetriableException.class, () ->
        rrw.addDoc("testPut", Arrays.asList(sr1), new JsonParser(), 1));
  }

  // Add a doc with a null key using the Avro parser
  @Test
  public void testAddDocAvroNullKey() throws Exception {
    Schema schema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct record = new Struct(schema)
        .put("name", "johnny");
    Object key = null;
    SinkRecord sr = new SinkRecord("testPut", 1, null, key, schema, record, 0);

    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw = new RocksetRequestWrapper(rcc, client);

    rrw.addDoc("testPut", Arrays.asList(sr), new AvroParser(), 10);

    Mockito.verify(client, Mockito.times(1)).newCall(Mockito.any());
  }

  // Add a doc with a null value using the Avro parser
  @Test
  public void testAddDocAvroNullValue() throws Exception {
    Schema schema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Object value = null;
    SinkRecord sr = new SinkRecord("testPut", 1, null, "key", schema, value, 0);

    OkHttpClient client = getMockOkHttpClient();

    RocksetRequestWrapper rrw = new RocksetRequestWrapper(rcc, client);

    rrw.addDoc("testPut", Arrays.asList(sr), new AvroParser(), 10);

    Mockito.verify(client, Mockito.times(1)).newCall(Mockito.any());
  }
}
