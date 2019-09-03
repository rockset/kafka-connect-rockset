package rockset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rockset.models.KafkaDocumentsRequest;
import rockset.models.KafkaMessage;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RocksetRequestWrapper implements RocksetWrapper {
  private static Logger log = LoggerFactory.getLogger(RocksetClientWrapper.class);

  public static final MediaType JSON = MediaType.parse("application/json");

  private final String KAFKA_ENDPOINT = "/v1/receivers/kafka";

  private OkHttpClient client;
  private String integrationKeyEncoded;
  private ObjectMapper mapper;
  private String apiServer;

  public RocksetRequestWrapper(String integrationKey, String apiServer) {
    if (client == null) {
      client = new OkHttpClient.Builder()
          .connectTimeout(1, TimeUnit.MINUTES)
          .writeTimeout(1, TimeUnit.MINUTES)
          .readTimeout(1, TimeUnit.MINUTES)
          .build();
    }

    parseConnectionString(integrationKey);
    this.apiServer = apiServer;
    this.mapper = new ObjectMapper();
  }

  // used for testing
  public RocksetRequestWrapper(String integrationKey, String apiServer, OkHttpClient client) {
    this.client = client;

    parseConnectionString(integrationKey);
    this.apiServer = apiServer;
    this.mapper = new ObjectMapper();
  }

  private void parseConnectionString(String integrationKey) {
    this.integrationKeyEncoded = base64EncodeAsUserPassword(integrationKey);
  }

  private static String base64EncodeAsUserPassword(String integrationKey) {
    final String userPassword = integrationKey + ":"; // password is empty
    return Base64.getEncoder().encodeToString(userPassword.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public boolean addDoc(String workspace, String collection, String topic,
                        Collection<SinkRecord> records, RecordParser recordParser) {
    LinkedList<KafkaMessage> list = new LinkedList<>();

    for (SinkRecord record : records) {
      String srId = RocksetSinkUtils.createId(record);
      try {
        Object val = recordParser.parse(record);
        Map<String, Object> doc = mapper.readValue(val.toString(), new TypeReference<Map<String, Object>>() {
        });
        doc.put("_id", srId);
        KafkaMessage message = new KafkaMessage()
            .document(doc)
            .offset(record.kafkaOffset())
            .partition(record.kafkaPartition());
        list.add(message);
      }
      catch (Exception e) {
        throw new ConnectException("Invalid JSON encountered in stream ", e);
      }
    }

    KafkaDocumentsRequest documentsRequest = new KafkaDocumentsRequest()
        .kafkaMessages(list)
        .topic(topic);

    try {
      RequestBody requestBody = RequestBody.create(JSON, mapper.writeValueAsString(documentsRequest));
      Request request = new Request.Builder()
          .url(this.apiServer + KAFKA_ENDPOINT)
          .addHeader("Authorization", "Basic " + integrationKeyEncoded)
          .post(requestBody)
          .build();

      try (Response response = client.newCall(request).execute()) {
        if (response.code() == 500) {
          // return false to retry
          return false;
        }

        if (response.code() != 200) {
          throw new ConnectException(String.format("Unable to write document"
                  + " in Rockset, cause: %s", response.message()));
        }
      }
    } catch (Exception e) {
      throw new ConnectException(e);
    }

    return true;
  }
}
