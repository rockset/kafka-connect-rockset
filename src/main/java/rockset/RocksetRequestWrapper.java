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
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.Map;

public class RocksetRequestWrapper implements RocksetWrapper {
  private static Logger log = LoggerFactory.getLogger(RocksetClientWrapper.class);

  public static final MediaType JSON = MediaType.parse("application/json");

  // TODO fill this
  private final String ROCKSET_APISERVER = "https://api.rs2.usw2.rockset.com";
  private final String KAFKA_ENDPOINT = "/v1/receivers/kafka";

  private OkHttpClient client;
  private String integrationKeyEncoded;
  private ObjectMapper mapper;

  public RocksetRequestWrapper(String integrationKey) {
    if (client == null) {
      client = new OkHttpClient();
    }

    parseConnectionString(integrationKey);
    this.mapper = new ObjectMapper();
  }

  // used for testing
  public RocksetRequestWrapper(String integrationKey, OkHttpClient client) {
    this.client = client;

    parseConnectionString(integrationKey);
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
  public boolean addDoc(String workspace, String collection, String json, SinkRecord sr) {
    LinkedList<KafkaMessage> list = new LinkedList<>();

    String srId = RocksetSinkUtils.createId(sr);
    try {
      Map<String, Object> doc = mapper.readValue(json, new TypeReference<Map<String, Object>>(){});
      doc.put("_id", srId);
      KafkaMessage message = new KafkaMessage()
          .document(doc)
          .offset(sr.kafkaOffset())
          .partition(sr.kafkaPartition());
      list.add(message);
    } catch (Exception e) {
      throw new ConnectException("Invalid JSON encountered in stream " + e);
    }

    KafkaDocumentsRequest documentsRequest = new KafkaDocumentsRequest()
        .kafkaMessages(list)
        .topic(sr.topic());

    try {
      RequestBody requestBody = RequestBody.create(JSON, mapper.writeValueAsString(documentsRequest));
      Request request = new Request.Builder()
          .url(ROCKSET_APISERVER + KAFKA_ENDPOINT)
          .addHeader("Authorization", "Basic " + integrationKeyEncoded)
          .post(requestBody)
          .build();

      try (Response response = client.newCall(request).execute()) {
        if (response.code() == 500) {
          // return false to retry
          return false;
        }

        if (response.code() != 200) {
          throw new ConnectException(String.format("Unable to write document " +
                  "to collection %s, workspace %s in Rockset, cause: %s",
              collection, workspace, response.message()));
        }
      }
    } catch (Exception e) {
      throw new ConnectException(e);
    }

    return true;
  }
}
