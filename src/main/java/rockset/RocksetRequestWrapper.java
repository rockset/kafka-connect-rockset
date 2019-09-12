package rockset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
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

import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RocksetRequestWrapper implements RocksetWrapper {
  private static Logger log = LoggerFactory.getLogger(RocksetClientWrapper.class);

  private static final MediaType JSON = MediaType.parse("application/json");

  private static final String KAFKA_ENDPOINT = "/v1/receivers/kafka";

  private OkHttpClient client;
  private String integrationKeyEncoded;
  private ObjectMapper mapper;
  private String apiServer;

  public RocksetRequestWrapper(RocksetConnectorConfig config) {
    if (client == null) {
      client = new OkHttpClient.Builder()
          .connectTimeout(1, TimeUnit.MINUTES)
          .writeTimeout(1, TimeUnit.MINUTES)
          .readTimeout(1, TimeUnit.MINUTES)
          .build();
    }

    parseConnectionString(config.getRocksetIntegrationKey());
    this.apiServer = config.getRocksetApiServerUrl();
    this.mapper = new ObjectMapper();
  }

  // used for testing
  public RocksetRequestWrapper(RocksetConnectorConfig config,
                               OkHttpClient client) {
    this.client = client;

    parseConnectionString(config.getRocksetApiServerUrl());
    this.apiServer = config.getRocksetApiServerUrl();
    this.mapper = new ObjectMapper();
  }

  private void parseConnectionString(String integrationKey) {
    this.integrationKeyEncoded = base64EncodeAsUserPassword(integrationKey);
  }

  private static String base64EncodeAsUserPassword(String integrationKey) {
    final String userPassword = integrationKey + ":"; // password is empty
    return Base64.getEncoder().encodeToString(userPassword.getBytes(StandardCharsets.UTF_8));
  }

  private boolean isInternalError(int code) {
    return code == 500 || code == 502 || code == 503 || code == 504;
  }

  @Override
  public boolean addDoc(String topic, Collection<SinkRecord> records,
                        RecordParser recordParser, int batchSize) {
    List<KafkaMessage> messages = new LinkedList<>();

    for (SinkRecord record : records) {
      // if the size exceeds batchsize, send the docs
      if (messages.size() >= batchSize) {
        // if sendDocs failed returned false
        if (!sendDocs(topic, messages)) {
          return false;
        }

        messages.clear();
      }

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
        messages.add(message);
      }
      catch (Exception e) {
        throw new ConnectException("Invalid JSON encountered in stream ", e);
      }
    }

    return sendDocs(topic, messages);
  }

  private boolean sendDocs(String topic, List<KafkaMessage> messages) {
    Preconditions.checkArgument(!messages.isEmpty());

    KafkaDocumentsRequest documentsRequest = new KafkaDocumentsRequest()
        .kafkaMessages(messages)
        .topic(topic);

    try {
      RequestBody requestBody = RequestBody.create(JSON, mapper.writeValueAsString(documentsRequest));
      Request request = new Request.Builder()
          .url(this.apiServer + KAFKA_ENDPOINT)
          .addHeader("Authorization", "Basic " + integrationKeyEncoded)
          .post(requestBody)
          .build();

      try (Response response = client.newCall(request).execute()) {
        if (isInternalError(response.code())) {
          // return false to retry
          return false;
        }

        if (response.code() != 200) {
          throw new ConnectException(String.format("Unable to write document"
                  + " in Rockset, cause: %s", response.message()));
        }
      }
    } catch (SocketTimeoutException ste) {
      log.warn("Encountered socket timeout exception. Can Retry", ste);
      return false;
    } catch (Exception e) {
      throw new ConnectException(e);
    }

    return true;
  }
}
