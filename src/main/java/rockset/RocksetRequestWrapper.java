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
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rockset.models.KafkaDocumentsRequest;
import rockset.models.KafkaMessage;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RocksetRequestWrapper implements RocksetWrapper {
  private static Logger log = LoggerFactory.getLogger(RocksetRequestWrapper.class);

  private static final MediaType JSON = MediaType.parse("application/json");

  private static final String KAFKA_ENDPOINT = "/v1/receivers/kafka";
  private static final ObjectMapper mapper = new ObjectMapper();

  private OkHttpClient client;
  private String integrationKeyEncoded;
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
  }

  // used for testing
  public RocksetRequestWrapper(RocksetConnectorConfig config,
                               OkHttpClient client) {
    this.client = client;

    parseConnectionString(config.getRocksetApiServerUrl());
    this.apiServer = config.getRocksetApiServerUrl();
  }

  private void parseConnectionString(String integrationKey) {
    this.integrationKeyEncoded = base64EncodeAsUserPassword(integrationKey);
  }

  private static String base64EncodeAsUserPassword(String integrationKey) {
    final String userPassword = integrationKey + ":"; // password is empty
    return Base64.getEncoder().encodeToString(userPassword.getBytes(StandardCharsets.UTF_8));
  }

  private boolean isInternalError(int code) {
    return code == 500  // INTERNALERROR
        || code == 502
        || code == 503  // NOT_READY
        || code == 504
        || code == 429; // RESOURCEEXCEEDED
  }

  @Override
  public void addDoc(String topic, Collection<SinkRecord> records,
                     RecordParser recordParser, int batchSize) {
    List<KafkaMessage> messages = new LinkedList<>();

    for (SinkRecord record : records) {
      // if the size exceeds batchsize, send the docs
      if (messages.size() >= batchSize) {
        sendDocs(topic, messages);
        messages.clear();
      }

      try {
        Object key = recordParser.parseKey(record);
        Map<String, Object> doc = toMap(recordParser.parseValue(record));

        KafkaMessage message = new KafkaMessage()
            .document(doc)
            .key(key)
            .offset(record.kafkaOffset())
            .partition(record.kafkaPartition());
        messages.add(message);
      } catch (Exception e) {
        throw new ConnectException("Invalid JSON encountered in stream ", e);
      }
    }

    sendDocs(topic, messages);
  }

  private void sendDocs(String topic, List<KafkaMessage> messages) {
    Preconditions.checkArgument(!messages.isEmpty());
    log.debug("Sending batch of {} messages for topic: {} to Rockset", messages.size(), topic);

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
          // internal errors are retriable
          throw new RetriableException(String.format(
              "Received internal error code: %s, message: %s", response.code(), response.message()));
        }

        if (response.code() != 200) {
          throw new ConnectException(String.format("Unable to write document"
                  + " in Rockset, cause: %s", response.message()));
        }
      }
    } catch (SocketTimeoutException ste) {
      throw new RetriableException("Encountered socket timeout exception. Can Retry", ste);
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  private static Map<String, Object> toMap(Object value) throws IOException {
    return mapper.readValue(value.toString(), new TypeReference<Map<String, Object>>() {});
  }
}
