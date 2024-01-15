package rockset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rockset.models.KafkaDocumentsRequest;
import rockset.models.KafkaMessage;
import rockset.parser.RecordParser;

public class RocksetRequestWrapper implements RequestWrapper {
  private static Logger log = LoggerFactory.getLogger(RocksetRequestWrapper.class);

  private static final MediaType JSON = MediaType.parse("application/json");

  private static final String KAFKA_ENDPOINT = "/v1/receivers/kafka";
  private static final ObjectMapper mapper = new ObjectMapper();

  private final OkHttpClient client;
  private final String integrationKeyEncoded;
  private final String apiServer;

  public RocksetRequestWrapper(RocksetConnectorConfig config, OkHttpClient client) {
    this.client = client;
    this.integrationKeyEncoded = base64EncodeAsUserPassword(config.getRocksetIntegrationKey());
    this.apiServer = config.getRocksetApiServerUrl();
  }

  private static String base64EncodeAsUserPassword(String integrationKey) {
    final String userPassword = integrationKey + ":"; // password is empty
    return Base64.getEncoder().encodeToString(userPassword.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void addDoc(
      String topic, Collection<SinkRecord> records, RecordParser recordParser, int batchSize) {
    List<KafkaMessage> messages = new LinkedList<>();

    for (SinkRecord record : records) {
      // if the size exceeds batchsize, send the docs
      if (messages.size() >= batchSize) {
        sendDocs(topic, messages);
        messages.clear();
      }

      try {
        Object key = recordParser.parseKey(record);
        Map<String, Object> doc = recordParser.parseValue(record);

        KafkaMessage message =
            new KafkaMessage()
                .document(doc)
                .key(key)
                .offset(record.kafkaOffset())
                .partition(record.kafkaPartition());

        if(record.timestamp() != null){
          if (record.timestampType() == TimestampType.CREATE_TIME){
            message.createTime(record.timestamp());
          } else if (record.timestampType() == TimestampType.LOG_APPEND_TIME){
            message.logAppendTime(record.timestamp());
          }
        }

        messages.add(message);
      } catch (Exception e) {
        throw new ConnectException("Invalid JSON encountered in stream ", e);
      }
    }

    sendDocs(topic, messages);
  }

  private boolean isRetriableHttpCode(int code) {
    return code == 429 || code >= 500;
  }

  private boolean isSuccessHttpCode(int code) {
    return code == 200;
  }

  private void sendDocs(String topic, List<KafkaMessage> messages) {
    Preconditions.checkArgument(!messages.isEmpty());
    if (Thread.interrupted()) {
      // Exit early in case another thread failed and the whole batch of requests from
      // RocksetSinkTask::put() are going to be cancelled
      throw new ConnectException("Interrupted during call to RocksetRequestWrapper::addDocs");
    }
    log.debug("Sending batch of {} messages for topic: {} to Rockset", messages.size(), topic);

    KafkaDocumentsRequest documentsRequest =
        new KafkaDocumentsRequest().kafkaMessages(messages).topic(topic);

    try {
      RequestBody requestBody =
          RequestBody.create(JSON, mapper.writeValueAsString(documentsRequest));
      Request request =
          new Request.Builder()
              .url(this.apiServer + KAFKA_ENDPOINT)
              .addHeader("Authorization", "Basic " + integrationKeyEncoded)
              .post(requestBody)
              .build();

      try (Response response = client.newCall(request).execute()) {
        if (isSuccessHttpCode(response.code())) {
          // Nothing to do, write succeeded
          return;
        }
        if (isRetriableHttpCode(response.code())) {
          throw new RetriableException(
              String.format(
                  "Received retriable http status code  %d, message: %s. Can Retry.",
                  response.code(), response.message()));
        }

        // Anything that is not retriable and is not a success is a permanent error
        throw new ConnectException(
            String.format(
                "Unable to write document" + " in Rockset, cause: %s", response.message()));
      }
    } catch (SocketTimeoutException ste) {
      throw new RetriableException("Encountered socket timeout exception. Can Retry.", ste);
    } catch (RetriableException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }
}
