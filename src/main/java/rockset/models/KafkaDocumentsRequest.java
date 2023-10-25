package rockset.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class KafkaDocumentsRequest {

  /*
   * Fields
   */

  @SerializedName("kafka_messages")
  public List<KafkaMessage> kafkaMessages = new ArrayList<>();

  @SerializedName("topic")
  public String topic;

  /*
   * Getters
   */

  @JsonProperty("kafka_messages")
  @ApiModelProperty(required = true, value = "Array of JSON documents")
  public List<KafkaMessage> getKafkaMessages() {
    return this.kafkaMessages;
  }

  @JsonProperty("topic")
  @ApiModelProperty(required = true, value = "Kafka topic")
  public String getTopic() {
    return this.topic;
  }

  /*
   * Setters
   */

  public void setKafkaMessages(List<KafkaMessage> kafkaMessages) {
    this.kafkaMessages = kafkaMessages;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  /*
   * Builders
   */

  public KafkaDocumentsRequest addKafkaMessage(KafkaMessage kafkaMessage) {
    this.kafkaMessages.add(kafkaMessage);
    return this;
  }

  public KafkaDocumentsRequest kafkaMessages(List<KafkaMessage> kafkaMessages) {
    this.kafkaMessages = kafkaMessages;
    return this;
  }

  public KafkaDocumentsRequest topic(String topic) {
    this.topic = topic;
    return this;
  }

  /*
   * Utilities
   */

  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("class KafkaDocumentsRequest {\n");

    sb.append("    topic: ").append(this.toIndentedString(this.topic)).append("\n");
    sb.append("    kafkaMessages: ").append(this.toIndentedString(this.kafkaMessages)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.kafkaMessages, this.topic);
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    final KafkaDocumentsRequest kafkaDocumentsRequest = (KafkaDocumentsRequest) o;
    return this.getTopic().equals(kafkaDocumentsRequest.getTopic())
        && Objects.equals(this.kafkaMessages, kafkaDocumentsRequest.kafkaMessages);
  }
}
