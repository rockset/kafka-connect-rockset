package rockset.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;

public class KafkaMessage {

  /*
   * Fields
   */

  @SerializedName("document") public Object document;
  @SerializedName("partition") public int partition;
  @SerializedName("offset") public long offset;
  @SerializedName("key") public Object key;
  @SerializedName("timestamp") public long timestamp;

  /*
   * Getters
   */

  @JsonProperty("document")
  @ApiModelProperty(
      required = true,
      value = "JSON documents")
  public Object getDocument() {
    return this.document;
  }

  @JsonProperty("partition")
  @ApiModelProperty(
      required = true,
      value = "Kafka partition")
  public int getPartition() {
    return this.partition;
  }

  @JsonProperty("offset")
  @ApiModelProperty(
      required = true,
      value = "Kafka offset")
  public long getOffset() {
    return this.offset;
  }

  @JsonProperty("key")
  @ApiModelProperty(
      required = true,
      value = "kafka key")
  public Object getKey() {
    return this.key;
  }

  @JsonProperty("timestamp")
  @ApiModelProperty(
      required = true,
      value = "kafka record timestamp")
  public long getTimestamp() {
    return this.timestamp;
  }

  /*
   * Setters
   */

  public void setDocument(Object document) {
    this.document = document;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public void setKey(Object key) {
    this.key = key;
  }

  /*
   * Builders
   */

  public KafkaMessage document(Object document) {
    this.document = document;
    return this;
  }

  public KafkaMessage partition(int partition) {
    this.partition = partition;
    return this;
  }

  public KafkaMessage offset(long offset) {
    this.offset = offset;
    return this;
  }

  public KafkaMessage key(Object key) {
    this.key = key;
    return this;
  }

  public KafkaMessage timestamp(long timestamp) {
    this.timestamp = timestamp;
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
    sb.append("class KafkaMessage {\n");

    sb.append("    partition: ").append(this.toIndentedString(this.partition)).append("\n");
    sb.append("    offset: ").append(this.toIndentedString(this.offset)).append("\n");
    sb.append("    document: ").append(this.toIndentedString(this.document)).append("\n");
    sb.append("    key: ").append(this.toIndentedString(this.key)).append("\n");
    sb.append("    timestamp: ").append(this.toIndentedString(this.timestamp)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.document, this.partition, this.offset, this.key, this.timestamp);
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    final KafkaMessage kafkaMessage = (KafkaMessage) o;
    return this.getPartition() == kafkaMessage.getPartition()
        && this.getOffset() == kafkaMessage.getOffset()
        && Objects.equals(this.document, kafkaMessage.document)
        && Objects.equals(this.key, kafkaMessage.key);
  }
}
