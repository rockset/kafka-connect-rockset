package rockset.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import io.swagger.annotations.ApiModelProperty;

public class KafkaMessage {

  /*
   * Fields
   */

  @SerializedName("document")
  public Object document;

  @SerializedName("partition")
  public int partition;

  @SerializedName("offset")
  public long offset;

  @SerializedName("key")
  public Object key;

  @SerializedName("create_time")
  public Long createTime;

  @SerializedName("log_append_time")
  public Long logAppendTime;

  /*
   * Getters
   */

  @JsonProperty("document")
  @ApiModelProperty(required = true, value = "JSON documents")
  public Object getDocument() {
    return this.document;
  }

  @JsonProperty("partition")
  @ApiModelProperty(required = true, value = "Kafka partition")
  public int getPartition() {
    return this.partition;
  }

  @JsonProperty("offset")
  @ApiModelProperty(required = true, value = "Kafka offset")
  public long getOffset() {
    return this.offset;
  }

  @JsonProperty("key")
  @ApiModelProperty(required = true, value = "kafka key")
  public Object getKey() {
    return this.key;
  }

  @JsonProperty("create_time")
  @ApiModelProperty(required = false, value = "Create time of the message")
  public Long getCreateTime() {
    return this.createTime;
  }

  @JsonProperty("log_append_time")
  @ApiModelProperty(required = false, value = "Log append time of the message")
  public Long getLogAppendTime() {
    return this.logAppendTime;
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


  public void setLogAppendTime(Long timestamp) {
    this.logAppendTime = timestamp;
  }

  public void setCreateTime(Long timestamp) {
    this.createTime = timestamp;
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

  public KafkaMessage logAppendTime(Long timestamp) {
    this.logAppendTime = timestamp;
    return this;
  }

  public KafkaMessage createTime(Long timestamp) {
    this.createTime = timestamp;
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KafkaMessage that = (KafkaMessage) o;
    return partition == that.partition && offset == that.offset && Objects.equal(document, that.document) && Objects.equal(key, that.key) && Objects.equal(createTime, that.createTime) && Objects.equal(logAppendTime, that.logAppendTime);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(document, partition, offset, key, createTime, logAppendTime);
  }

  @Override
  public String toString() {
    return "KafkaMessage{" +
            "document=" + document +
            ", partition=" + partition +
            ", offset=" + offset +
            ", key=" + key +
            ", createTime=" + createTime +
            ", logAppendTime=" + logAppendTime +
            '}';
  }
}
