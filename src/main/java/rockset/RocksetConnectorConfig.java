package rockset;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksetConnectorConfig extends AbstractConfig {
  private static Logger log = LoggerFactory.getLogger(RocksetConnectorConfig.class);
  public static final String FORMAT = "format";
  public static final String ROCKSET_APISERVER_URL = "rockset.apiserver.url";
  public static final String ROCKSET_APIKEY = "rockset.apikey";
  public static final String ROCKSET_INTEGRATION_KEY = "rockset.integration.key";
  public static final String ROCKSET_COLLECTION = "rockset.collection";
  public static final String ROCKSET_WORKSPACE = "rockset.workspace";
  public static final String ROCKSET_TASK_THREADS = "rockset.task.threads";
  public static final String ROCKSET_BATCH_SIZE = "rockset.batch.size";

  private RocksetConnectorConfig(ConfigDef config, Map<String, String> originals) {
    super(config, originals, true);
    log.info(
        "Building Rockset connector config. Apiserver: {}" + "Number of Threads: {}, Format: {}",
        getRocksetApiServerUrl(),
        getRocksetTaskThreads(),
        getFormat());
  }

  public RocksetConnectorConfig(Map<String, String> parsedConfig) {
    this(config(), parsedConfig);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(ROCKSET_APISERVER_URL, Type.STRING)
                .documentation("Rockset API Server URL")
                .importance(Importance.HIGH)
                .validator(RocksetConnectorConfig::validateApiServer)
                .defaultValue("https://api.rs2.usw2.rockset.com")
                .build())
        .define(
            ConfigKeyBuilder.of(ROCKSET_INTEGRATION_KEY, Type.STRING)
                .documentation("Rockset Integration Key")
                .importance(Importance.HIGH)
                .validator(RocksetConnectorConfig::validateIntegrationKey)
                .defaultValue(null)
                .build())
        .define(
            ConfigKeyBuilder.of(ROCKSET_TASK_THREADS, Type.INT)
                .documentation("Number of threads that each task will use to write to Rockset")
                .importance(Importance.MEDIUM)
                .defaultValue(5)
                .build())
        .define(
            ConfigKeyBuilder.of(ROCKSET_BATCH_SIZE, Type.INT)
                .documentation("Number of documents batched before a write to Rockset")
                .importance(Importance.MEDIUM)
                .defaultValue(1000)
                .build())
        .define(
            ConfigKeyBuilder.of(FORMAT, Type.STRING)
                .documentation("Format of the data stream.")
                .importance(Importance.HIGH)
                .validator(RocksetConnectorConfig::validateFormat)
                .defaultValue("json")
                .build())
        .define(
            ConfigKeyBuilder.of(ROCKSET_APIKEY, Type.STRING)
                .documentation("(Deprecated) Rockset API Key")
                .importance(Importance.HIGH)
                .defaultValue(null)
                .build())
        .define(
            ConfigKeyBuilder.of(ROCKSET_COLLECTION, Type.STRING)
                .documentation(
                    "(Deprecated) Rockset collection that incoming documents will be written to.")
                .importance(Importance.HIGH)
                .defaultValue(null)
                .build())
        .define(
            ConfigKeyBuilder.of(ROCKSET_WORKSPACE, Type.STRING)
                .documentation(
                    "(Deprecated) Rockset workspace that incoming documents will be written to.")
                .importance(Importance.HIGH)
                .defaultValue("commons")
                .build());
  }

  private static void validateApiServer(String key, Object value) {
    checkNotNull(key);
    checkArgument(key.equals(ROCKSET_APISERVER_URL));

    String apiserver = (String) value;
    checkConfig(
        apiserver != null && apiserver.endsWith("rockset.com"),
        invalidConfigMessage(ROCKSET_APISERVER_URL, apiserver));
  }

  private static void validateIntegrationKey(String key, Object value) {
    checkNotNull(key);
    checkArgument(key.equals(ROCKSET_INTEGRATION_KEY));

    String integrationKey = (String) value;
    checkConfig(
        integrationKey == null || integrationKey.startsWith("kafka"),
        invalidConfigMessage(ROCKSET_INTEGRATION_KEY, integrationKey));
  }

  private static void validateFormat(String key, Object value) {
    checkNotNull(key);
    checkArgument(key.equals(FORMAT));

    String format = (String) value;
    checkConfig(
        format.equalsIgnoreCase("json") || format.equalsIgnoreCase("avro"),
        "%s. Supported formats are: ['JSON', 'AVRO']",
        invalidConfigMessage(FORMAT, format));
  }

  private static void checkConfig(boolean condition, String msgFormat, Object... args) {
    if (!condition) {
      throw new ConfigException(String.format(msgFormat, args));
    }
  }

  private static String invalidConfigMessage(String key, String value) {
    return String.format("Invalid value \"%s\" for configuration `%s`", value, key);
  }

  public String getRocksetApiServerUrl() {
    return this.getString(ROCKSET_APISERVER_URL);
  }

  public String getRocksetIntegrationKey() {
    return this.getString(ROCKSET_INTEGRATION_KEY);
  }

  public int getRocksetTaskThreads() {
    return this.getInt(ROCKSET_TASK_THREADS);
  }

  public int getRocksetBatchSize() {
    return this.getInt(ROCKSET_BATCH_SIZE);
  }

  public String getFormat() {
    return this.getString(FORMAT);
  }
}
