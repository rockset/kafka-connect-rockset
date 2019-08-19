package rockset;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class RocksetConnectorConfig extends AbstractConfig {
  private static Logger log = LoggerFactory.getLogger(RocksetConnectorConfig.class);
  public static final String FORMAT = "format";
  public static final String ROCKSET_APISERVER_URL = "rockset.apiserver.url";
  public static final String ROCKSET_APIKEY = "rockset.apikey";
  public static final String ROCKSET_INTEGRATION_KEY = "rockset.integration.key";
  public static final String ROCKSET_COLLECTION = "rockset.collection";
  public static final String ROCKSET_WORKSPACE = "rockset.workspace";
  public static final String ROCKSET_TASK_THREADS = "rockset.task.threads";

  public RocksetConnectorConfig(ConfigDef config, Map<String, String> originals) {
    super(config, originals, true);
    log.info("Building Rockset connector config. Collection: {}, Workspace: {}, " +
        "Number of Threads: {}, Format: {}", getRocksetCollection(), getRocksetWorkspace(),
        getRocksetTaskThreads(), getFormat());
    checkConfig(originals);
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
                .defaultValue("https://api.rs2.usw2.rockset.com")
                .build()
        )

        .define(
            ConfigKeyBuilder.of(ROCKSET_APIKEY, Type.STRING)
                .documentation("Rockset API Key")
                .importance(Importance.HIGH)
                .build()
        )

        .define(
            ConfigKeyBuilder.of(ROCKSET_INTEGRATION_KEY, Type.STRING)
                .documentation("Rockset Integration Key")
                .importance(Importance.HIGH)
                .defaultValue("")
                .build()
        )

        .define(
            ConfigKeyBuilder.of(ROCKSET_TASK_THREADS, Type.INT)
                .documentation("Number of threads that each task will use to write to Rockset")
                .importance(Importance.MEDIUM)
                .defaultValue(5)
                .build()
        )

        .define(
            ConfigKeyBuilder.of(ROCKSET_COLLECTION, Type.STRING)
                .documentation("Rockset collection that incoming documents will be written to.")
                .importance(Importance.HIGH)
                .build()
        )

        .define(
            ConfigKeyBuilder.of(ROCKSET_WORKSPACE, Type.STRING)
                .documentation("Rockset workspace that incoming documents will be written to.")
                .importance(Importance.HIGH)
                .defaultValue("commons")
                .build()
        )

        .define(
            ConfigKeyBuilder.of(FORMAT, Type.STRING)
                .documentation("Format of the data stream.")
                .importance(Importance.HIGH)
                .defaultValue("json")
                .build()
        );
  }

  private void checkConfig(Map<String, String> config) throws ConnectException {
    if (config.containsKey(ROCKSET_APISERVER_URL) &&
        !(config.get(ROCKSET_APISERVER_URL).endsWith("rockset.com"))) {
      throw new ConnectException(String.format("Invalid url: %s",
          config.get(ROCKSET_APISERVER_URL)));
    }

    if (config.containsKey(ROCKSET_INTEGRATION_KEY) &&
        !(config.get(ROCKSET_INTEGRATION_KEY).startsWith("kafka"))) {
      throw new ConnectException(String.format("Invalid integration key: %s",
          config.get(ROCKSET_INTEGRATION_KEY)));
    }

    if (config.containsKey(FORMAT) &&
        !(config.get(FORMAT).equals("json") || config.get(FORMAT).equals("avro"))) {
      throw new ConnectException(String.format("Invalid format: %s, " +
          "supported formats are avro and json", config.get(FORMAT)));
    }
  }

  public String getRocksetApiServerUrl() {
    return this.getString(ROCKSET_APISERVER_URL);
  }

  public String getRocksetApikey() {
    return this.getString(ROCKSET_APIKEY);
  }

  public String getRocksetIntegrationKey() {
    return this.getString(ROCKSET_INTEGRATION_KEY);
  }

  public String getRocksetCollection() {
    return this.getString(ROCKSET_COLLECTION);
  }

  public String getRocksetWorkspace() {
    return this.getString(ROCKSET_WORKSPACE);
  }

  public int getRocksetTaskThreads() { return this.getInt(ROCKSET_TASK_THREADS); }

  public String getFormat() {
    return this.getString(FORMAT);
  }
}
