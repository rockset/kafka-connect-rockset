package rockset;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;

import java.util.Map;


public class RocksetConnectorConfig extends AbstractConfig {
  public static final String ROCKSET_APISERVER_URL = "rockset.apiserver.url";
  public static final String ROCKSET_APIKEY = "rockset.apikey";
  public static final String ROCKSET_COLLECTION = "rockset.collection";

  public RocksetConnectorConfig(ConfigDef config, Map<String, String> originals) {
    super(config, originals, true);
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
            ConfigKeyBuilder.of(ROCKSET_COLLECTION, Type.STRING)
                .documentation("Rockset collection that incoming documents will be written to.")
                .importance(Importance.HIGH)
                .build()
        );
  }

  public String getRocksetApiServerUrl() {
    return this.getString(ROCKSET_APISERVER_URL);
  }

  public String getRocksetApikey() {
    return this.getString(ROCKSET_APIKEY);
  }

  public String getRocksetCollection() {
    return this.getString(ROCKSET_COLLECTION);
  }
}
