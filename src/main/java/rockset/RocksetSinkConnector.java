package rockset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksetSinkConnector extends SinkConnector {
  private static Logger log = LoggerFactory.getLogger(RocksetSinkConnector.class);
  public static final String VERSION = "1.0";
  private Map<String, String> configProperties;

  @Override
  public void start(Map<String, String> settings) {
    log.info("Starting RocksetSinkConnector");
    configProperties = settings;
    new RocksetConnectorConfig(settings);
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>();
      config.putAll(configProperties);
      configs.add(config);
    }
    return configs;
  }

  @Override
  public void stop() {
    // not required.
  }

  @Override
  public ConfigDef config() {
    return RocksetConnectorConfig.config();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return RocksetSinkTask.class;
  }

  @Override
  public String version() {
    return VERSION;
  }
}
