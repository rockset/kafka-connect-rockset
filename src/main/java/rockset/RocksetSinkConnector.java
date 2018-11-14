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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;

@Description("Rockset Sink Connector for Kafka Connect")
@DocumentationImportant("")
@DocumentationTip("")
@DocumentationNote("")
@Title("Rockset Sink Connector for Kafka Connect")
public class RocksetSinkConnector extends SinkConnector {
  private static Logger log = LoggerFactory.getLogger(RocksetSinkConnector.class);
  private Map<String, String> configProperties;

  @Override
  public void start(Map<String, String> settings) {
    try {
      configProperties = settings;
      new RocksetConnectorConfig(settings);
    } catch (Exception e) {
      throw new ConnectException("Bad configuration for RocksetConnectorConfig", e);
    }
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
    // do nothing.
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
    return VersionUtil.version(this.getClass());
  }
}
