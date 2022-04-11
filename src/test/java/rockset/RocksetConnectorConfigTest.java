package rockset;

import org.apache.kafka.common.config.ConfigException;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RocksetConnectorConfigTest {
  private static final Logger log = LoggerFactory.getLogger(RocksetConnectorConfigTest.class);

  @Test
  public void testBadFormat() {
    Map<String, String> badFormatSettings = new HashMap<>();
    badFormatSettings.put("rockset.integration.key", "kafka://5");
    badFormatSettings.put("format", "abc");

    assertThrows(ConfigException.class, () ->  new RocksetConnectorConfig(badFormatSettings));
  }

  @Test
  public void testBadUrl() {
    Map<String, String> badUrlSettings = new HashMap<>();
    badUrlSettings.put("rockset.integration.key", "kafka://5");
    badUrlSettings.put("rockset.apiserver.url", "abc.com");

    assertThrows(ConfigException.class, () -> new RocksetConnectorConfig(badUrlSettings));
  }

  @Test
  public void testBadTaskThreads() {
    Map<String, String> badTaskThreadSettings = new HashMap<>();
    badTaskThreadSettings.put("rockset.integration.key", "kafka://5");
    badTaskThreadSettings.put("rockset.task.threads", "abc");

    assertThrows(ConfigException.class, () -> new RocksetConnectorConfig(badTaskThreadSettings));
  }

  @Test
  public void testBadBatchConfig() {
    Map<String, String> badBatchConfig = new HashMap<>();
    badBatchConfig.put("rockset.integration.key", "kafka://5");
    badBatchConfig.put("rockset.batch.size", "xyz");

    assertThrows(ConfigException.class, () -> new RocksetConnectorConfig(badBatchConfig));
  }

  @Test
  public void testGoodConfig() {
    Map<String, String> goodSettings = new HashMap<>();
    goodSettings.put("rockset.integration.key", "kafka://5");
    goodSettings.put("rockset.batch.size", "10");
    goodSettings.put("format", "jSon");

    assertDoesNotThrow(() -> new RocksetConnectorConfig(goodSettings));
  }
}
