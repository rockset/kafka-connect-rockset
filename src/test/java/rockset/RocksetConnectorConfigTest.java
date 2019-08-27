package rockset;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
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

    assertThrows(ConnectException.class, () -> {
      RocksetConnectorConfig rcc = new RocksetConnectorConfig(badFormatSettings);
    });
  }

  @Test
  public void testBadUrl() {
    Map<String, String> badUrlSettings = new HashMap<>();
    badUrlSettings.put("rockset.integration.key", "kafka://5");
    badUrlSettings.put("rockset.apiserver.url", "abc.com");

    assertThrows(ConnectException.class, () -> {
      RocksetConnectorConfig rcc = new RocksetConnectorConfig(badUrlSettings);
    });
  }

  @Test
  public void testBadTaskThreads() {
    Map<String, String> badTaskThreadSettings = new HashMap<>();
    badTaskThreadSettings.put("rockset.integration.key", "kafka://5");
    badTaskThreadSettings.put("rockset.task.threads", "abc");

    assertThrows(ConfigException.class, () -> {
      RocksetConnectorConfig rcc = new RocksetConnectorConfig(badTaskThreadSettings);
    });
  }

  @Test
  public void testGoodConfig() {
    Map<String, String> goodSettings = new HashMap<>();
    goodSettings.put("rockset.integration.key", "kafka://5");

    assertDoesNotThrow(() -> {
      RocksetConnectorConfig rcc = new RocksetConnectorConfig(goodSettings);
    });
  }
}
