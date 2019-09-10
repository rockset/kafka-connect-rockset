package rockset;

public class RocksetClientFactory {
  public static RocksetWrapper getRocksetWrapper(RocksetConnectorConfig config) {
    if (!config.getRocksetIntegrationKey().isEmpty()) {
      return getRocksetRequest(config);
    }

    return getRocksetClient(config);
  }

  private static RocksetWrapper getRocksetRequest(RocksetConnectorConfig config) {
    return new RocksetRequestWrapper(config);
  }

  private static RocksetWrapper getRocksetClient(RocksetConnectorConfig config) {
    return new RocksetClientWrapper(config);
  }
}