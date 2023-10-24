package rockset;

public class RocksetClientFactory {
  public static RocksetWrapper getRocksetWrapper(RocksetConnectorConfig config) {
    if (hasRocksetIntegrationKey(config)) {
      return getRocksetRequest(config);
    }

    return getRocksetClient(config);
  }

  private static boolean hasRocksetIntegrationKey(RocksetConnectorConfig config) {
    String integrationKey = config.getRocksetIntegrationKey();
    return integrationKey != null && !integrationKey.isEmpty();
  }

  private static RocksetWrapper getRocksetRequest(RocksetConnectorConfig config) {
    return new RocksetRequestWrapper(config);
  }

  private static RocksetWrapper getRocksetClient(RocksetConnectorConfig config) {
    return new RocksetClientWrapper(config);
  }
}
