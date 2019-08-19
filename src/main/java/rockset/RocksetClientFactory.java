package rockset;

public class RocksetClientFactory {
  public static RocksetWrapper getRocksetWrapper(RocksetConnectorConfig config) {
    if (!config.getRocksetIntegrationKey().isEmpty()) {
      return getRocksetRequest(config.getRocksetIntegrationKey());
    }

    return getRocksetClient(config.getRocksetApikey(), config.getRocksetApiServerUrl());
  }

  private static RocksetWrapper getRocksetRequest(String integrationKey) {
    return new RocksetRequestWrapper(integrationKey);
  }

  private static RocksetWrapper getRocksetClient(String apiKey, String apiServer) {
    return new RocksetClientWrapper(apiKey, apiServer);
  }
}