package rockset;

public class RocksetClientFactory {
  public static RocksetWrapper getRocksetWrapper(RocksetConnectorConfig config) {
    if (!config.getRocksetIntegrationKey().isEmpty()) {
      return getRocksetRequest(config.getRocksetIntegrationKey(), config.getRocksetApiServerUrl());
    }

    return getRocksetClient(config.getRocksetApikey(), config.getRocksetApiServerUrl());
  }

  private static RocksetWrapper getRocksetRequest(String integrationKey, String apiServer) {
    return new RocksetRequestWrapper(integrationKey, apiServer);
  }

  private static RocksetWrapper getRocksetClient(String apiKey, String apiServer) {
    return new RocksetClientWrapper(apiKey, apiServer);
  }
}