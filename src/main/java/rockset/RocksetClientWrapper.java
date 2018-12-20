package rockset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rockset.client.RocksetClient;
import com.rockset.client.model.AddDocumentsRequest;
import com.rockset.client.model.AddDocumentsResponse;
import java.util.LinkedList;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksetClientWrapper {
  private static Logger log = LoggerFactory.getLogger(RocksetClientWrapper.class);
  private RocksetClient client;

  public RocksetClientWrapper(String apiKey, String apiServer) {
    if (this.client == null) {
      this.client = new RocksetClient(apiKey, apiServer);
    }
  }

  public void addDoc(String collection, String json) {
    LinkedList<Object> list = new LinkedList<>();
    AddDocumentsResponse res = null;
    ObjectMapper mapper = new ObjectMapper();

    try {
      list.add(mapper.readValue(json, new TypeReference<Map<String, Object>>(){}));
      AddDocumentsRequest documentsRequest = new AddDocumentsRequest().data(list);
      res = client.addDocuments(collection, documentsRequest);
    } catch (Exception e) {
      log.error("{}: {}", e.getMessage(), res != null ? res.toString() : "no response");
      return;
    }
    log.info("Added doc: {}", StringUtils.abbreviate(json, 10));
  }
}
