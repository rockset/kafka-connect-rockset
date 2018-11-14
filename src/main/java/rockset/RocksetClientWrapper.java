package rockset;

import com.google.gson.Gson;
import com.rockset.client.RocksetClient;
import com.rockset.client.model.AddDocumentsRequest;
import com.rockset.client.model.AddDocumentsResponse;
import java.util.LinkedList;
import java.util.Map;
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

  public void addDoc(String collection, String json, Boolean val) {
    LinkedList<Object> list = new LinkedList<>();
    Map map = new Gson().fromJson(json, Map.class);
    list.add(map);
    AddDocumentsRequest documentsRequest =
        new AddDocumentsRequest().data(list);

    AddDocumentsResponse res = null;
    try {
      res = client.addDocuments(collection, documentsRequest);
    } catch (Exception e) {
      log.error("%s - %s", e.getMessage(), res != null ? res.toString() : "");
    }
  }

}
