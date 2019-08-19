package rockset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rockset.client.ApiException;
import com.rockset.client.RocksetClient;
import com.rockset.client.model.AddDocumentsRequest;
import com.rockset.client.model.ErrorModel;
import java.util.LinkedList;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksetClientWrapper implements RocksetWrapper {
  private static Logger log = LoggerFactory.getLogger(RocksetClientWrapper.class);
  private RocksetClient client;
  private ObjectMapper mapper;

  public RocksetClientWrapper(String apiKey, String apiServer) {
    if (this.client == null) {
      log.info("Creating new Rockset client");
      this.client = new RocksetClient(apiKey, apiServer);
    }

    this.mapper = new ObjectMapper();
  }

  // used for testing
  public RocksetClientWrapper(RocksetClient client) {
    this.client = client;
    this.mapper = new ObjectMapper();
  }

  // returns false on a Rockset internalerror exception to retry adding the doc,
  // returns true otherwise
  @Override
  public boolean addDoc(String workspace, String collection, String json, SinkRecord sr) {
    LinkedList<Object> list = new LinkedList<>();

    String srId = RocksetSinkUtils.createId(sr);
    try {
      Map<String, Object> doc = mapper.readValue(json, new TypeReference<Map<String, Object>>(){});
      doc.put("_id", srId);
      list.add(doc);
    } catch (Exception e) {
      throw new ConnectException("Invalid JSON encountered in stream");
    }

    try {
      AddDocumentsRequest documentsRequest = new AddDocumentsRequest().data(list);
      client.addDocuments(workspace, collection, documentsRequest);
    } catch (Exception e) {
      if (e instanceof ApiException &&
          ((ApiException) e).getErrorModel().getType() == ErrorModel.TypeEnum.INTERNALERROR) {
        // return false to retry
        return false;
      }
      throw new ConnectException(String.format("Unable to write document " +
          "to collection %s, workspace %s in Rockset, cause: %s",
          collection, workspace, e.getMessage()));
    }
    return true;
  }
}
