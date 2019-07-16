package rockset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rockset.client.ApiException;
import com.rockset.client.RocksetClient;
import com.rockset.client.model.AddDocumentsRequest;
import com.rockset.client.model.AddDocumentsResponse;
import com.rockset.client.model.ErrorModel;
import java.util.LinkedList;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
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

  // returns false on a Rockset internalerror exception to retry adding the doc,
  // returns true otherwise
  public boolean addDoc(String workspace, String collection, String json, SinkRecord sr) {
    LinkedList<Object> list = new LinkedList<>();
    ObjectMapper mapper = new ObjectMapper();

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

  private String createId(SinkRecord sr) {
    if (sr.key() != null) {
      if (sr.key() instanceof String) {
        return String.valueOf(sr.key());
      } else {
        // only supports string keys
        throw new ConnectException(String.format("Only keys of type String are supported, " +
            "key is of type %s", sr.key().getClass()));
      }
    } else {
      return sr.topic() + "+" + sr.kafkaPartition() + "+" + sr.kafkaOffset();
    }
  }
}
