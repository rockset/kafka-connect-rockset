package rockset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rockset.client.RocksetClient;
import com.rockset.client.model.AddDocumentsRequest;
import com.rockset.client.model.AddDocumentsResponse;
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

  public void addDoc(String workspace, String collection, String json, SinkRecord sr) {
    LinkedList<Object> list = new LinkedList<>();
    ObjectMapper mapper = new ObjectMapper();

    String srId = createId(sr);
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
      throw new ConnectException(String.format("Unable to write document " +
          "to collection %s, workspace %s in Rockset, cause: %s",
          collection, workspace, e.getMessage()));
    }
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
