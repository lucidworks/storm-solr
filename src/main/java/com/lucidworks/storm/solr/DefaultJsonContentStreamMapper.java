package com.lucidworks.storm.solr;

import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultJsonContentStreamMapper implements JsonContentStreamMapper {

  public static final String JSON_CONTENT_TYPE = "application/json";

  protected ObjectMapper mapper = new ObjectMapper();

  public ContentStream toContentStream(String docId, Object docObj) throws Exception {
    String jsonString = (docObj instanceof String) ? (String)docObj : mapper.writeValueAsString(docObj);
    ContentStreamBase.StringStream ss = new ContentStreamBase.StringStream(jsonString);
    ss.setContentType(JSON_CONTENT_TYPE);
    return ss;
  }
}
