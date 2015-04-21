package com.lucidworks.storm.solr;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

public interface SolrInputDocumentMapper {
  UpdateRequest createUpdateRequest(String collection);
  SolrInputDocument toInputDoc(String docId, Object obj);
}
