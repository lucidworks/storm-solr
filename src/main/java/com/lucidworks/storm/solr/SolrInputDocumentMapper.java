package com.lucidworks.storm.solr;

import org.apache.solr.common.SolrInputDocument;

public interface SolrInputDocumentMapper {
  SolrInputDocument toInputDoc(String docId, Object obj);
}
