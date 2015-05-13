package com.lucidworks.storm.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.util.NamedList;

/**
 * Implements the error handling strategy around sending update requests to Solr.
 */
public interface SolrUpdateRequestStrategy {
  NamedList<Object> sendUpdateRequest(SolrClient solrClient, String collection, SolrRequest req);
}
