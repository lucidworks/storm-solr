package com.lucidworks.storm.solr;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * Defines an interface to a class that can determine which Solr collection a document should be assigned to.
 */
public interface DocumentAssignmentStrategy {
  String getCollectionForDoc(CloudSolrClient cloudSolrClient, SolrInputDocument doc) throws Exception;
}
