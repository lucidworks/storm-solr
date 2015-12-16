package com.lucidworks.storm.solr;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class DefaultDocumentAssignmentStrategy implements DocumentAssignmentStrategy {

  protected String collection;

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getCollectionForDoc(CloudSolrClient cloudSolrClient, SolrInputDocument doc) throws Exception {
    return (collection != null) ? collection : cloudSolrClient.getDefaultCollection();
  }
}
