package com.lucidworks.storm.solr;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static com.lucidworks.storm.spring.SpringBolt.ExecuteResult;

/**
 * A simple Spring-managed POJO for sending JSON documents processed by a Storm topology
 * to SolrCloud using the built-in /update/json/docs endpoint in Solr.
 */
public class SolrJsonBoltAction extends SolrBoltAction {

  public static Logger log = Logger.getLogger(SolrJsonBoltAction.class);

  @Autowired
  public JsonContentStreamMapper jsonContentStreamMapper;

  protected String updatePath = "/update/json/docs";
  protected String split;
  protected List<String> fieldMappings;

  @Autowired
  public SolrJsonBoltAction(CloudSolrClient cloudSolrClient) {
    super(cloudSolrClient);
  }

  @Override
  protected ExecuteResult processInputDoc(String docId, Object docObj) {
    if (jsonContentStreamMapper == null)
      jsonContentStreamMapper = new DefaultJsonContentStreamMapper();

    ContentStream contentStream = null;
    try {
      contentStream = jsonContentStreamMapper.toContentStream(docId, docObj);
    } catch (Exception exc) {
      if (exc instanceof RuntimeException) {
        throw (RuntimeException) exc;
      } else {
        throw new RuntimeException(exc);
      }
    }

    ContentStreamUpdateRequest req = new ContentStreamUpdateRequest(updatePath);
    if (split != null)
      req.setParam("split", split);

    if (fieldMappings != null && !fieldMappings.isEmpty()) {
      ModifiableSolrParams params = req.getParams();
      if (params == null) {
        params = new ModifiableSolrParams();
        req.setParams(params);
      }
      for (String mapping : fieldMappings) {
        params.add("f", mapping);
      }
    } else {
      req.setParam("f", "$FQN:/**");
    }
    req.addContentStream(contentStream);

    // ugh - hacky, but this class needs to be re-worked anyway
    String collection = ((DefaultDocumentAssignmentStrategy)documentAssignmentStrategy).getCollection();
    updateRequestStrategy.sendUpdateRequest(cloudSolrClient, collection, req);

    if (indexedCounter != null)
      indexedCounter.inc();

    return ExecuteResult.ACK;
  }

  public String getUpdatePath() {
    return updatePath;
  }

  public void setUpdatePath(String updatePath) {
    this.updatePath = updatePath;
  }

  public String getSplit() {
    return split;
  }

  public void setSplit(String split) {
    this.split = split;
  }

  public List<String> getFieldMappings() {
    return fieldMappings;
  }

  public void setFieldMappings(List<String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }
}
