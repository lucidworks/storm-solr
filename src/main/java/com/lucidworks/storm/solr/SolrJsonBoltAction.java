package com.lucidworks.storm.solr;

import com.codahale.metrics.Counter;
import com.ryantenney.metrics.annotation.Metric;
import org.apache.log4j.Logger;
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

  @Metric
  public Counter failedUpdates;

  protected String updatePath = "/update/json/docs";
  protected String split;
  protected List<String> fieldMappings;

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

    try {
      cloudSolrClient.request(req);
    } catch (Exception e) {

      Exception failedExc = null;
      if (shouldRetry(e)) {
        log.error("Send JSON to collection " + collection + " failed due to " + e + "; will retry ...");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        try {
          cloudSolrClient.request(req);
          failedExc = null;
        } catch (Exception innerExc) {
          failedExc = innerExc;
        }
      } else {
        failedExc = e;
      }

      if (failedExc != null) {

        if (failedUpdates != null)
          failedUpdates.inc();

        if (failedExc instanceof RuntimeException) {
          throw (RuntimeException) failedExc;
        } else {
          throw new RuntimeException(failedExc);
        }
      }

    }

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
