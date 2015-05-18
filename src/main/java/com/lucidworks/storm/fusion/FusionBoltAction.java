package com.lucidworks.storm.fusion;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.lucidworks.storm.solr.DefaultJsonContentStreamMapper;
import com.lucidworks.storm.solr.JsonContentStreamMapper;
import com.lucidworks.storm.solr.SolrUpdateRequestStrategy;
import com.lucidworks.storm.spring.SpringBolt;
import com.lucidworks.storm.spring.StreamingDataAction;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.common.util.ContentStream;

import org.apache.solr.client.solrj.impl.NoOpResponseParser;

public class FusionBoltAction implements StreamingDataAction {

  private static final Logger log = Logger.getLogger(FusionBoltAction.class);

  // for basic auth to the pipeline service
  private final class PreEmptiveBasicAuthenticator implements HttpRequestInterceptor {
    private final UsernamePasswordCredentials credentials;

    public PreEmptiveBasicAuthenticator(String user, String pass) {
      this.credentials = new UsernamePasswordCredentials(user, pass);
    }

    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      request.addHeader((new BasicScheme()).authenticate(credentials, request, context));
    }
  }

  protected LBHttpSolrClient lbHttpSolrClient;
  protected SolrUpdateRequestStrategy updateRequestStrategy;
  protected JsonContentStreamMapper jsonContentStreamMapper;
  protected String updatePath;
  protected String fusionRealm;

  public FusionBoltAction(String endpoints, String fusionUser, String fusionPass) throws MalformedURLException {
    this(endpoints, fusionUser, fusionPass, null);
  }

  public FusionBoltAction(String endpoints, String fusionUser, String fusionPass, String fusionRealm) throws MalformedURLException {
    lbHttpSolrClient = new LBHttpSolrClient(endpoints.split(","));
    this.fusionRealm = fusionRealm;
    initAuthentication(fusionUser, fusionPass, fusionRealm);
  }

  protected void initAuthentication(String fusionUser, String fusionPass, String fusionRealm) {
    if (fusionUser != null) {
      HttpClient httpClient = lbHttpSolrClient.getHttpClient();
      if (!(httpClient instanceof DefaultHttpClient))
        throw new IllegalStateException("Expected DefaultHttpClient but got "+
          (httpClient.getClass().getName())+" instead!");
      DefaultHttpClient dhc = (DefaultHttpClient)httpClient;
      dhc.addRequestInterceptor(new PreEmptiveBasicAuthenticator(fusionUser, fusionPass));
    }
  }

  public SpringBolt.ExecuteResult execute(Tuple input, OutputCollector collector) {
    String docId = input.getString(0);
    Map<String,Object> values = (Map<String,Object>)input.getValue(1);
    ContentStreamUpdateRequest req = buildUpdateRequest(docId, values);
    req.setResponseParser(new NoOpResponseParser());
    if (fusionRealm != null)
      req.setParam("realm", fusionRealm);
    updateRequestStrategy.sendUpdateRequest(lbHttpSolrClient, updatePath, req);
    return SpringBolt.ExecuteResult.ACK;
  }

  public SolrUpdateRequestStrategy getUpdateRequestStrategy() {
    return updateRequestStrategy;
  }

  public void setUpdateRequestStrategy(SolrUpdateRequestStrategy updateRequestStrategy) {
    this.updateRequestStrategy = updateRequestStrategy;
  }

  public String getUpdatePath() {
    return updatePath;
  }

  public void setUpdatePath(String updatePath) {
    this.updatePath = updatePath;
  }

  public JsonContentStreamMapper getJsonContentStreamMapper() {
    return jsonContentStreamMapper;
  }

  public void setJsonContentStreamMapper(JsonContentStreamMapper jsonContentStreamMapper) {
    this.jsonContentStreamMapper = jsonContentStreamMapper;
  }

  protected ContentStreamUpdateRequest buildUpdateRequest(String docId, Map<String,Object> values) {

    Map<String,Object> json = new HashMap<String,Object>(10);
    json.put("id", docId);
    List fieldList = new ArrayList();
    for (String field : values.keySet())
      fieldList.add(buildField(field, values.get(field)));
    json.put("fields", fieldList);


    if (jsonContentStreamMapper == null)
      jsonContentStreamMapper = new DefaultJsonContentStreamMapper();

    ContentStream contentStream = null;
    try {
      contentStream = jsonContentStreamMapper.toContentStream(docId, json);
    } catch (Exception exc) {
      if (exc instanceof RuntimeException) {
        throw (RuntimeException) exc;
      } else {
        throw new RuntimeException(exc);
      }
    }

    ContentStreamUpdateRequest req = new ContentStreamUpdateRequest(updatePath);
    req.addContentStream(contentStream);
    return req;
  }

  protected Map<String,Object> buildField(String name, Object value) {
    Map<String,Object> tsFld = new HashMap<String, Object>();
    tsFld.put("name", name);
    if (value != null) {
      tsFld.put("value", value);
    } else {
      tsFld.put("value", "");
    }
    return tsFld;
  }

}
