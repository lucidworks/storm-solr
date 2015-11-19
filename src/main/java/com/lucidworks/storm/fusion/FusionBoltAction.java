package com.lucidworks.storm.fusion;

import java.net.MalformedURLException;
import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.lucidworks.storm.spring.SpringBolt;
import com.lucidworks.storm.spring.StreamingDataAction;
import org.apache.log4j.Logger;

public class FusionBoltAction implements StreamingDataAction {

  private static final Logger log = Logger.getLogger(FusionBoltAction.class);

  protected FusionPipelineClient fusionPipelineClient;
  protected String updatePath;

  public FusionBoltAction(String endpoints, String fusionUser, String fusionPass, String fusionRealm, String updatePath) throws MalformedURLException {

    this.updatePath = updatePath;

    StringBuilder sb = new StringBuilder();
    String[] ep = endpoints.split(",");
    for (int e=0; e < ep.length; e++) {
      if (e > 0) sb.append(",");
      String base = ep[e].trim();
      if (base.endsWith("/"))
        base = base.substring(0,base.length()-1);
      sb.append(base).append(updatePath);
    }
    String endpointsWithPath = sb.toString();
    fusionPipelineClient = new FusionPipelineClient(endpointsWithPath, fusionUser, fusionPass, fusionRealm);
  }

  public SpringBolt.ExecuteResult execute(Tuple input, OutputCollector collector) {
    String docId = input.getString(0);
    Map<String,Object> values = (Map<String,Object>)input.getValue(1);

    Map<String,Object> json = new HashMap<String,Object>(10);
    json.put("id", docId);
    List fieldList = new ArrayList();
    for (String field : values.keySet())
      fieldList.add(buildField(field, values.get(field)));
    json.put("fields", fieldList);

    try {
      fusionPipelineClient.postBatchToPipeline(Collections.singletonList(json));
    } catch (Exception e) {
      log.error("Failed to send doc "+docId+" to Fusion due to: "+e);
      throw new RuntimeException(e);
    }

    return SpringBolt.ExecuteResult.ACK;
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
