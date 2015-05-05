package com.lucidworks.storm.solr;

import java.io.Serializable;
import java.util.*;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.lucidworks.storm.StreamingApp;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.*;

public class ShardGrouping implements CustomStreamGrouping, Serializable {

  private transient List<Integer> targetTasks;
  private transient int numTasks;
  private transient CloudSolrClient cloudSolrClient;
  private transient DocCollection docCollection;
  private transient DocRouter docRouter;
  private transient Map<String, Integer> shardIndexCache;

  protected Map stormConf;
  protected String collection;
  protected Integer numShards;

  public ShardGrouping(Map stormConf, String collection) {
    this.stormConf = stormConf;
    this.collection = collection;
  }

  public void setCloudSolrClient(CloudSolrClient client) {
    cloudSolrClient = client;
  }

  public int getNumShards() {
    if (numShards == null)
      numShards = new Integer(initShardInfo());

    return numShards.intValue();
  }

  public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
    this.targetTasks = targetTasks;
    this.numTasks = targetTasks.size();
    initShardInfo(); // setup for doing shard to task mapping
  }

  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    if (values == null || values.size() < 1)
      return Collections.singletonList(targetTasks.get(0));

    String docId = (String) values.get(0);
    if (docId == null)
      return Collections.singletonList(targetTasks.get(0));

    Slice slice = docCollection.getRouter().getTargetSlice(docId, null, null, null, docCollection);
    int shardIndex = shardIndexCache.get(slice.getName());
    int selectedTask = shardIndex % numTasks;
    return Collections.singletonList(targetTasks.get(selectedTask));
  }

  protected int initShardInfo() {
    if (cloudSolrClient == null) {
      // lookup the Solr client from the Spring context for this topology
      cloudSolrClient = (CloudSolrClient) StreamingApp.spring(stormConf).getBean("cloudSolrClient");
      cloudSolrClient.connect();
    }

    this.docCollection = cloudSolrClient.getZkStateReader().getClusterState().getCollection(collection);

    // do basic checks once
    DocRouter docRouter = docCollection.getRouter();
    if (docRouter instanceof ImplicitDocRouter)
      throw new IllegalStateException("Implicit document routing not supported by this Partitioner!");

    Collection<Slice> shards = docCollection.getSlices();
    if (shards == null || shards.size() == 0)
      throw new IllegalStateException("Collection '" + collection + "' does not have any shards!");

    shardIndexCache = new HashMap<String, Integer>(20);
    int s = 0;
    for (Slice next : shards) {
      shardIndexCache.put(next.getName(), s);
      ++s;
    }

    return shards.size();
  }
}
