package com.lucidworks.storm.solr;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.lucidworks.storm.StreamingApp;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Slice;

public class ShardGrouping implements CustomStreamGrouping, Serializable {

  private transient List<Integer> targetTasks;
  private transient CloudSolrClient cloudSolrClient;
  private transient DocCollection docCollection;
  private transient Map<String, Integer> shardIndexCache;

  protected Map stormConf;
  protected String collection;
  protected Integer numShards;
  protected UniformIntegerDistribution random;
  protected int tasksPerShard;

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
    int numTasks = targetTasks.size();
    int numShards = initShardInfo(); // setup for doing shard to task mapping 
    if (numTasks % numShards != 0)
      throw new IllegalArgumentException("Number of tasks ("+numTasks+") should be a multiple of the number of shards ("+numShards+")!");

    this.tasksPerShard = numTasks/numShards;
    this.random = new UniformIntegerDistribution(0, tasksPerShard-1);
  }

  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    if (values == null || values.size() < 1)
      return Collections.singletonList(targetTasks.get(0));

    String docId = (String) values.get(0);
    if (docId == null)
      return Collections.singletonList(targetTasks.get(0));

    Slice slice = docCollection.getRouter().getTargetSlice(docId, null, null, null, docCollection);

    // map this doc into one of the tasks for that shard
    int shardIndex = shardIndexCache.get(slice.getName());
    int selectedTask = (tasksPerShard > 1) ? shardIndex + (random.sample() * tasksPerShard) : shardIndex;
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
    for (Slice next : shards) shardIndexCache.put(next.getName(), s++);
    return shards.size();
  }
}
