package com.lucidworks.storm.solr;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the ShardGrouping impl
 */
public class ShardGroupingTest extends TestSolrCloudClusterSupport {

  @Ignore
  @Test
  public void testBasicShardGroupingLogic() throws Exception {

    Map stormConf = new HashMap();

    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    String testCollection = "test1x1";
    int numShards = 1;
    int replicationFactor = 1;
    createCollection(testCollection, numShards, replicationFactor, confName, confDir);
    cloudSolrClient.setDefaultCollection(testCollection);

    // this test is pretty weak since the default collection only has 1 shard
    ShardGrouping grouping = new ShardGrouping(stormConf, testCollection);
    grouping.setCloudSolrClient(cloudSolrClient);

    List<Integer> targetTasks = new ArrayList<Integer>(1);
    targetTasks.add(5); // just some task ID is fine
    grouping.prepare(null, null, targetTasks);

    List<Object> values = new ArrayList<Object>(2);
    values.add("someDocId");
    values.add("someValue");

    List<Integer> list = grouping.chooseTasks(1, values);
    assertNotNull(list);
    assertTrue(list.size() == 1);
    assertTrue(5 == list.get(0));
  }

  @Test
  public void testMultipleShardGroupingLogic() throws Exception {

    Map stormConf = new HashMap();

    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    String testCollection = "test3x1";
    int numShards = 3;
    int replicationFactor = 1;
    createCollection(testCollection, numShards, replicationFactor, confName, confDir);
    cloudSolrClient.setDefaultCollection(testCollection);

    ShardGrouping grouping = new ShardGrouping(stormConf, testCollection);
    grouping.setCloudSolrClient(cloudSolrClient);

    int numTasks = 3 * numShards;
    List<Integer> targetTasks = new ArrayList<Integer>(numTasks);
    for (int i=0; i < numTasks; i++)
      targetTasks.add(i);
    grouping.prepare(null, null, targetTasks);

    DocCollection docCollection = cloudSolrClient.getZkStateReader().getClusterState().getCollection(testCollection);
    int numDocs = 1000;
    for (int d=0; d < numDocs; d++) {
      List<Object> values = new ArrayList<Object>(2);
      values.add("doc"+d);
      values.add("someValue");

      List<Integer> list = grouping.chooseTasks(1, values);
      assertNotNull(list);
      assertTrue(list.size() == 1);

      Slice slice = docCollection.getRouter().getTargetSlice((String)values.get(0), null, null, null, docCollection);
      int shardIndex = Integer.parseInt(slice.getName().substring("shard".length())) - 1;
      Integer taskId = list.get(0);
      // ensures that this doc was assigned to the correct shard
      assertTrue((taskId - shardIndex) % numShards == 0);
    }
  }
}
