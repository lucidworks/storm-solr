package com.lucidworks.storm.solr;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the ShardGrouping impl
 */
public class ShardGroupingTest extends TestSolrCloudClusterSupport {

  @Test
  public void testShardGroupingLogic() throws Exception {

    Map stormConf = new HashMap();

    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    String testCollection = "test";
    int numShards = 1;
    int replicationFactor = 1;
    createCollection(testCollection, numShards, replicationFactor, confName, confDir);
    cloudSolrServer.setDefaultCollection(testCollection);

    // this test is pretty weak since the default collection only has 1 shard
    ShardGrouping grouping = new ShardGrouping(stormConf, testCollection);
    grouping.setCloudSolrClient(cloudSolrServer);

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
}
