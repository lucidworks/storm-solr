package com.lucidworks.storm.solr;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Supports tests that need a SolrCloud cluster.
 */
public abstract class TestSolrCloudClusterSupport {

  static final Logger log = Logger.getLogger(TestSolrCloudClusterSupport.class);

  protected static MiniSolrCloudCluster cluster;
  protected static CloudSolrClient cloudSolrClient;
  protected static Path tempDir;

  @BeforeClass
  public static void startCluster() throws Exception {
    File solrXml = new File("src/test/resources/solr.xml");

    tempDir = Files.createTempDirectory("MiniSolrCloudCluster");

    try {
      cluster = new MiniSolrCloudCluster(1, null, tempDir, MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML, null, null);
    } catch (Exception exc) {
      log.error("Failed to initialize a MiniSolrCloudCluster due to: " + exc, exc);
      throw exc;
    }

    cloudSolrClient = new CloudSolrClient(cluster.getZkServer().getZkAddress(), true);
    cloudSolrClient.connect();

    assertTrue(!cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes().isEmpty());
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    if (cloudSolrClient != null) {
      cloudSolrClient.shutdown();
    }
    if (cluster != null) {
      cluster.shutdown();
    }

    // Delete tempDir content
    Files.walkFileTree(tempDir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }

    });
  }

  protected static void createCollection(String collectionName, int numShards, int replicationFactor, String
      confName) throws Exception {
    createCollection(collectionName, numShards, replicationFactor, confName, null);
  }

  protected static void createCollection(String collectionName, int numShards, int replicationFactor, String
      confName, File confDir) throws Exception {
    if (confDir != null) {
      assertTrue("Specified Solr config directory '" +
          confDir.getAbsolutePath() + "' not found!", confDir.isDirectory());

      // upload the test configs
      SolrZkClient zkClient = cloudSolrClient.getZkStateReader().getZkClient();
      ZkConfigManager zkConfigManager =
          new ZkConfigManager(zkClient);

      zkConfigManager.uploadConfigDir(confDir.toPath(), confName);
    }

    ModifiableSolrParams modParams = new ModifiableSolrParams();
    modParams.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name());
    modParams.set("name", collectionName);
    modParams.set("numShards", numShards);
    modParams.set("replicationFactor", replicationFactor);


    int liveNodes = cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes().size();
    int maxShardsPerNode = (int) Math.ceil(((double) numShards * replicationFactor) / liveNodes);

    modParams.set("maxShardsPerNode", maxShardsPerNode);
    modParams.set("collection.configName", confName);
    QueryRequest request = new QueryRequest(modParams);
    request.setPath("/admin/collections");
    cloudSolrClient.request(request);
    ensureAllReplicasAreActive(collectionName, numShards, replicationFactor, 20);
  }

  protected static void ensureAllReplicasAreActive(String testCollectionName, int shards, int rf, int maxWaitSecs)
      throws Exception {
    long startMs = System.currentTimeMillis();

    ZkStateReader zkr = cloudSolrClient.getZkStateReader();
    zkr.updateClusterState(); // force the state to be fresh

    ClusterState cs = zkr.getClusterState();
    Collection<Slice> slices = cs.getActiveSlices(testCollectionName);
    assertTrue(slices.size() == shards);
    boolean allReplicasUp = false;
    long waitMs = 0L;
    long maxWaitMs = maxWaitSecs * 1000L;
    Replica leader = null;
    while (waitMs < maxWaitMs && !allReplicasUp) {
      // refresh state every 2 secs
      if (waitMs % 2000 == 0) {
        log.info("Updating ClusterState");
        cloudSolrClient.getZkStateReader().updateClusterState();
      }

      cs = cloudSolrClient.getZkStateReader().getClusterState();
      assertNotNull(cs);
      allReplicasUp = true; // assume true
      for (Slice shard : cs.getActiveSlices(testCollectionName)) {
        String shardId = shard.getName();
        assertNotNull("No Slice for " + shardId, shard);
        Collection<Replica> replicas = shard.getReplicas();
        assertTrue(replicas.size() == rf);
        leader = shard.getLeader();
        assertNotNull(leader);
        log.info("Found " + replicas.size() + " replicas and leader on " +
            leader.getNodeName() + " for " + shardId + " in " + testCollectionName);

        // ensure all replicas are "active"
        for (Replica replica : replicas) {
          String replicaState = replica.getStr(ZkStateReader.STATE_PROP);
          if (!"active".equals(replicaState)) {
            log.info("Replica " + replica.getName() + " for shard " + shardId + " is currently " + replicaState);
            allReplicasUp = false;
          }
        }
      }

      if (!allReplicasUp) {
        try {
          Thread.sleep(500L);
        } catch (Exception ignoreMe) {
        }
        waitMs += 500L;
      }
    } // end while

    if (!allReplicasUp)
      fail("Didn't see all replicas for " + testCollectionName +
          " come up within " + maxWaitMs + " ms! ClusterState: " + printClusterStateInfo(testCollectionName));

    long diffMs = (System.currentTimeMillis() - startMs);
    log.info("Took " + diffMs + " ms to see all replicas become active for " + testCollectionName);
  }

  protected static String printClusterStateInfo(String collection) throws Exception {
    cloudSolrClient.getZkStateReader().updateClusterState();
    String cs = null;
    ClusterState clusterState = cloudSolrClient.getZkStateReader().getClusterState();
    if (collection != null) {
      cs = clusterState.getCollection(collection).toString();
    } else {
      Map<String, DocCollection> map = new HashMap<String, DocCollection>();
      for (String coll : clusterState.getCollections())
        map.put(coll, clusterState.getCollection(coll));
      CharArr out = new CharArr();
      new JSONWriter(out, 2).write(map);
      cs = out.toString();
    }
    return cs;
  }

}
