package com.lucidworks.storm.example.hdfs

import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import com.lucidworks.storm.StormTopologyFactory
import com.lucidworks.storm.StreamingApp
import com.lucidworks.storm.solr.ShardGrouping
import com.lucidworks.storm.spring.SpringBolt
import com.lucidworks.storm.spring.SpringSpout

class HdfsDirectoryToSolrTopology implements StormTopologyFactory {

  static Fields spoutFields = new Fields("fileUri")

  String getName() { return "hdfs-to-solr" }

  StormTopology build(StreamingApp app) throws Exception {
    // setup spout and bolts for accessing Spring-managed POJOs at runtime
    SpringSpout hdfsSpout = new SpringSpout("hdfsDirectoryListingDataProvider", spoutFields);
    SpringBolt csvParserBolt = new SpringBolt("csvParserBoltAction", new Fields("id", ""));
    SpringBolt solrBolt = new SpringBolt("solrBoltAction", app.tickRate("solrBolt"));

    // Route messages based on shard assignment in Solr (because we can)
    // and set the parallelism to the number of shards in the collection
    String collection = app.getStormConfig().get("spring.defaultCollection");
    ShardGrouping shardGrouping = new ShardGrouping(app.getStormConfig(), collection);
    int numShards = shardGrouping.getNumShards();

    // wire up the topology to read from hdfs and send to Solr
    TopologyBuilder builder = new TopologyBuilder()
    builder.setSpout("hdfsSpout", hdfsSpout, app.parallelism("hdfsSpout"))
    builder.setBolt("csvParserBolt", csvParserBolt, app.parallelism("csvParserBolt")).shuffleGrouping("hdfsSpout")
    builder.setBolt("solrBolt", solrBolt, numShards).customGrouping("csvParserBolt", shardGrouping)

    return builder.createTopology()
  }
}