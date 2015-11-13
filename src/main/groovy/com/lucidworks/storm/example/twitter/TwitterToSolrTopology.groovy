package com.lucidworks.storm.example.twitter

import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import com.lucidworks.storm.StormTopologyFactory
import com.lucidworks.storm.StreamingApp
import com.lucidworks.storm.solr.ShardGrouping
import com.lucidworks.storm.spring.SpringBolt
import com.lucidworks.storm.spring.SpringSpout

class TwitterToSolrTopology implements StormTopologyFactory {

  static final Fields spoutFields = new Fields("id", "tweet")

  String getName() { return "twitter-to-solr" }

  StormTopology build(StreamingApp app) throws Exception {
    // setup spout and bolts for accessing Spring-managed POJOs at runtime
    SpringSpout twitterSpout = new SpringSpout("twitterDataProvider", spoutFields);
    SpringBolt solrBolt = new SpringBolt("solrBoltAction", app.tickRate("solrBolt"));

    SpringBolt solrJsonBolt = new SpringBolt("solrJsonBoltAction", app.tickRate("solrBolt"));

    // Route messages based on shard assignment in Solr (because we can)
    // and set the parallelism to the number of shards in the collection
    String collection = app.getStormConfig().get("spring.defaultCollection");
    ShardGrouping shardGrouping = new ShardGrouping(app.getStormConfig(), collection);
    int numShards = shardGrouping.getNumShards();

    // wire up the topology to read tweets and send to Solr
    TopologyBuilder builder = new TopologyBuilder()
    builder.setSpout("twitterSpout", twitterSpout, app.parallelism("twitterSpout"))
    //builder.setBolt("solrBolt", solrBolt, numShards).customGrouping("twitterSpout", shardGrouping)
    builder.setBolt("solrJsonBolt", solrJsonBolt, numShards).customGrouping("twitterSpout", shardGrouping)

    return builder.createTopology()
  }
}