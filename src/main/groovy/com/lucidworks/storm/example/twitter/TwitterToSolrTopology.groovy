package com.lucidworks.storm.example.twitter

import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields

import com.lucidworks.storm.StormTopologyFactory
import com.lucidworks.storm.StreamingApp
import com.lucidworks.storm.spring.SpringBolt
import com.lucidworks.storm.spring.SpringSpout

class TwitterToSolrTopology implements StormTopologyFactory {

  String getName() { return "twitter-to-solr" }

  StormTopology build(StreamingApp app) throws Exception {
    // setup spout and bolts for accessing Spring-managed POJOs at runtime
    SpringSpout twitterSpout = new SpringSpout("twitterDataProvider", new Fields("id","tweet"));
    SpringBolt solrBolt = new SpringBolt("solrBoltAction", app.tickRate("solrBolt"));

    // wire up the topology to read tweets and send to Solr
    TopologyBuilder builder = new TopologyBuilder()
    builder.setSpout("twitterSpout", twitterSpout, app.parallelism("twitterSpout"))
    builder.setBolt("solrBolt", solrBolt, app.parallelism("solrBolt")).shuffleGrouping("twitterSpout")

    return builder.createTopology()
  }
}
