package com.lucidworks.storm.eventsim;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.lucidworks.storm.StormTopologyFactory;
import com.lucidworks.storm.StreamingApp;
import com.lucidworks.storm.solr.HashRangeGrouping;
import com.lucidworks.storm.spring.SpringBolt;
import com.lucidworks.storm.spring.SpringSpout;

public class EventsimTopology implements StormTopologyFactory {

  static final Fields spoutFields = new Fields("id","line");

  public String getName() {
    return "eventsim";
  }

  public StormTopology build(StreamingApp app) throws Exception {
    SpringSpout eventsimSpout = new SpringSpout("eventsimSpout", spoutFields);
    SpringBolt collectionPerTimeFrameSolrBolt = new SpringBolt("collectionPerTimeFrameSolrBoltAction",
        app.tickRate("collectionPerTimeFrameSolrBoltAction"));

    // Send all docs for the same hash range to the same bolt instance,
    // which allows us to use a streaming approach to send docs to the leader
    int numShards = Integer.parseInt(String.valueOf(app.getStormConfig().get("spring.eventsimNumShards")));
    HashRangeGrouping hashRangeGrouping = new HashRangeGrouping(app.getStormConfig(), numShards);
    int tasksPerShard = hashRangeGrouping.getNumShards()*2;

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("eventsimSpout", eventsimSpout, app.parallelism("eventsimSpout"));
    builder.setBolt("collectionPerTimeFrameSolrBolt", collectionPerTimeFrameSolrBolt, tasksPerShard)
           .customGrouping("eventsimSpout", hashRangeGrouping);

    return builder.createTopology();
  }
}
