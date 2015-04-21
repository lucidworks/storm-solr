package com.lucidworks.storm;

import backtype.storm.generated.StormTopology;

/**
 * A class that builds a StormTopology to be executed in a common framework by this driver.
 */
public interface StormTopologyFactory {
    String getName();
    StormTopology build(StreamingApp app) throws Exception;
}
