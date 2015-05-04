package com.lucidworks.storm.spring;

import backtype.storm.tuple.Tuple;

/**
 * Interface to a POJO that implements some action on streaming data in a Storm topology.
 */
public interface StreamingDataAction {
    SpringBolt.ExecuteResult execute(Tuple input);
}
