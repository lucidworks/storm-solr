package com.lucidworks.storm.spring;

import java.util.Map;

import com.lucidworks.storm.spring.NamedValues;

/**
 * Interface to a Spring-managed POJO that performs some spout logic in Storm topology.
 */
public interface StreamingDataProvider {
  void open(Map stormConf);

  boolean next(NamedValues record) throws Exception;
}
