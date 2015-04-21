package com.lucidworks.storm

import org.junit.After
import org.junit.Before

import backtype.storm.Config

class StreamingDataProviderTestBase {

  Config stormConf

  @Before
  void setup() {
    stormConf = StreamingApp.getConfig(StreamingApp.ENV.test.toString())
    stormConf.put("storm.id", "unit-test")
  }

  @After
  void tearDown() {

  }
}

