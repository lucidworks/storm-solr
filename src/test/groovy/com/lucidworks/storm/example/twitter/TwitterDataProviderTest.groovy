package com.lucidworks.storm.example.twitter

import backtype.storm.tuple.Fields
import com.lucidworks.storm.spring.NamedValues
import com.lucidworks.storm.StreamingDataProviderTestBase

import org.junit.Ignore
import org.junit.Test

import static org.junit.Assert.assertTrue
import static org.junit.Assert.assertNotNull

/**
 * Test case is ignored by default since it requires valid Twitter credentials.
 */
@Ignore
class TwitterDataProviderTest extends StreamingDataProviderTestBase {
  @Test
  void testDataProvider() {
    TwitterDataProvider provider = new TwitterDataProvider()
    provider.open(stormConf)

    Thread.sleep(5000) // give a little time to connect to twitter and start receiving tweets

    NamedValues record = new NamedValues(new Fields("id","tweet"))
    assertTrue provider.next(record)
    assertTrue record.messageId == null

    List<Object> values = record.values()
    assertNotNull values
    assertTrue values.size() == TwitterToSolrTopology.spoutFields.size()
    assertNotNull values.get(0)
    assertNotNull values.get(1)
  }
}