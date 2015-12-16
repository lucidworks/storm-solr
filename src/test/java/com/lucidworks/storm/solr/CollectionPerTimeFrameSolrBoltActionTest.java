package com.lucidworks.storm.solr;

import backtype.storm.tuple.Tuple;
import com.lucidworks.storm.spring.SpringBolt;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CollectionPerTimeFrameSolrBoltActionTest extends TestSolrCloudClusterSupport {

  class TestDoc {
    public String id;
    public String text;
    public int number;
    private Date timestamp;

    TestDoc(String id, String text, int number, Date timestamp) throws ParseException {
      this.id = id;
      this.text = text;
      this.number = number;
      this.timestamp = timestamp;
    }

    public Date getTimestamp() {
      return timestamp;
    }
  }

  SimpleDateFormat ISO_8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  @Before
  public void setupCollection() throws Exception {
    ISO_8601.setTimeZone(TimeZone.getTimeZone("UTC"));

    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    String testCollection = "test_2015-12-01-01-05";
    int numShards = 1;
    int replicationFactor = 1;
    createCollection(testCollection, numShards, replicationFactor, confName, confDir);
    cloudSolrClient.setDefaultCollection(testCollection);
  }

  @Test
  public void testBoltAction() throws Exception {
    CollectionPerTimeFrameAssignmentStrategy strat = new CollectionPerTimeFrameAssignmentStrategy();
    strat.setDateTimePattern("yyyy-MM-dd-HH-mm");
    strat.setTimeFrame(5); // 5 minutes
    strat.setTimeUnit(TimeUnit.MINUTES);
    strat.setCollectionNameBase("test_");
    strat.setFieldName("timestamp_tdt");
    strat.setTimezoneId("UTC");
    strat.setNumShards(1);
    strat.setReplicationFactor(1);
    strat.setConfigName("testConfig");

    // Spring @Autowired property at runtime
    SolrBoltAction bolt = new SolrBoltAction(cloudSolrClient);
    bolt.setUpdateRequestStrategy(new DefaultUpdateRequestStrategy());
    bolt.setDocumentAssignmentStrategy(strat);
    bolt.setMaxBufferSize(1); // to avoid buffering docs

    // Mock the Storm tuple
    String docId = "1";
    TestDoc testDoc = new TestDoc(docId, "foo", 10, ISO_8601.parse("2015-12-01T01:09:59.999Z"));
    Tuple mockTuple = mock(Tuple.class);
    when(mockTuple.size()).thenReturn(2);
    when(mockTuple.getString(0)).thenReturn(docId);
    when(mockTuple.getValue(1)).thenReturn(testDoc);
    SpringBolt.ExecuteResult result = bolt.execute(mockTuple, null);
    assertTrue(result == SpringBolt.ExecuteResult.ACK);
    cloudSolrClient.commit();

    // verify the object to Solr mapping worked correctly using reflection and dynamic fields
    SolrQuery query = new SolrQuery("id:" + docId);
    QueryResponse qr = cloudSolrClient.query(query);
    SolrDocumentList results = qr.getResults();
    assertTrue(results.getNumFound() == 1);
    SolrDocument doc = results.get(0);
    assertNotNull(doc);
    assertEquals("foo", doc.getFirstValue("text_s"));
    assertEquals(new Integer(testDoc.number), doc.getFirstValue("number_i"));
    assertTrue(doc.getFirstValue("timestamp_tdt") != null);
  }
}
