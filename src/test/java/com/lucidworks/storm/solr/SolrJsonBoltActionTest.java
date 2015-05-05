package com.lucidworks.storm.solr;

import backtype.storm.tuple.Tuple;
import com.lucidworks.storm.spring.SpringBolt;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the SolrJsonBoltAction
 */
public class SolrJsonBoltActionTest extends SolrBoltActionTest {

  @Override
  protected void doBoltActionTest() throws Exception {
    SolrJsonBoltAction sba = new SolrJsonBoltAction();
    sba.batchSize = 1; // to avoid buffering docs
    sba.cloudSolrClient = cloudSolrServer; // Spring @Autowired property in a real env

    // Mock the Storm tuple
    String docId = "1";
    String testDoc = "{" +
      "'id':'1',"+
      "'text':'foo',"+
      "'number':10"+
      "}";
    testDoc = testDoc.replace('\'','"');

    Tuple mockTuple = mock(Tuple.class);
    when(mockTuple.size()).thenReturn(2);
    when(mockTuple.getString(0)).thenReturn(docId);
    when(mockTuple.getValue(1)).thenReturn(testDoc);
    SpringBolt.ExecuteResult result = sba.execute(mockTuple, null);
    assertTrue(result == SpringBolt.ExecuteResult.ACK);
    cloudSolrServer.commit();

    // verify the object to Solr mapping worked correctly using reflection and dynamic fields
    SolrQuery query = new SolrQuery("id:"+docId);
    QueryResponse qr = cloudSolrServer.query(query);
    SolrDocumentList results = qr.getResults();
    assertTrue(results.getNumFound()==1);
    SolrDocument doc = results.get(0);
    assertNotNull(doc);
    assertEquals("foo", doc.getFirstValue("text"));
    assertEquals("10", doc.getFirstValue("number"));
  }
}
