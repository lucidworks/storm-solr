package com.lucidworks.storm.solr;

import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lucidworks.storm.spring.SpringBolt;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the SolrJsonBoltAction
 */
public class SolrJsonBoltActionTest extends SolrBoltActionTest {

  @Override
  protected void doBoltActionTest() throws Exception {
    SolrJsonBoltAction sba = new SolrJsonBoltAction(cloudSolrServer);
    sba.setUpdateRequestStrategy(new DefaultUpdateRequestStrategy());
    sba.setMaxBufferSize(1); // to avoid buffering docs

    // Mock the Storm tuple
    String docId = "1";
    String testDoc = "{" +
      "'id':'1'," +
      "'text':'foo'," +
      "'number':10" +
      "}";
    testDoc = testDoc.replace('\'', '"');

    Tuple mockTuple = mock(Tuple.class);
    when(mockTuple.size()).thenReturn(2);
    when(mockTuple.getString(0)).thenReturn(docId);
    when(mockTuple.getValue(1)).thenReturn(testDoc);
    SpringBolt.ExecuteResult result = sba.execute(mockTuple, null);
    assertTrue(result == SpringBolt.ExecuteResult.ACK);
    cloudSolrServer.commit();

    // verify the object to Solr mapping worked correctly using reflection and dynamic fields
    SolrQuery query = new SolrQuery("id:" + docId);
    QueryResponse qr = cloudSolrServer.query(query);
    SolrDocumentList results = qr.getResults();
    assertTrue(results.getNumFound() == 1);
    SolrDocument doc = results.get(0);
    assertNotNull(doc);
    assertEquals("foo", doc.getFirstValue("text"));
    assertEquals("10", doc.getFirstValue("number"));

    // gen a big JSON object now ... one that gets streamed out to Solr
    String[] vocab = new String[]{
        "quick","red","fox","jumped","over","the","lazy","big","dog"
    };
    Random rand = new Random(5150);
    docId = "2";
    JsonNodeFactory factory = JsonNodeFactory.instance;
    ObjectNode root = factory.objectNode();
    root.put("id", docId);
    for (int i=0; i < 200; i++) {
      root.put("f"+i+"_s", randomString(vocab, rand));
    }

    mockTuple = mock(Tuple.class);
    when(mockTuple.size()).thenReturn(2);
    when(mockTuple.getString(0)).thenReturn(docId);
    when(mockTuple.getValue(1)).thenReturn(root);
    result = sba.execute(mockTuple, null);
    assertTrue(result == SpringBolt.ExecuteResult.ACK);
    cloudSolrServer.commit();

    query = new SolrQuery("id:" + docId);
    qr = cloudSolrServer.query(query);
    results = qr.getResults();
    assertTrue(results.getNumFound() == 1);
    assertNotNull(results.get(0));
  }

  protected String randomString(final String[] vocab, final Random rand) {
    int numWords = rand.nextInt(1000)+1;
    StringBuilder sb = new StringBuilder();
    for (int w=0; w < numWords; w++) {
      if (w > 0) sb.append(" ");
      sb.append(vocab[rand.nextInt(vocab.length)]);
    }
    return sb.toString();
  }
}
