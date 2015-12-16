package com.lucidworks.storm.solr;

import java.io.File;
import java.util.Date;

import backtype.storm.tuple.Tuple;
import com.lucidworks.storm.spring.SpringBolt;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.json.simple.JSONArray;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertTrue;

/**
 * Tests the SolrBoltAction
 */
public class SolrBoltActionTest extends TestSolrCloudClusterSupport {

  class TestDoc {
    public String id;
    public String text;
    public int number;
    private Date timestamp;

    TestDoc(String id, String text, int number) {
      this.id = id;
      this.text = text;
      this.number = number;
      this.timestamp = new Date();
    }

    public Date getTimestamp() {
      return timestamp;
    }
  }

  @Before
  public void setupCollection() throws Exception {
    String confName = "testConfig";
    File confDir = new File("src/test/resources/conf");
    String testCollection = "test";
    int numShards = 1;
    int replicationFactor = 1;
    createCollection(testCollection, numShards, replicationFactor, confName, confDir);
    cloudSolrClient.setDefaultCollection(testCollection);
  }

  @Test
  public void testBoltAction() throws Exception {
    doBoltActionTest();
    //doNestedDocTest();
  }

  protected void doBoltActionTest() throws Exception {
    // Spring @Autowired property at runtime
    SolrBoltAction sba = new SolrBoltAction(cloudSolrClient);
    sba.setUpdateRequestStrategy(new DefaultUpdateRequestStrategy());
    sba.setMaxBufferSize(1); // to avoid buffering docs

    // Mock the Storm tuple
    String docId = "1";
    TestDoc testDoc = new TestDoc(docId, "foo", 10);
    Tuple mockTuple = mock(Tuple.class);
    when(mockTuple.size()).thenReturn(2);
    when(mockTuple.getString(0)).thenReturn(docId);
    when(mockTuple.getValue(1)).thenReturn(testDoc);
    SpringBolt.ExecuteResult result = sba.execute(mockTuple, null);
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

  protected void doNestedDocTest() throws Exception {
    SolrBoltAction sba = new SolrBoltAction(cloudSolrClient);
    sba.setSolrInputDocumentMapper(new NestedDocumentMapper());
    sba.setUpdateRequestStrategy(new DefaultUpdateRequestStrategy());
    sba.setMaxBufferSize(1); // to avoid buffering docs

    String docId = "1";

    JSONArray jsonDocs = NestedDocumentMapperTest.loadNestedDocs();
    Tuple mockTuple = mock(Tuple.class);
    when(mockTuple.size()).thenReturn(2);
    when(mockTuple.getString(0)).thenReturn(docId);
    when(mockTuple.getValue(1)).thenReturn(jsonDocs.get(0));

    SpringBolt.ExecuteResult result = sba.execute(mockTuple, null);
    assertTrue(result == SpringBolt.ExecuteResult.ACK);
    cloudSolrClient.commit();

    // verify the object to Solr mapping worked correctly using reflection and dynamic fields
    SolrQuery query = new SolrQuery("id:" + docId);
    query.set("fl", "*,[child parentFilter=id:"+docId+" limit=100]");
    QueryResponse qr = cloudSolrClient.query(query);
    SolrDocumentList results = qr.getResults();
    assertTrue(results.getNumFound() == 1);
    SolrDocument doc = results.get(0);
    assertNotNull(doc);

    System.out.println("\n\n>> doc: "+doc+"\n\n");
    if (doc.hasChildDocuments()) {
      for (SolrDocument child : doc.getChildDocuments()) {
        System.out.println("\n>> "+child+"\n");
      }
    } else {
      System.out.println("no child documents returned");
    }

  }
}
