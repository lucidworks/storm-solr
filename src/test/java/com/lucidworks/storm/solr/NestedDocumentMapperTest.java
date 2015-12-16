package com.lucidworks.storm.solr;

import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class NestedDocumentMapperTest {

  public static Logger log = Logger.getLogger(NestedDocumentMapperTest.class);

  public static JSONArray loadNestedDocs() throws Exception {
    JSONArray jsonDocs = null;
    InputStreamReader isr = null;
    String testDocsOnCpath = "test-data/nested_docs.json";
    try {
      isr = new InputStreamReader(NestedDocumentMapperTest.class.getClassLoader().getResourceAsStream(testDocsOnCpath),
          StandardCharsets.UTF_8);
      jsonDocs = (JSONArray)JSONValue.parse(isr);
    } finally {
      if (isr != null) {
        try {
          isr.close();
        } catch (Exception ignore){}
      }
    }
    assertNotNull("Failed to load test JSON docs from " + testDocsOnCpath, jsonDocs);
    return jsonDocs;
  }

  @Test
  public void testNestedDocs() throws Exception {
    JSONArray jsonDocs = loadNestedDocs();
    NestedDocumentMapper mapper = new NestedDocumentMapper();
    for (int d=0; d < jsonDocs.size(); d++) {
      String docId = "doc"+d;
      SolrInputDocument doc = mapper.toInputDoc(docId, jsonDocs.get(d));
      assertNotNull(doc);
      debugDoc(doc);
    }
  }

  protected void debugDoc(SolrInputDocument doc) {
    debugDoc(System.out, doc, 0);
  }

  protected void debugDoc(PrintStream out, SolrInputDocument doc, int depth) {
    Collection<String> fieldNames = doc.getFieldNames();
    Iterator<String> fieldNameIter = fieldNames.iterator();
    String tabs = tabs(depth);
    while (fieldNameIter.hasNext()) {
      String fieldName = fieldNameIter.next();
      if ("id".equals(fieldName))
        continue;

      out.print(tabs);
      out.println(doc.get(fieldName));
    }

    List<SolrInputDocument> childDocs = doc.getChildDocuments();
    if (childDocs != null && !childDocs.isEmpty()) {
      out.print(tabs);
      out.println("_childDocuments_: [");
      int childDepth = depth+1;
      String childTabs = tabs(depth+1);
      for (SolrInputDocument child : childDocs) {
        out.print(childTabs);
        out.println(child.getFieldValue("id") + " : {");
        debugDoc(out, child, childDepth+1);
        out.print(childTabs);
        out.println("}");
      }
      out.print(tabs);
      out.println("]");
    }
  }

  protected String tabs(int depth) {
    String tabs = "";
    for (int t=0; t < depth; t++)
      tabs += "  ";
    return tabs;
  }
}
