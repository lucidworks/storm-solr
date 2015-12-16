package com.lucidworks.storm.solr;

import org.apache.solr.common.SolrInputDocument;

import java.util.List;
import java.util.Map;

public class NestedDocumentMapper implements SolrInputDocumentMapper {

  protected String idFieldName = "id";
  protected String nestedIdSep = ".";

  public SolrInputDocument toInputDoc(String docId, Object obj) {
    Map mapObj = (Map)obj;
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(idFieldName, docId);
    addFieldsToDoc(docId, doc, mapObj);
    return doc;
  }

  protected void addFieldsToDoc(String blockId, SolrInputDocument doc, Map map) {
    for (Object key : map.keySet()) {
      Object val = map.get(key);
      if (val != null)
        addFieldToDoc(blockId, doc, key.toString(), val);
    }
  }

  protected void addFieldToDoc(String blockId, SolrInputDocument doc, String fieldName, Object fieldValue) {
    if (fieldValue instanceof List) {
      // peek in the list to see if the values are flat or nested objects
      List list = (List)fieldValue;
      if (list.isEmpty())
        return;

      Object first = list.get(0);
      if (first instanceof Map) {
        // nested object here
        for (int i=0; i < list.size(); i++) {
          Map childMap = (Map)list.get(i);
          Object childId = childMap.get(idFieldName);
          if (childId == null)
            childId = String.format("%s%s%s%s%d",
                doc.getFieldValue(idFieldName), nestedIdSep, fieldName, nestedIdSep, i);

          SolrInputDocument child = new SolrInputDocument();
          child.setField(idFieldName, childId.toString());
          doc.addChildDocument(child);
          addFieldsToDoc(blockId, child, childMap);
        }
      } else if (first instanceof List) {
        // list of list
        for (int i=0; i < list.size(); i++) {
          String listId = String.format("%s%s%s%s%d",
              doc.getFieldValue(idFieldName), nestedIdSep, fieldName, nestedIdSep, i);
          SolrInputDocument child = new SolrInputDocument();
          child.setField(idFieldName, listId);
          doc.addChildDocument(child);
          addFieldToDoc(blockId, child, listId, (List) list.get(i));
        }
      } else {
        // just a multi-valued field here
        doc.setField(fieldName, fieldValue);
      }
    } else if (fieldValue instanceof Map) {
      Map childMap = (Map)fieldValue;
      Object childId = childMap.get(idFieldName);
      if (childId == null)
        childId = String.format("%s%s%s", doc.getFieldValue(idFieldName), nestedIdSep, fieldName);

      SolrInputDocument child = new SolrInputDocument();
      child.setField(idFieldName, childId.toString());
      doc.addChildDocument(child);
      addFieldsToDoc(blockId, child, childMap);
    } else {
      // default behavior, just add this field to the doc
      doc.setField(fieldName, fieldValue);
    }
  }
}
