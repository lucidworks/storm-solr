package com.lucidworks.storm.solr;

import org.apache.solr.common.SolrInputDocument;

import java.util.List;
import java.util.Map;

/**
 * Take a Map and transform it into a SolrInputDocument by flattening nested fields into a single document; for
 * block-join style nested documents, see NestedDocumentMapper
 */
public class JsonDocumentMapper implements SolrInputDocumentMapper {

  protected String idFieldName = "id";
  protected String compoundNameDelimiter = ".";

  public String getIdFieldName() {
    return idFieldName;
  }

  public void setIdFieldName(String idFieldName) {
    this.idFieldName = idFieldName;
  }

  public String getCompoundNameDelimiter() {
    return compoundNameDelimiter;
  }

  public void setCompoundNameDelimiter(String compoundNameDelimiter) {
    this.compoundNameDelimiter = compoundNameDelimiter;
  }

  public SolrInputDocument toInputDoc(String docId, Object obj) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(idFieldName, docId);
    addFieldsToDoc(null /* top level no prefix */, doc, (Map)obj);
    return doc;
  }

  protected void addFieldsToDoc(String prefix, SolrInputDocument doc, Map map) {
    for (Object key : map.keySet()) {
      Object val = map.get(key);
      if (val != null)
        addFieldToDoc(prefix, doc, key.toString(), val);
    }
  }

  protected void addFieldToDoc(String prefix, SolrInputDocument doc, String fieldName, Object fieldValue) {
    if (fieldValue instanceof List) {
      // peek in the list to see if the values are flat or nested objects
      List list = (List)fieldValue;
      if (list.isEmpty())
        return;

      Object first = list.get(0);
      if (first instanceof Map) {
        // nested object here
        String fieldNamePrefix = fname(prefix, fieldName);
        for (int i=0; i < list.size(); i++) {
          addFieldsToDoc(fname(fieldNamePrefix,i), doc, (Map)list.get(i));
        }
      } else if (first instanceof List) {
        // list of list
        String fieldNamePrefix = fname(prefix, fieldName);
        for (int i=0; i < list.size(); i++) {
          addFieldToDoc(fieldNamePrefix, doc, String.valueOf(i), (List)list.get(i));
        }
      } else {
        // just a multi-valued field here
        doc.setField(fname(prefix, fieldName), fieldValue);
      }
    } else if (fieldValue instanceof Map) {
      addFieldsToDoc(fname(prefix, fieldName), doc, (Map)fieldValue);
    } else {
      // default behavior, just add this field to the doc
      doc.setField(fname(prefix, fieldName), fieldValue);
    }
  }

  protected final String fname(final String prefix, final String name) {
    return (prefix != null) ? prefix + compoundNameDelimiter + name : name;
  }

  protected final String fname(final String prefix, final int index) {
    return prefix + compoundNameDelimiter + index;
  }
}
