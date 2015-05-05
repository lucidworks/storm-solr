package com.lucidworks.storm.solr;

import org.apache.solr.common.util.ContentStream;

/**
 * Interface representing an object that converts an object into a ContentStream for one or more JSON documents.
 */
public interface JsonContentStreamMapper {
  ContentStream toContentStream(String docId, Object docObj) throws Exception;
}
