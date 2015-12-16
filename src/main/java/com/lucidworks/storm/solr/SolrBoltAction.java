package com.lucidworks.storm.solr;

import static com.lucidworks.storm.spring.SpringBolt.ExecuteResult;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.lucidworks.storm.spring.StreamingDataAction;
import com.lucidworks.storm.spring.TickTupleAware;
import com.ryantenney.metrics.annotation.Metric;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple Spring-managed POJO for sending messages processed by a Storm topology to SolrCloud.
 * Bean implementations do not need to be thread-safe but should be created in the prototype scope
 * to support multiple bolts running in the same JVM in the same Storm topology.
 */
public class SolrBoltAction implements StreamingDataAction, TickTupleAware, Closeable {

  public static Logger log = Logger.getLogger(SolrBoltAction.class);
  
  @Metric
  public Timer sendBatchToSolr;

  @Metric
  public Counter indexedCounter;

  @Metric
  public Counter tuplesReceived;

  protected CloudSolrClient cloudSolrClient;
  protected SolrInputDocumentMapper solrInputDocumentMapper;
  protected int maxBufferSize = 100; // avoids sending 100's of requests per second to Solr in high-throughput envs
  protected long bufferTimeoutMs = 500L;
  protected SolrUpdateRequestStrategy updateRequestStrategy;
  protected DocumentAssignmentStrategy documentAssignmentStrategy;

  // used internally for buffering docs before sending to Solr
  private Map<String,DocBuffer> buffers = new HashMap<String,DocBuffer>();

  @Autowired
  public SolrBoltAction(CloudSolrClient cloudSolrClient) {
    this.cloudSolrClient = cloudSolrClient;
    this.cloudSolrClient.connect();
  }

  public ExecuteResult onTick() {
    boolean anyNeedsFlush = false;
    for (DocBuffer b : buffers.values()) {
      // this catches the case where we have buffered docs, but don't see any more docs flowing in for a while
      if (b.shouldFlushBuffer()) {
        anyNeedsFlush = true;
        break;
      }
    }

    if (anyNeedsFlush) {
      // have to flush them all so we can ack correctly
      for (DocBuffer b : buffers.values()) {
        flushBufferedDocs(b);
      }
      return ExecuteResult.ACK;
    }

    // todo: remove old DocBuffer objects from the map
    // todo: could pro-actively create collections that will be needed soon here

    return ExecuteResult.IGNORED;
  }

  public ExecuteResult execute(Tuple input, OutputCollector outputCollector) {

    if (tuplesReceived != null) {
      tuplesReceived.inc();
    }

    String docId = input.getString(0);
    Object docObj = input.getValue(1);
    if (docId == null || docObj == null) {

      log.warn("Ignored tuple: "+input);

      return ExecuteResult.IGNORED; // nothing to index
    }

    try {
      return processInputDoc(docId, docObj);
    } catch (Exception exc) {
      log.error("Failed to process "+docId+" due to: "+exc);
      if (exc instanceof RuntimeException) {
        throw (RuntimeException)exc;
      } else {
        throw new RuntimeException(exc);
      }
    }
  }

  /**
   * Process an input document that has already been validated; good place to start for sub-classes to
   * plug-in their own input Tuple processing logic.
   */
  protected ExecuteResult processInputDoc(String docId, Object docObj) throws Exception {
    // default if not auto-wired
    if (solrInputDocumentMapper == null)
      solrInputDocumentMapper = new DefaultSolrInputDocumentMapper();

    SolrInputDocument doc = solrInputDocumentMapper.toInputDoc(docId, docObj);
    if (doc == null)
      return ExecuteResult.IGNORED; // mapper doesn't want this object indexed

    if (documentAssignmentStrategy == null) {
      // relies on the CloudSolrClient having a default collection specified
      documentAssignmentStrategy = new DefaultDocumentAssignmentStrategy();
    }

    return bufferDoc(documentAssignmentStrategy.getCollectionForDoc(cloudSolrClient, doc), doc);
  }

  public int getMaxBufferSize() {
    return maxBufferSize;
  }

  public void setMaxBufferSize(int maxBufferSize) {
    this.maxBufferSize = maxBufferSize;
  }

  public long getBufferTimeoutMs() {
    return bufferTimeoutMs;
  }

  public void setBufferTimeoutMs(long bufferTimeoutMs) {
    this.bufferTimeoutMs = bufferTimeoutMs;
  }

  public SolrInputDocumentMapper getSolrInputDocumentMapper() {
    return solrInputDocumentMapper;
  }

  public void setSolrInputDocumentMapper(SolrInputDocumentMapper solrInputDocumentMapper) {
    this.solrInputDocumentMapper = solrInputDocumentMapper;
  }

  public SolrUpdateRequestStrategy getUpdateRequestStrategy() {
    return updateRequestStrategy;
  }

  public void setUpdateRequestStrategy(SolrUpdateRequestStrategy updateRequestStrategy) {
    this.updateRequestStrategy = updateRequestStrategy;
  }

  public DocumentAssignmentStrategy getDocumentAssignmentStrategy() {
    return documentAssignmentStrategy;
  }

  public void setDocumentAssignmentStrategy(DocumentAssignmentStrategy documentAssignmentStrategy) {
    this.documentAssignmentStrategy = documentAssignmentStrategy;
  }

  public UpdateRequest createUpdateRequest(String collection) {
    UpdateRequest req = new UpdateRequest();
    req.setParam("collection", collection);
    return req;
  }

  protected ExecuteResult bufferDoc(String collection, SolrInputDocument doc) {
    DocBuffer docBuffer = buffers.get(collection);
    if (docBuffer == null) {
      docBuffer = new DocBuffer(collection, maxBufferSize, bufferTimeoutMs);
      buffers.put(collection, docBuffer);
    }
    docBuffer.add(doc);
    return docBuffer.shouldFlushBuffer() ? flushBufferedDocs(docBuffer) : ExecuteResult.BUFFERED;
  }

  protected ExecuteResult flushBufferedDocs(DocBuffer b) {
    int numDocsInBatch = b.buffer.size();
    if (numDocsInBatch == 0) {
      b.reset();
      return ExecuteResult.ACK;
    }

    Timer.Context timer = (sendBatchToSolr != null) ? sendBatchToSolr.time() : null;
    try {
      sendBatchToSolr(b);
    } finally {
      if (timer != null)
        timer.stop();

      if (indexedCounter != null)
        indexedCounter.inc(numDocsInBatch);

      b.reset();
    }

    return ExecuteResult.ACK;
  }

  protected void sendBatchToSolr(DocBuffer b) {
    if (log.isDebugEnabled())
      log.debug("Sending buffer of " + b.buffer.size() + " to collection " + b.collection);

    UpdateRequest req = createUpdateRequest(b.collection);
    req.add(b.buffer);
    updateRequestStrategy.sendUpdateRequest(cloudSolrClient, b.collection, req);
  }

  public void close() throws IOException {

    // flush any buffered docs before shutting down
    for (DocBuffer b : buffers.values()) {
      if (!b.buffer.isEmpty()) {
        try {
          flushBufferedDocs(b);
        } catch (Exception exc) {
          log.error("Failed to flush buffered docs for "+b.collection+" before shutting down due to: "+exc, exc);
        }
      }
    }
    buffers.clear();

    if (documentAssignmentStrategy != null && documentAssignmentStrategy instanceof Closeable) {
      try {
        ((Closeable)documentAssignmentStrategy).close();
      } catch (Exception ignore) {
        log.warn("Error when trying to close the documentAssignmentStrategy due to: "+ignore);
      }
    }

    if (updateRequestStrategy != null && updateRequestStrategy instanceof Closeable) {
      try {
        ((Closeable)updateRequestStrategy).close();
      } catch (Exception ignore) {
        log.warn("Error when trying to close the updateRequestStrategy due to: "+ignore);
      }
    }
    
    if (solrInputDocumentMapper != null && solrInputDocumentMapper instanceof Closeable) {
      try {
        ((Closeable)solrInputDocumentMapper).close();
      } catch (Exception ignore) {
        log.warn("Error when trying to close the solrInputDocumentMapper due to: "+ignore);
      }
    }
  }
}
