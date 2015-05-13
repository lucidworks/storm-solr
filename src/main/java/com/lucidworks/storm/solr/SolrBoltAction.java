package com.lucidworks.storm.solr;

import static com.lucidworks.storm.spring.SpringBolt.ExecuteResult;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.lucidworks.storm.spring.StreamingDataAction;
import com.lucidworks.storm.spring.TickTupleAware;
import com.ryantenney.metrics.annotation.Metric;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A simple Spring-managed POJO for sending messages processed by a Storm topology to SolrCloud.
 * Bean implementations do not need to be thread-safe but should be created in the prototype scope
 * to support multiple bolts running in the same JVM in the same Storm topology.
 */
public class SolrBoltAction implements StreamingDataAction, TickTupleAware {

  public static Logger log = Logger.getLogger(SolrBoltAction.class);
  
  @Metric
  public Timer sendBatchToSolr;

  @Metric
  public Counter indexedCounter;

  @Metric
  public Histogram batchSizeHisto;

  protected CloudSolrClient cloudSolrClient;
  protected SolrInputDocumentMapper solrInputDocumentMapper;
  protected String collection; // defaults to the default set on the cloud client
  protected int maxBufferSize = 100; // avoids sending 100's of requests per second to Solr in high-throughput envs
  protected List<SolrInputDocument> buffer;
  protected long bufferTimeoutMs = 500L;
  protected SolrUpdateRequestStrategy updateRequestStrategy;

  private long bufferTimeoutAtNanos = -1L;

  @Autowired
  public SolrBoltAction(CloudSolrClient cloudSolrClient) {
    this.cloudSolrClient = cloudSolrClient;
  }

  public ExecuteResult onTick() {
    // this catches the case where we have buffered docs, but don't see any more docs flowing in for a while
    if (buffer != null && !buffer.isEmpty() && (bufferTimeoutAtNanos < 0 || System.nanoTime() >= bufferTimeoutAtNanos))
      return flushBufferedDocs();

    return ExecuteResult.IGNORED;
  }

  public ExecuteResult execute(Tuple input, OutputCollector outputCollector) {

    if (collection == null) {
      collection = cloudSolrClient.getDefaultCollection();
      if (collection == null)
        throw new IllegalStateException("Collection name not configured for the Solr bolt!");

      String mapper = (solrInputDocumentMapper != null ? solrInputDocumentMapper.getClass().getSimpleName()
        : DefaultSolrInputDocumentMapper.class.getSimpleName());
      log.info("Configured to send docs to " + collection +
        " using mapper=" + mapper + ", maxBufferSize=" + maxBufferSize + ", and bufferTimeoutMs=" + bufferTimeoutMs);
    }

    String docId = input.getString(0);
    Object docObj = input.getValue(1);
    if (docId == null || docObj == null)
      return ExecuteResult.IGNORED; // nothing to index

    return processInputDoc(docId, docObj);
  }

  /**
   * Process an input document that has already been validated; good place to start for sub-classes to
   * plug-in their own input Tuple processing logic.
   */
  protected ExecuteResult processInputDoc(String docId, Object docObj) {
    // default if not auto-wired
    if (solrInputDocumentMapper == null)
      solrInputDocumentMapper = new DefaultSolrInputDocumentMapper();

    SolrInputDocument doc = solrInputDocumentMapper.toInputDoc(docId, docObj);
    if (doc == null)
      return ExecuteResult.IGNORED; // mapper doesn't want this object indexed

    return bufferDoc(doc);
  }

  protected ExecuteResult bufferDoc(SolrInputDocument doc) {
    if (buffer == null)
      buffer = new ArrayList<SolrInputDocument>(maxBufferSize);

    buffer.add(doc);

    ExecuteResult result = ExecuteResult.BUFFERED;
    if (buffer.size() >= maxBufferSize) {
      result = flushBufferedDocs();
    } else {
      // initial state
      if (bufferTimeoutAtNanos == -1L)
        resetBufferTimeout();

      // see if we've waited too long for more docs
      if (System.nanoTime() >= bufferTimeoutAtNanos)
        result = flushBufferedDocs(); // took too long to see a full buffer worth of docs, so send what we have ...
    }

    return result;
  }

  protected ExecuteResult flushBufferedDocs() {
    int numDocsInBatch = buffer.size();
    Timer.Context timer = (sendBatchToSolr != null) ? sendBatchToSolr.time() : null;
    try {
      sendBatchToSolr(buffer);
    } finally {
      if (timer != null)
        timer.stop();

      if (batchSizeHisto != null)
        batchSizeHisto.update(numDocsInBatch);

      if (indexedCounter != null)
        indexedCounter.inc(numDocsInBatch);

      resetBufferTimeout();
    }

    return ExecuteResult.ACK;
  }

  protected void resetBufferTimeout() {
    bufferTimeoutAtNanos = System.nanoTime() + TimeUnit.NANOSECONDS.convert(bufferTimeoutMs, TimeUnit.MILLISECONDS);
  }

  public int getMaxBufferSize() {
    return maxBufferSize;
  }

  public void setMaxBufferSize(int maxBufferSize) {
    this.maxBufferSize = maxBufferSize;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
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

  public UpdateRequest createUpdateRequest(String collection) {
    UpdateRequest req = new UpdateRequest();
    req.setParam("collection", collection);
    return req;
  }

  protected void sendBatchToSolr(Collection<SolrInputDocument> buffer) {
    if (log.isDebugEnabled())
      log.debug("Sending buffer of " + buffer.size() + " to collection " + collection);

    UpdateRequest req = createUpdateRequest(collection);
    req.add(buffer);
    try {
      updateRequestStrategy.sendUpdateRequest(cloudSolrClient, collection, req);
    } finally {
      buffer.clear();
    }
  }
}
