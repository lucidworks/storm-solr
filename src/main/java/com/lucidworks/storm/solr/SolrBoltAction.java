package com.lucidworks.storm.solr;

import static com.lucidworks.storm.spring.SpringBolt.ExecuteResult;

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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.ConnectException;
import java.net.SocketException;
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

  @Autowired
  public CloudSolrClient cloudSolrClient;

  @Autowired
  public SolrInputDocumentMapper solrInputDocumentMapper;

  @Metric
  public Timer sendBatchToSolr;

  @Metric
  public Counter indexedCounter;

  @Metric
  public Histogram batchSizeHisto;

  @Metric
  public Counter retriedBatches;

  @Metric
  public Counter failedBatches;

  protected String collection; // defaults to the default set on the cloud client
  protected int batchSize = 100; // avoids sending 100's of requests per second to Solr in high-throughput envs
  protected List<SolrInputDocument> batch;
  protected long bufferTimeoutMs = 500L;

  private long bufferTimeoutAtNanos = -1L;

  public ExecuteResult onTick() {
    // this catches the case where we have buffered docs, but don't see any more docs flowing in for a while
    if (!batch.isEmpty() && (bufferTimeoutAtNanos < 0 || System.nanoTime() >= bufferTimeoutAtNanos))
      return flushBufferedDocs();

    return ExecuteResult.IGNORED;
  }

  public ExecuteResult execute(Tuple input) {

    if (collection == null) {
      collection = cloudSolrClient.getDefaultCollection();
      if (collection == null)
        throw new IllegalStateException("Collection name not configured for the Solr bolt!");

      String mapper = (solrInputDocumentMapper != null ? solrInputDocumentMapper.getClass().getSimpleName()
        : DefaultSolrInputDocumentMapper.class.getSimpleName());
      log.info("Configured to send docs to "+collection+
        " using mapper="+mapper+", batchSize="+batchSize+", and bufferTimeoutMs="+bufferTimeoutMs);
    }

    String docId = input.getString(0);
    Object docObj = input.getValue(1);
    if (docId == null || docObj == null)
      return ExecuteResult.IGNORED; // nothing to index

    // default if not auto-wired
    if (solrInputDocumentMapper == null)
      solrInputDocumentMapper = new DefaultSolrInputDocumentMapper();

    SolrInputDocument doc = solrInputDocumentMapper.toInputDoc(docId, docObj);
    if (doc == null)
      return ExecuteResult.IGNORED; // mapper doesn't want this object indexed

    return bufferDoc(doc);
  }

  protected ExecuteResult bufferDoc(SolrInputDocument doc) {
    if (batch == null)
      batch = new ArrayList<SolrInputDocument>(batchSize);

    batch.add(doc);

    ExecuteResult result = ExecuteResult.BUFFERED;
    if (batch.size() >= batchSize) {
      result = flushBufferedDocs();
    } else {
      // initial state
      if (bufferTimeoutAtNanos == -1L)
        resetBufferTimeout();

      // see if we've waited too long for more docs
      if (System.nanoTime() >= bufferTimeoutAtNanos)
        result = flushBufferedDocs(); // took too long to see a full batch worth of docs, so send what we have ...
    }

    return result;
  }

  protected ExecuteResult flushBufferedDocs() {
    int numDocsInBatch = batch.size();
    Timer.Context timer = (sendBatchToSolr != null) ? sendBatchToSolr.time() : null;
    try {
      sendBatchToSolr(batch);
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

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
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

  protected void sendBatchToSolr(Collection<SolrInputDocument> batch) {
    UpdateRequest req = solrInputDocumentMapper.createUpdateRequest(collection);

    if (log.isDebugEnabled())
      log.debug("Sending batch of " + batch.size() + " to collection " + collection);

    req.add(batch);
    try {
      cloudSolrClient.request(req);
    } catch (Exception e) {
      if (shouldRetry(e)) {
        log.error("Send batch to collection "+collection+" failed due to "+e+"; will retry ...");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        try {
          cloudSolrClient.request(req);
        } catch (Exception e1) {
          log.error("Retry send batch to collection "+collection+" failed due to: "+e1, e1);
          if (e1 instanceof RuntimeException) {
            throw (RuntimeException)e1;
          } else {
            throw new RuntimeException(e1);
          }
        } finally {
          if (retriedBatches != null) {
            retriedBatches.inc();
          }
        }
      } else {
        log.error("Send batch to collection "+collection+" failed due to: "+e, e);

        if (failedBatches != null) {
          failedBatches.inc();
        }

        if (e instanceof RuntimeException) {
          throw (RuntimeException)e;
        } else {
          throw new RuntimeException(e);
        }
      }
    } finally {
      batch.clear();
    }
  }

  private boolean shouldRetry(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    return (rootCause instanceof ConnectException || rootCause instanceof SocketException);
  }
}
