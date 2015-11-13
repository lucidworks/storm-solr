package com.lucidworks.storm.solr;

import com.codahale.metrics.Counter;
import com.ryantenney.metrics.annotation.Metric;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;

import java.net.ConnectException;
import java.net.SocketException;

/**
 * Bean that sends an update request to Solr with tunable retry support.
 * The default implementation should be sufficient for most purposes but
 * you can inject a different implementation using Spring.
 */
public class DefaultUpdateRequestStrategy implements SolrUpdateRequestStrategy {

  public static Logger log = Logger.getLogger(DefaultUpdateRequestStrategy.class);

  @Metric
  public Counter okRequests;

  @Metric
  public Counter retriedRequests;

  @Metric
  public Counter failedRequests;

  protected int waitSecsBetweenRetries = 5;
  protected int maxRetryAttempts = 1;

  public NamedList<Object> sendUpdateRequest(SolrClient solrClient, String collection, SolrRequest req) {
    return sendUpdateRequestWithRetry(solrClient, collection, req, maxRetryAttempts);
  }

  protected NamedList<Object> sendUpdateRequestWithRetry(SolrClient solrClient, String collection, SolrRequest req, int remainingRetryAttempts) {
    NamedList<Object> resp = null;
    try {
      resp = solrClient.request(req);
      if (okRequests != null)
        okRequests.inc();

    } catch (Exception e) {
      if (remainingRetryAttempts <= 0) {
        log.error("Send update request to "+collection+" failed due to " + e +
          "; max number of re-try attempts "+maxRetryAttempts+" reached, no more attempts available, request fails!");

        if (failedRequests != null)
          failedRequests.inc();

        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new RuntimeException(e);
        }
      }

      if (shouldRetry(e)) {
        log.error("Send update request to "+collection+" failed due to " + e +
          "; will retry after waiting "+waitSecsBetweenRetries+" secs");
        try {
          Thread.sleep(waitSecsBetweenRetries * 1000L);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        try {
          sendUpdateRequestWithRetry(solrClient, collection, req, --remainingRetryAttempts);
        } finally {
          if (retriedRequests != null)
            retriedRequests.inc();
        }
      } else {
        log.error("Send update request to collection " + collection + " failed due to: " + e+
          ", which cannot be retried, request fails!", e);

        if (failedRequests != null)
          failedRequests.inc();

        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new RuntimeException(e);
        }
      }
    }
    return resp;
  }

  public int getWaitSecsBetweenRetries() {
    return waitSecsBetweenRetries;
  }

  public void setWaitSecsBetweenRetries(int waitSecsBetweenRetries) {
    this.waitSecsBetweenRetries = waitSecsBetweenRetries;
  }

  public int getMaxRetryAttempts() {
    return maxRetryAttempts;
  }

  public void setMaxRetryAttempts(int maxRetryAttempts) {
    this.maxRetryAttempts = maxRetryAttempts;
  }

  protected boolean shouldRetry(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    return (rootCause instanceof ConnectException || rootCause instanceof SocketException);
  }
}
