package com.lucidworks.storm.solr;

import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DocBuffer {
  public final String collection;
  public final List<SolrInputDocument> buffer;
  public final long bufferTimeoutMs;
  public final int maxBufferSize;

  private long bufferTimeoutAtNanos = -1L;

  public DocBuffer(String collection, int maxBufferSize, long bufferTimeoutMs) {
    this.collection = collection;
    this.maxBufferSize = maxBufferSize;
    this.bufferTimeoutMs = bufferTimeoutMs;
    this.buffer = new ArrayList<SolrInputDocument>(maxBufferSize);
  }

  public void add(SolrInputDocument doc) {
    buffer.add(doc);

    // start the timer when the first doc arrives in this batch
    if (bufferTimeoutAtNanos == -1L)
      bufferTimeoutAtNanos = System.nanoTime() + TimeUnit.NANOSECONDS.convert(bufferTimeoutMs, TimeUnit.MILLISECONDS);
  }

  public void reset() {
    bufferTimeoutAtNanos = -1L;
    buffer.clear();
  }

  public boolean shouldFlushBuffer() {
    if (buffer.isEmpty())
      return false;

    return (buffer.size() >= maxBufferSize) || System.nanoTime() >= bufferTimeoutAtNanos;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DocBuffer: ").append(collection).append(", ").append(buffer.size()).append(", shouldFlush? ").append(shouldFlushBuffer());
    return sb.toString();
  }
}
