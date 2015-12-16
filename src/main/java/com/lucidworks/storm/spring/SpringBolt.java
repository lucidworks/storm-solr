package com.lucidworks.storm.spring;

import java.io.Closeable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.lucidworks.storm.StreamingApp;
import org.apache.log4j.Logger;

/**
 * Executes a Spring-managed BoltAction implementation.
 */
public class SpringBolt extends BaseRichBolt {

  private static final Logger log = Logger.getLogger(SpringBolt.class);

  public static final boolean isTickTuple(final Tuple tuple) {
    return tuple != null &&
      Constants.SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent()) &&
      Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
  }

  public static enum ExecuteResult {
    ACK, BUFFERED, IGNORED
  }

  protected String boltLogicBeanId;
  protected Fields outputFields;
  protected int tickRate = -1;

  private boolean isTickTupleAware = false;
  private transient StreamingDataAction delegate;
  private transient OutputCollector collector;
  private transient Map stormConf;

  // bolt action impls may employ a small buffer to avoid sending too many requests to an external system
  // but we don't want to ack tuples until the action verifies the buffer was successfully sent
  private transient LinkedList<Tuple> bufferedTuples;

  public SpringBolt(String boltBeanId) {
    this(boltBeanId, null, -1);
  }

  public SpringBolt(String boltBeanId, int tickRate) {
    this(boltBeanId, null, tickRate);
  }

  public SpringBolt(String boltBeanId, Fields outputFields) {
    this(boltBeanId, outputFields, -1);
  }

  public SpringBolt(String boltBeanId, Fields outputFields, int tickRate) {
    this.boltLogicBeanId = boltBeanId;
    this.outputFields = outputFields;
    this.tickRate = tickRate;
  }

  public int getTickRate() {
    return tickRate;
  }

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.stormConf = map;
    this.collector = outputCollector;
    getStreamingDataActionBean();
    bufferedTuples = new LinkedList<Tuple>();
  }

  public void execute(Tuple input) {
    try {
      ExecuteResult result = ExecuteResult.IGNORED;
      if (isTickTuple(input)) {
        if (isTickTupleAware)
          result = ((TickTupleAware) delegate).onTick();
      } else {
        result = delegate.execute(input, collector);
      }

      if (result == ExecuteResult.IGNORED) {
        // bolt action ignored this tuple, so we just ack and keep processing
        collector.ack(input);
        return;
      }

      bufferedTuples.add(input);

      if (result == ExecuteResult.ACK) {

        // ack the current tuple and all buffered tuples
        try {
          for (Tuple buffered : bufferedTuples)
            collector.ack(buffered);
        } finally {
          bufferedTuples.clear();
        }
      }
    } catch (Throwable exc) {
      collector.reportError(exc);

      bufferedTuples.add(input);
      try {
        for (Tuple buffered : bufferedTuples)
          collector.fail(buffered);
      } finally {
        bufferedTuples.clear();
      }
    }
  }

  protected StreamingDataAction getStreamingDataActionBean() {
    if (delegate == null) {
      // Get the Bolt Logic POJO from Spring
      delegate = (StreamingDataAction) StreamingApp.spring(stormConf).getBean(boltLogicBeanId);
      isTickTupleAware = (delegate instanceof TickTupleAware);
    }
    return delegate;
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    if (outputFields != null && outputFields.size() > 0)
      outputFieldsDeclarer.declare(outputFields);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    if (tickRate > 0)
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickRate);
    return conf;
  }

  @Override
  public void cleanup() {
    StreamingDataAction sda = getStreamingDataActionBean();
    if (sda instanceof Closeable) {
      try {
        ((Closeable) sda).close();
      } catch (Exception ignore) {
        log.warn("Error when trying to close StreamingDataAction due to: "+ignore);
      }
    }
  }
}
