package com.lucidworks.storm.spring;

import java.util.HashMap;
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

  protected String boltLogicBeanId;
  protected Fields outputFields;
  protected int tickRate = -1;

  private boolean isTickTupleAware = false;
  private transient StreamingDataAction delegate;
  private transient OutputCollector collector;
  private transient Map stormConf;

  public static final boolean isTickTuple(final Tuple tuple) {
    return tuple != null &&
           Constants.SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent()) &&
           Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
  }

  public SpringBolt(String boltBeanId) {
    this(boltBeanId, null, -1);
  }

  public SpringBolt(String boltBeanId, int tickRate) {
    this(boltBeanId, null, tickRate);
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
  }

  public void execute(Tuple input) {
    try {
      if (isTickTuple(input)) {
        if (isTickTupleAware)
          ((TickTupleAware)delegate).onTick();
      } else {
        delegate.execute(input);
      }
      collector.ack(input);
    } catch (Throwable exc) {
      collector.reportError(exc);
      collector.fail(input);
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
}
