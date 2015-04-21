package com.lucidworks.storm.spring;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.lucidworks.storm.StreamingApp;
import org.apache.log4j.Logger;

/**
 * A spout that delegates to a Spring-managed SpoutDataProvider POJO to produce tuples.
 */
public class SpringSpout extends BaseRichSpout {

  private static final Logger log = Logger.getLogger(SpringSpout.class);

  private String spoutLogicBeanId;
  private Fields outputFields;
  private NamedValues reusableNamedValues;
  private int maxPendingMessages;

  private transient Map stormConf;
  private transient SpoutOutputCollector _collector;
  private transient TopologyContext topologyContext;
  private transient StreamingDataProvider dataProvider;
  private transient AtomicLong messageIdCounter;
  private transient Set<Object> pendingMessages;

  private int emitted = 0;
  private int acked = 0;

  public SpringSpout(String spoutLogicBeanId, Fields outputFields) {
    this.spoutLogicBeanId = spoutLogicBeanId;
    this.outputFields = outputFields;
    this.reusableNamedValues = new NamedValues(outputFields);
  }

  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this.stormConf = map;
    this.topologyContext = topologyContext;
    this._collector = spoutOutputCollector;
    getSpoutDataProvider();
    dataProvider.open(stormConf); // this should only happen once

    messageIdCounter = new AtomicLong();
    pendingMessages = new HashSet<Object>();

    maxPendingMessages = (stormConf.get("maxPendingMessages") != null ? ((Long) stormConf.get("maxPendingMessages")).intValue() : -1);
  }

  protected StreamingDataProvider getSpoutDataProvider() {
    if (dataProvider == null)
      dataProvider = (StreamingDataProvider) StreamingApp.spring(stormConf).getBean(spoutLogicBeanId);
    return dataProvider;
  }

  public void nextTuple() {

    if (maxPendingMessages > 0 && pendingMessages.size() >= maxPendingMessages) {
      if (log.isDebugEnabled()) {
        log.debug(String.format("Too many pending messages (%d), waiting until there are less than %d before emitting more.",
          pendingMessages.size(), maxPendingMessages));
      }

      Utils.sleep(50);
      return; // too many messages in-flight, don't provide anymore until we catch up
    }

    boolean hasNext = false;
    try {
      hasNext = dataProvider.next(reusableNamedValues);
    } catch (Exception exc) {
      log.error(String.format("Failed to get next record from %s due to: %s", spoutLogicBeanId, exc.toString()));
    }

    if (!hasNext) {
      Utils.sleep(50);
      return;
    }

    String messageId = reusableNamedValues.getMessageId();
    if (messageId == null) {
      messageId = String.valueOf(messageIdCounter.incrementAndGet());
    }
    _collector.emit(reusableNamedValues.values(), messageId);

    if (++emitted % 1000 == 0) {
      log.debug("emitted: " + emitted);
    }

    if (maxPendingMessages > 0) {
      pendingMessages.add(messageId); // keep track of this message as being "pending"
    }
  }

  @Override
  public void ack(Object id) {
    if (++acked % 1000 == 0) {
      log.debug("acked: " + acked);
    }

    pendingMessages.remove(id);
  }

  @Override
  public void fail(Object id) {
    pendingMessages.remove(id);
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    if (outputFields != null && outputFields.size() > 0)
      outputFieldsDeclarer.declare(outputFields);
  }
}
