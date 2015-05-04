package com.lucidworks.storm.spring;

/**
 * Interface to be implemented by any action bean that wishes to be
 * notified when tick tuples are sent through the message stream.
 */
public interface TickTupleAware {
  SpringBolt.ExecuteResult onTick();
}
