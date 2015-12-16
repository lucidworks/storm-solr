package com.lucidworks.storm.io.parsers;

import java.util.Map;

/**
 * Defines a basic interface to a class that knows how to parse log lines.
 */
public interface LogLineParser {
  /**
   * This method must be thread safe as it will be invoked by many threads concurrently.
   */
  Map<String,Object> parseLine(String fileName, int lineNum, String line) throws Exception;
}
