package com.lucidworks.storm.io.parsers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Just stuffs the entire line into the _raw_content_ field, which can be parsed on the Fusion side.
 */
public class NoOpLogLineParser implements LogLineParser {

  public Map<String, Object> parseLine(String fileName, int lineNum, String line) throws Exception {
    Map<String,Object> doc = new HashMap<String,Object>(3);
    doc.put("id", String.format("%s:%d", fileName, lineNum));
    doc.put("_raw_content_", line);
    return doc;
  }
}
