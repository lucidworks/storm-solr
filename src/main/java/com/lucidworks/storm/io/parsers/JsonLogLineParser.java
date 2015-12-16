package com.lucidworks.storm.io.parsers;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonLogLineParser implements LogLineParser {

  protected ObjectMapper jsonMapper = new ObjectMapper();

  public Map<String, Object> parseLine(String fileName, int lineNum, String line) throws Exception {
    return jsonMapper.readValue(line, Map.class);
  }
}
