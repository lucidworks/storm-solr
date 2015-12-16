package com.lucidworks.storm.io;

import com.codahale.metrics.Counter;
import com.lucidworks.storm.io.parsers.LogLineParser;
import com.lucidworks.storm.io.parsers.NoOpLogLineParser;
import com.lucidworks.storm.spring.NamedValues;
import com.lucidworks.storm.spring.StreamingDataProvider;

import java.io.File;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.ryantenney.metrics.annotation.Metric;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.log4j.Logger;

public class FileTailingDataProvider extends TailerListenerAdapter implements StreamingDataProvider {

  public static final Logger log = Logger.getLogger(FileTailingDataProvider.class);

  @Metric
  public Counter linesOutput;

  @Metric
  public Counter linesRead;

  protected File fileToParse;
  protected String fileToParseName;
  protected long tailerDelayMs = 500;
  protected Tailer tailer;
  protected int maxQueueSize = 100000;
  protected BlockingQueue<Map> parsedLineQueue;
  protected LogLineParser lineParser;
  protected int pollQueueTimeMs = 50;
  protected String idFieldName = "id";

  protected int currentLine;

  public FileTailingDataProvider(File fileToParse) {
    if (fileToParse == null)
      throw new IllegalStateException("Must specify which file to tail!");

    if (!fileToParse.isFile())
      throw new IllegalStateException(fileToParse.getAbsolutePath()+" not found!");

    this.fileToParse = fileToParse;
  }

  public void open(Map stormConf) {
    fileToParseName = fileToParse.getAbsolutePath();
    parsedLineQueue = new LinkedBlockingQueue<Map>(maxQueueSize);
    currentLine = 0;

    if (lineParser == null)
      lineParser = new NoOpLogLineParser();

    tailer = Tailer.create(fileToParse, this, tailerDelayMs);
    log.info("Started tailing "+fileToParseName);
  }

  public void handle(String line) {
    ++currentLine;

    if (line == null || line.isEmpty())
      return;

    try {
      handleLine(line);
    } catch (Exception exc) {
      log.error("Failed to parse line "+currentLine+" in "+
          fileToParse.getAbsolutePath()+" due to: "+exc+"; line=["+line+"]", exc);
    }
  }

  protected void handleLine(String line) throws Exception {
    if (linesRead != null)
      linesRead.inc();

    Map parsedLine = lineParser.parseLine(fileToParseName, currentLine, line);
    if (parsedLine != null) {
      if (parsedLine.get(idFieldName) == null) {
        parsedLine.put(idFieldName, String.format("%s-%d", fileToParseName, currentLine));
      }
      parsedLineQueue.put(parsedLine); // must block until queue space becomes available ...
    }
  }

  public boolean next(NamedValues record) throws Exception {
    Map nextLine = parsedLineQueue.poll(pollQueueTimeMs, TimeUnit.MILLISECONDS);
    if (nextLine != null) {
      record.set("id", (String)nextLine.get(idFieldName));
      record.set("line", nextLine);

      if (linesOutput != null)
        linesOutput.inc();

      return true;
    }
    return false;
  }

  public long getTailerDelayMs() {
    return tailerDelayMs;
  }

  public void setTailerDelayMs(long tailerDelayMs) {
    this.tailerDelayMs = tailerDelayMs;
  }

  public int getMaxQueueSize() {
    return maxQueueSize;
  }

  public void setMaxQueueSize(int maxQueueSize) {
    this.maxQueueSize = maxQueueSize;
  }

  public LogLineParser getLineParser() {
    return lineParser;
  }

  public void setLineParser(LogLineParser lineParser) {
    this.lineParser = lineParser;
  }

  public int getPollQueueTimeMs() {
    return pollQueueTimeMs;
  }

  public void setPollQueueTimeMs(int pollQueueTimeMs) {
    this.pollQueueTimeMs = pollQueueTimeMs;
  }
}
