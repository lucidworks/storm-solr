package com.lucidworks.storm.example.hdfs

import backtype.storm.Config
import backtype.storm.task.IOutputCollector
import backtype.storm.task.OutputCollector
import backtype.storm.tuple.Tuple
import com.lucidworks.storm.StreamingApp
import com.lucidworks.storm.spring.NamedValues
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataOutputStream
import com.lucidworks.storm.hdfs.HdfsTestBase
import org.junit.Test

import java.nio.charset.StandardCharsets

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

class HdfsDirectoryListingDataProviderTest extends HdfsTestBase {

  class TestOutputCollector implements IOutputCollector {

    Map<String,List> emitted = new HashMap<String,List>()

    List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
      String streamKey = (streamId != null) ? streamId : "default"
      List tuplesInStream = emitted.get(streamKey)
      if (tuplesInStream == null) {
        tuplesInStream = new ArrayList()
        emitted.put(streamKey, tuplesInStream)
      }
      tuplesInStream.add(tuple)
      return null
    }

    void emitDirect(int i, String s, Collection<Tuple> collection, List<Object> list) {}
    void ack(Tuple tuple) {}
    void fail(Tuple tuple) {}
    void reportError(Throwable throwable) {}
  }

  @Test
  void testDataProvider() {

    // Get the HDFS FileSystem (setup by the parent class)
    FileSystem hdfs = getFileSystem()

    Config stormConf = StreamingApp.getConfig(StreamingApp.ENV.test.toString())
    stormConf.put("storm.id", "unit-test")

    Path testDirPath = new Path("test")
    assertTrue("Failed to make ${testDirPath} in mini HDFS cluster!", hdfs.mkdirs(testDirPath))

    String testDataFile = "books.csv"
    InputStream input = getClass().getClassLoader().getResourceAsStream(testDataFile)
    assertNotNull("Test file ${testDataFile} not found on classpath!", input)

    Path testCsvFile = new Path(testDirPath, testDataFile)
    int numLines = writeUtf8ResourceToHdfs(input, hdfs.create(testCsvFile, true))
    assertTrue("${testCsvFile} not found in mini HDFS cluster!", hdfs.exists(testCsvFile))

    Path qualifiedCsvFilePath = testCsvFile.makeQualified(hdfs)

    Map<String,String> hdfsConfig = new HashMap<String,String>()
    hdfsConfig.put(HdfsFileSystemProvider.fsDefaultFS, hdfs.getCanonicalUri().toString())
    HdfsFileSystemProvider fsProvider = new HdfsFileSystemProvider()
    fsProvider.setHdfsConfig(hdfsConfig)

    HdfsDirectoryListingDataProvider provider = new HdfsDirectoryListingDataProvider()
    provider.hdfsFileSystemProvider = fsProvider
    provider.setDirPath(testDirPath.toUri().toString())
    provider.setGlobFilter("*.csv")
    provider.open(stormConf)

    NamedValues record = new NamedValues(HdfsDirectoryToSolrTopology.spoutFields)
    assertTrue "Expected 1 record output from ${provider.class.simpleName}", provider.next(record)

    List<Object> values = record.values()
    assertNotNull values
    assertTrue values.size() == HdfsDirectoryToSolrTopology.spoutFields.size()
    assertNotNull values.get(0)
    URI outputUri = values.get(0)
    assertEquals "Expected ${qualifiedCsvFilePath} as output from the spout, but got ${outputUri} instead!",
            qualifiedCsvFilePath.toUri(), outputUri

    record.resetValues()
    assertTrue !provider.next(record)

    CsvParserBoltAction csvBolt = new CsvParserBoltAction()
    csvBolt.hdfsFileSystemProvider = fsProvider

    Tuple mockTuple = mock(Tuple.class);
    when(mockTuple.size()).thenReturn(1);
    when(mockTuple.getValue(0)).thenReturn(outputUri);

    TestOutputCollector collector = new TestOutputCollector()
    csvBolt.execute(mockTuple, new OutputCollector(collector))

    List defaultStream = collector.emitted.get("default")
    assertTrue defaultStream.size() == numLines-1
  }

  protected int writeUtf8ResourceToHdfs(InputStream resource, FSDataOutputStream out) {
    BufferedReader br = null
    String line = null
    int numLines = 0
    try {
      br = new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8))
      while ((line = br.readLine()) != null) {
        line = line.trim()
        if (line.length() > 0) {
          line += "\n"
          byte[] bytes = line.getBytes(StandardCharsets.UTF_8)
          out.write(bytes, 0, bytes.length)
          ++numLines
        }
      }
      out.flush()
    } finally {
      if (br != null)
        br.close()
      out.close()
    }
    return numLines
  }
}
