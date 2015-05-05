package com.lucidworks.storm.example.hdfs

import backtype.storm.Config
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

class HdfsDirectoryListingDataProviderTest extends HdfsTestBase {

  @Test
  void testDataProvider() {

    // Get the HDFS FileSystem (setup by the parent class)
    FileSystem hdfs = getFileSystem()

    Config stormConf = StreamingApp.getConfig(StreamingApp.ENV.test.toString())
    stormConf.put("storm.id", "unit-test")
    // the bolt action needs this to bootstrap the Hadoop FileSystem
    stormConf.put(HdfsDirectoryListingDataProvider.fsDefaultFS, hdfs.getCanonicalUri().toString())

    Path testDirPath = new Path("test")
    assertTrue("Failed to make ${testDirPath} in mini HDFS cluster!", hdfs.mkdirs(testDirPath))

    String testDataFile = "books.csv"
    InputStream input = getClass().getClassLoader().getResourceAsStream(testDataFile)
    assertNotNull("Test file ${testDataFile} not found on classpath!", input)

    Path testCsvFile = new Path(testDirPath, testDataFile)
    writeUtf8ResourceToHdfs(input, hdfs.create(testCsvFile, true))
    assertTrue("${testCsvFile} not found in mini HDFS cluster!", hdfs.exists(testCsvFile))

    Path qualifiedCsvFilePath = testCsvFile.makeQualified(hdfs);

    HdfsDirectoryListingDataProvider provider = new HdfsDirectoryListingDataProvider()
    provider.setDirPath(testDirPath.toUri().toString())
    provider.setGlobFilter("*.csv")
    provider.open(stormConf)

    NamedValues record = new NamedValues(HdfsDirectoryToSolrTopology.spoutFields)
    assertTrue "Expected 1 record output from ${provider.class.simpleName}", provider.next(record)

    List<Object> values = record.values()
    assertNotNull values
    assertTrue values.size() == HdfsDirectoryToSolrTopology.spoutFields.size()
    assertNotNull values.get(0)
    String outputPath = values.get(0)
    assertEquals "Expected ${qualifiedCsvFilePath} as output from the spout, but got ${outputPath} instead!",
            qualifiedCsvFilePath.toString(), outputPath

    record.resetValues()
    assertTrue !provider.next(record)
  }

  protected void writeUtf8ResourceToHdfs(InputStream resource, FSDataOutputStream out) {
    BufferedReader br = null
    String line = null
    String NEWLINE = "\n"
    try {
      br = new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8))
      while ((line = br.readLine()) != null) {
        line = line.trim()
        if (line.length() > 0) {
          out.writeUTF(line)
          out.writeUTF(NEWLINE)
        }
      }
      out.flush()
    } finally {
      if (br != null)
        br.close()
      out.close()
    }
  }
}
