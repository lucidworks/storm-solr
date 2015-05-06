package com.lucidworks.storm.example.hdfs

import backtype.storm.task.OutputCollector
import backtype.storm.tuple.Tuple
import com.lucidworks.storm.spring.SpringBolt.ExecuteResult
import com.lucidworks.storm.spring.StreamingDataAction
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired

import java.nio.charset.StandardCharsets

class CsvParserBoltAction implements StreamingDataAction {

  static Logger log = Logger.getLogger(CsvParserBoltAction)

  @Autowired
  HdfsFileSystemProvider hdfsFileSystemProvider

  String[] fields

  String delimiter = ","
  boolean firstRowIsHeader = true

  ExecuteResult execute(Tuple input, OutputCollector collector) {

    URI fileUri = (URI)input.getValue(0)
    String scheme = fileUri.getScheme()

    log.info("Parsing CSV file ${fileUri}, scheme is ${scheme} using delimiter = ${delimiter} and firstRowIsHeader = ${firstRowIsHeader}")

    int idFieldIndex
    int numRecords = 0
    int numSkipped = 0
    BufferedReader reader = null
    try {
      if (scheme == "file") {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fileUri)), StandardCharsets.UTF_8))
      } else {
        FileSystem hdfs = hdfsFileSystemProvider.getFileSystem()
        Path filePath = new Path(fileUri)
        if (hdfs.isFile(filePath))
          reader = new BufferedReader(new InputStreamReader(hdfs.open(filePath), StandardCharsets.UTF_8))
      }

      if (!reader)
        return ExecuteResult.IGNORED

      int rowNum = 0
      String line = null
      while ((line = reader.readLine()) != null) {
        ++rowNum
        line = line.trim()

        if (rowNum == 1 && firstRowIsHeader) {
          fields = line.split(delimiter)
          idFieldIndex = 0
          for (int f=0; f < fields.length; f++) {
            if (fields[f] == "id") {
              idFieldIndex = f
              break
            }
          }
          continue
        }

        if (line.length() == 0)
          continue

        String[] cols = line.split(delimiter)
        if (cols.length != fields.length) {
          ++numSkipped
          continue
        }

        String docId = null
        Map<String,String> row = new HashMap<String,String>()
        for (int i=0; i < cols.length; i++) {
          row.put(fields[i], cols[i])
          if (i == idFieldIndex)
            docId = cols[i]
        }

        if (docId == null)
          continue

        List<Object> tuple = new ArrayList<>()
        tuple.add(docId)
        tuple.add(row)
        collector.emit(tuple)
        ++numRecords
      }

    } finally {
      if (reader)
        reader.close()
    }

    log.info("Parsed ${fileUri}, numRecords: ${numRecords}, numSkipped: ${numSkipped}")

    return ExecuteResult.ACK
  }

  String getDelimiter() {
    return delimiter
  }

  void setDelimiter(String delimiter) {
    this.delimiter = delimiter
  }

  boolean getFirstRowIsHeader() {
    return firstRowIsHeader
  }

  void setFirstRowIsHeader(boolean firstRowIsHeader) {
    this.firstRowIsHeader = firstRowIsHeader
  }

  String[] getFields() {
    return fields
  }

  void setFields(String[] fields) {
    this.fields = fields
  }
}
