package com.lucidworks.storm.example.hdfs

import com.lucidworks.storm.spring.NamedValues
import com.lucidworks.storm.spring.StreamingDataProvider
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.GlobFilter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired

/**
 * Very basic spout implementation that emits URIs of files in an HDFS directory (non-recursive).
 */
class HdfsDirectoryListingDataProvider implements StreamingDataProvider {

  static Logger log = Logger.getLogger(HdfsDirectoryListingDataProvider)

  String dirPath
  String globFilter = "*"

  @Autowired
  HdfsFileSystemProvider hdfsFileSystemProvider

  private RemoteIterator<FileStatus> fileIterator

  void open(Map stormConf) {
    log.info("Opening FileStatus iterator for path ${dirPath} using filter ${globFilter}")
    fileIterator =
      hdfsFileSystemProvider.getFileSystem().listLocatedStatus(new Path(dirPath), new GlobFilter(globFilter))
  }

  boolean next(NamedValues record) throws Exception {
    if (fileIterator == null)
      return false

    FileStatus nextFile = null
    while (fileIterator.hasNext()) {
      FileStatus next = fileIterator.next()
      if (next.isFile()) {
        nextFile = next
        break
      }
    }

    if (nextFile == null)
      return false

    URI fileUri = nextFile.getPath().toUri()
    log.info("Emitting ${fileUri}")
    record.set("fileUri", fileUri)

    return true
  }
}
