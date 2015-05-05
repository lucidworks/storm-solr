package com.lucidworks.storm.example.hdfs

import com.lucidworks.storm.spring.NamedValues
import com.lucidworks.storm.spring.StreamingDataProvider
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.GlobFilter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.Logger

/**
 * Very basic spout implementation that emits URIs of files in an HDFS directory (non-recursive).
 */
class HdfsDirectoryListingDataProvider implements StreamingDataProvider {

  static Logger log = Logger.getLogger(HdfsDirectoryListingDataProvider)

  static String fsDefaultFS = "fs.defaultFS"

  String dirPath
  String globFilter = "*"

  transient FileSystem hdfs
  transient RemoteIterator<FileStatus> fileIterator

  void open(Map stormConf) {
    Configuration hdfsConf = new Configuration()
    stormConf.findAll{it.key instanceof String && (it.key.startsWith("hdfs.") || it.key.startsWith("fs."))}.each{hdfsConf.set(it.key,it.value)}
    if (UserGroupInformation.isSecurityEnabled())
      SecurityUtil.login(hdfsConf, "hdfs.keytab.file", "hdfs.kerberos.principal");
    hdfs = DistributedFileSystem.get(URI.create(dirPath), hdfsConf)
    fileIterator = hdfs.listLocatedStatus(new Path(dirPath), new GlobFilter(globFilter))
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

    record.set("fileUri", nextFile.getPath().toUri())
    return true
  }

}
