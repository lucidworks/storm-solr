package com.lucidworks.storm.example.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.Logger

class HdfsFileSystemProvider {

  static String fsDefaultFS = "fs.defaultFS"

  static Logger log = Logger.getLogger(HdfsFileSystemProvider)

  private Map<String,String> hdfsConfig
  private FileSystem hdfs
  
  FileSystem getFileSystem() {
    if (!hdfs) {
      Configuration hdfsConf = new Configuration()
      hdfsConfig.each{ hdfsConf.set(it.key, it.value) }
      UserGroupInformation.setConfiguration(hdfsConf)
      if (UserGroupInformation.isSecurityEnabled()) {
        log.debug("Using a kerberized hdfs instance")
        SecurityUtil.login(hdfsConf, "hdfs.keytab.file", "hdfs.kerberos.principal")
      }
      hdfs = FileSystem.get(hdfsConf)
    }
    return hdfs
  }

  Map<String, String> getHdfsConfig() {
    return hdfsConfig
  }

  void setHdfsConfig(Map<String, String> hdfsConfig) {
    this.hdfsConfig = hdfsConfig
  }
}
