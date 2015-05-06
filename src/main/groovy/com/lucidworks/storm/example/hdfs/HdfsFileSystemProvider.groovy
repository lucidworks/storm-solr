package com.lucidworks.storm.example.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation

class HdfsFileSystemProvider {

  static String fsDefaultFS = "fs.defaultFS"

  private Map<String,String> hdfsConfig
  private FileSystem hdfs
  
  FileSystem getFileSystem() {
    if (!hdfs) {
      Configuration hdfsConf = new Configuration()
      hdfsConfig.each{ hdfsConf.set(it.key, it.value) }
      if (UserGroupInformation.isSecurityEnabled())
        SecurityUtil.login(hdfsConf, "hdfs.keytab.file", "hdfs.kerberos.principal");
      hdfs = DistributedFileSystem.get(hdfsConf)
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
