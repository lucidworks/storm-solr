package com.lucidworks.storm.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public abstract class HdfsTestBase {

  protected HdfsTestCluster testCluster;

  @Before
  public void setup() throws Exception {
    setupHdfsTestCluster();
  }

  @After
  public void tearDown() throws IOException {
    tearDownHdfsTestCluster();
  }

  protected FileSystem getFileSystem() throws IOException {
    return testCluster.getDFSCluster().getFileSystem();
  }

  protected void setupHdfsTestCluster() {
    int numNodes = getNumTestNodes();
    testCluster = new HdfsTestCluster(getClass().getSimpleName(), getNumTestNodes());
    try {

      testCluster.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start Hdfs test cluster due to: "+e, e);
    }
  }

  protected int getNumTestNodes() {
    return 1;
  }

  protected void tearDownHdfsTestCluster() {
    testCluster.stop();
  }

}