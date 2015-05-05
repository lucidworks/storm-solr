package com.lucidworks.storm.hdfs;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class HdfsTestCluster {

  protected final static Log log = LogFactory.getLog(HdfsTestCluster.class);

  protected MiniDFSCluster dfsCluster = null;

  /**
   * Unique cluster name
   */
  protected final String clusterName;

  /**
   * Configuration build at runtime
   */
  protected Configuration configuration;

  /**
   * Monitor sync for start and stop
   */
  protected final Object startupShutdownMonitor = new Object();

  /**
   * Flag for cluster state
   */
  protected boolean started;

  /**
   * Number of nodes for yarn and dfs
   */
  protected int nodes = 1;

  /**
   * Instantiates a mini cluster with default
   * cluster node count.
   *
   * @param clusterName the unique cluster name
   */
  public HdfsTestCluster(String clusterName) {
    this.clusterName = clusterName;
  }

  /**
   * Instantiates a mini cluster with given
   * cluster node count.
   *
   * @param clusterName the unique cluster name
   * @param nodes       the node count
   */
  public HdfsTestCluster(String clusterName, int nodes) {
    this.clusterName = clusterName;
    this.nodes = nodes;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public MiniDFSCluster getDFSCluster() {
    return dfsCluster;
  }

  public void start() throws IOException {
    log.info("Checking if cluster=" + clusterName + " needs to be started");
    synchronized (this.startupShutdownMonitor) {
      if (started) {
        return;
      }
      log.info("Starting cluster=" + clusterName);
      configuration = new Configuration();

      configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "target/" + clusterName + "-dfs");

      dfsCluster = new MiniDFSCluster.Builder(configuration).
        numDataNodes(nodes).
        build();

      log.info("Started cluster=" + clusterName);
      started = true;
    }
  }

  public void stop() {
    log.info("Checking if cluster=" + clusterName + " needs to be stopped");
    synchronized (this.startupShutdownMonitor) {
      if (!started) {
        return;
      }
      if (dfsCluster != null) {
        dfsCluster.shutdown();
        dfsCluster = null;
      }
      log.info("Stopped cluster=" + clusterName);
      started = false;
    }
  }

  /**
   * Sets a number of nodes for cluster. Every node
   * will act as yarn and dfs role. Default is one node.
   *
   * @param nodes the number of nodes
   */
  public void setNodes(int nodes) {
    this.nodes = nodes;
  }

}
