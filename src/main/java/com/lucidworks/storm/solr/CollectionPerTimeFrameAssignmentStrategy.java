package com.lucidworks.storm.solr;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.StrUtils;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Assigns documents to time-series collections based on the document timestamp;
 * creates the collections on-the-fly as needed. Uses a ZK-based distributed lock to
 * ensure multiple bolt instances don't try to create the same collection concurrently.
 */
public class CollectionPerTimeFrameAssignmentStrategy implements DocumentAssignmentStrategy, Closeable {

  public static final Logger log = Logger.getLogger(CollectionPerTimeFrameAssignmentStrategy.class);

  protected String fieldName;
  protected int timeFrame = 1;
  protected TimeUnit timeUnit = TimeUnit.DAYS;
  protected String dateTimePattern = "yyyy-MM-dd";
  protected String collectionNameBase;
  protected String timezoneId = "UTC";
  protected Date startDate = null;

  protected String alias;
  protected int maxCollectionsInAlias = -1; // keep them all
  protected String configName;
  protected int numShards = 1;
  protected int replicationFactor = 1;
  protected CuratorFramework curatorClient;
  protected String lockZnodePath = "/create-collection-locks";

  private ThreadLocal<SimpleDateFormat> dateFormatter = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat sdf = new SimpleDateFormat(getDateTimePattern());
      sdf.setTimeZone(TimeZone.getTimeZone(getTimezoneId()));
      return sdf;
    }
  };

  public void close() throws IOException {
    if (curatorClient != null) {
      CloseableUtils.closeQuietly(curatorClient);
    }
  }

  public String getCollectionForDoc(CloudSolrClient cloudSolrClient, SolrInputDocument doc) throws Exception {
    Object obj = doc.getFieldValue(fieldName);
    if (obj == null)
      throw new IllegalArgumentException("Document " + doc + " cannot be routed because " + fieldName + " is null!");

    Date timestamp = null;
    try {
      timestamp = asTimestamp(obj);
    } catch (ParseException pe) {
      throw new IllegalArgumentException("Cannot parse "+obj+" for "+fieldName+" into a java.util.Date due to: "+pe);
    }

    Date startFrom = null;
    // if they use a multi-day time frame, then we need a start date from which to start calculating offsets
    if (timeUnit.equals(TimeUnit.DAYS) && timeFrame > 1) {
      if (this.startDate == null) {
        throw new IllegalStateException("Must specify a start date for multi-day time frames!");
      }
      startFrom = this.startDate;
    } else {
      // start back at the beginning of the current day and increment from there until we find the correct "bucket"
      Calendar cal = Calendar.getInstance();
      cal.setTimeZone(TimeZone.getTimeZone(timezoneId));
      cal.setTimeInMillis(timestamp.getTime());
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, 0);
      startFrom = cal.getTime();
    }

    // increment by the length of each timeframe (in millis) until we are past the timestamp,
    // the resulting value of prev will determine the collection
    Date next = startFrom;
    long prev = next.getTime();
    final long timeFrameMs = TimeUnit.MILLISECONDS.convert(timeFrame, timeUnit);
    while (timestamp.after(next)) {
      prev = next.getTime();
      next.setTime(prev + timeFrameMs);
    }

    String collection = collectionNameBase+dateFormatter.get().format(new Date(prev));
    checkCollectionExists(cloudSolrClient, collection);
    return collection;
  }

  protected void checkCollectionExists(CloudSolrClient cloudSolrClient, String collection) throws Exception {
    if (!cloudSolrClient.getZkStateReader().getClusterState().hasCollection(collection)) {

      synchronized (this) {
        if (curatorClient == null) {
          curatorClient = CuratorFrameworkFactory.newClient(cloudSolrClient.getZkHost(), new ExponentialBackoffRetry(1000, 3));
          curatorClient.start();
        }
      }

      String lockPath = lockZnodePath+"/"+collection;
      InterProcessMutex lock = new InterProcessMutex(curatorClient, lockPath);
      try {
        if (!lock.acquire(60, TimeUnit.SECONDS)) {
          // couldn't acquire the lock, but let's check to see if the collection was created before failing
          cloudSolrClient.getZkStateReader().updateClusterState();
          if (!cloudSolrClient.getZkStateReader().getClusterState().hasCollection(collection)) {
            throw new IllegalStateException("Failed to acquire the create collection lock within 60 seconds! Cannot create "+collection);
          }
        }

        // we have the lock here ...
        cloudSolrClient.getZkStateReader().updateClusterState();
        if (!cloudSolrClient.getZkStateReader().getClusterState().hasCollection(collection)) {
          log.info("Acquired inter-process lock for creating " + collection);

          // ok, it doesn't exist ... go ahead and create it
          long startMs = System.currentTimeMillis();
          createCollection(cloudSolrClient, collection);
          log.info("Collection created, took "+(System.currentTimeMillis()-startMs)+" ms ... updating alias: "+alias);

          // add the new collection to the collection alias if one is registered
          if (alias != null) {
            List<String> aliasList = getAliasList(cloudSolrClient, alias);
            if (!aliasList.contains(collection)) {
              aliasList.add(collection);
              log.info("Added " + collection + " to the " + alias + " alias");

              // trim the alias down to the desired size
              int numColls = aliasList.size();
              if (maxCollectionsInAlias > 0 && numColls > maxCollectionsInAlias) {
                Collections.sort(aliasList);

                int numToRemove = numColls - maxCollectionsInAlias;
                aliasList = aliasList.subList(numToRemove, numColls);
                log.info("Removed "+numToRemove+" collections from alias: "+aliasList);
              }

              CollectionAdminRequest.CreateAlias createAliasCmd = new CollectionAdminRequest.CreateAlias();
              createAliasCmd.setAliasName(alias);
              createAliasCmd.setAliasedCollections(StrUtils.join(aliasList, ','));
              cloudSolrClient.request(createAliasCmd);
            }
          }
        } else {
          log.info("Collection "+collection+" was created by another process while we were waiting to acquire the lock ...");
        }
      } finally {
        lock.release();
      }
    }
  }

  protected List<String> getAliasList(CloudSolrClient cloudSolrClient, String collectionAlias) {
    ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
    Aliases aliases = zkStateReader.getAliases();
    String collectionsInAlias = aliases.getCollectionAlias(collectionAlias);
    log.info("Looked up collection list "+collectionsInAlias+" for collection collectionsInAlias: "+collectionAlias);
    return (collectionsInAlias != null) ? StrUtils.splitSmart(collectionsInAlias, ",", true) : new ArrayList<String>(0);
  }

  protected void createCollection(CloudSolrClient cloudSolrClient, String collection) throws Exception {
    CollectionAdminRequest.Create createCmd = new CollectionAdminRequest.Create();
    createCmd.setCollectionName(collection);
    createCmd.setNumShards(numShards);
    createCmd.setConfigName(configName);
    createCmd.setReplicationFactor(replicationFactor);
    int liveNodes = cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes().size();
    int maxShardsPerNode = (int) Math.max(Math.ceil((numShards * replicationFactor) / liveNodes), 1);
    createCmd.setMaxShardsPerNode(maxShardsPerNode);
    log.info("Creating new collection " + collection + " with " + numShards + " shards and " + replicationFactor + " replicas per shard");
    try {
      cloudSolrClient.request(createCmd);
    } catch (Exception exc) {
      // may have been created by another bolt instance
      cloudSolrClient.getZkStateReader().updateClusterState();
      if (!cloudSolrClient.getZkStateReader().getClusterState().hasCollection(collection)) {
        // failed to create the collection and it doesn't exist ... throw the error
        log.error("Failed to create collection "+collection+" due to: "+exc, exc);
        throw exc;
      }
    }
  }

  public Date asTimestamp(Object obj) throws ParseException {
    Date ts;
    if (obj instanceof String) {
      ts = DateUtil.parseDate((String) obj);
    } else if (obj instanceof Date) {
      ts = (Date)obj;
    } else if (obj instanceof Calendar) {
      ts = ((Calendar)obj).getTime();
    } else if (obj instanceof Long) {
      ts = new Date((Long)obj);
    } else {
      ts = DateUtil.parseDate(obj.toString());
    }
    return ts;
  }

  public int getTimeFrame() {
    return timeFrame;
  }

  public void setTimeFrame(int timeFrame) {
    this.timeFrame = timeFrame;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
  }

  public String getDateTimePattern() {
    return dateTimePattern;
  }

  public void setDateTimePattern(String dateTimePattern) {
    this.dateTimePattern = dateTimePattern;
  }

  public String getCollectionNameBase() {
    return collectionNameBase;
  }

  public void setCollectionNameBase(String collectionNameBase) {
    this.collectionNameBase = collectionNameBase;
  }

  public String getTimezoneId() {
    return timezoneId;
  }

  public void setTimezoneId(String timezoneId) {
    this.timezoneId = timezoneId;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public void setStartDate(String startDateStr) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    try {
      this.startDate = sdf.parse(startDateStr);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Cannot parse "+startDateStr+" into a Date due to: "+e+"; expected format: yyyy-MM-dd");
    }
  }

  public String getConfigName() {
    return configName;
  }

  public void setConfigName(String configName) {
    this.configName = configName;
  }

  public int getNumShards() {
    return numShards;
  }

  public void setNumShards(int numShards) {
    this.numShards = numShards;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public int getMaxCollectionsInAlias() {
    return maxCollectionsInAlias;
  }

  public void setMaxCollectionsInAlias(int maxCollectionsInAlias) {
    this.maxCollectionsInAlias = maxCollectionsInAlias;
  }
}
