package com.lucidworks.storm.solr;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.util.Hash;

import java.io.Serializable;
import java.util.*;

public class HashRangeGrouping implements CustomStreamGrouping, Serializable {

  private transient List<Integer> targetTasks;
  private transient List<DocRouter.Range> ranges;

  protected Map stormConf;
  protected int numShards;
  protected UniformIntegerDistribution random;
  protected int tasksPerShard;

  public HashRangeGrouping(Map stormConf, int numShards) {
    this.stormConf = stormConf;
    this.numShards = numShards;
  }

  public int getNumShards() {
    return numShards;
  }

  public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
    this.targetTasks = targetTasks;
    int numTasks = targetTasks.size();
    if (numTasks % numShards != 0)
      throw new IllegalArgumentException("Number of tasks ("+numTasks+") should be a multiple of the number of shards ("+numShards+")!");

    this.tasksPerShard = numTasks/numShards;
    this.random = new UniformIntegerDistribution(0, tasksPerShard-1);

    CompositeIdRouter docRouter =  new CompositeIdRouter();
    this.ranges = docRouter.partitionRange(numShards, docRouter.fullRange());
  }

  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    if (values == null || values.size() < 1)
      return Collections.singletonList(targetTasks.get(0));

    String docId = (String) values.get(0);
    if (docId == null)
      return Collections.singletonList(targetTasks.get(0));


    final int hash = Hash.murmurhash3_x86_32(docId, 0, docId.length(), 0);
    int rangeIndex = 0;
    for (int r=0; r < ranges.size(); r++) {
      if (ranges.get(r).includes(hash)) {
        rangeIndex = r;
        break;
      }
    }
    int selectedTask = (tasksPerShard > 1) ? rangeIndex + (random.sample() * tasksPerShard) : rangeIndex;
    return Collections.singletonList(targetTasks.get(selectedTask));
  }
}
