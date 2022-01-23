package org.apache.hadoop.hbase.ipc;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Queue balancer that just randomly selects a queue in the range [0, num queues).
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Stable
public class RandomQueueBalancer implements QueueBalancer {
  private final int queueSize;

  public RandomQueueBalancer(Configuration conf, List<BlockingQueue<CallRunner>> queues) {
    this.queueSize = queues.size();
  }

  @Override public int getNextQueue(CallRunner callRunner) {
    return ThreadLocalRandom.current().nextInt(queueSize);
  }
}
