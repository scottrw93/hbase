package org.apache.hadoop.hbase.ipc;

import com.google.errorprone.annotations.RestrictedApi;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Queue balancer that just randomly selects a queue in the range [0, num queues).
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class RandomQueueBalancer implements QueueBalancer {
  private final int queueSize;
  private final List<BlockingQueue<CallRunner>> queues;

  public RandomQueueBalancer(Configuration conf, List<BlockingQueue<CallRunner>> queues) {
    this.queueSize = queues.size();
    this.queues = queues;
  }

  @Override public int getNextQueue(CallRunner callRunner) {
    return ThreadLocalRandom.current().nextInt(queueSize);
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  List<BlockingQueue<CallRunner>> getQueues() {
    return queues;
  }
}
