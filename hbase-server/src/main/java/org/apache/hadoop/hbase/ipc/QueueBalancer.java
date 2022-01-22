package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Interface for balancing requests across IPC queues
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Stable
public interface QueueBalancer {
  /**
   * @return the index of the next queue to which a request should be inserted
   */
  int getNextQueue(CallRunner callRunner);
}
