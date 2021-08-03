package org.apache.hadoop.hbase.ipc;

import java.util.Optional;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.LimitedPrivate("hubspot")
@InterfaceStability.Unstable
public interface UpstreamCaller {

  UpstreamCaller NONE = new UpstreamCaller() {};

  String HBASE_UPSTREAM_CALLER = "hbase.upstream.caller.impl";

  default Optional<String> getUpstreamCaller() {
    return Optional.empty();
  }
}
