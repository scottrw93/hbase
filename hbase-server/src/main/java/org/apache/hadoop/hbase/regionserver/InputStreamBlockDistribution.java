/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes the HDFSBlockDistribution for a file based on the underlying located blocks
 * for an HdfsDataInputStream reading that file. This computation may involve a call to
 * the namenode, so the value is cached based on
 * {@link #HBASE_INPUT_STREAM_BLOCK_DISTRIBUTION_CACHE_PERIOD}.
 */
@InterfaceAudience.Private
public class InputStreamBlockDistribution {
  private static final Logger LOG = LoggerFactory.getLogger(InputStreamBlockDistribution.class);

  private static final String HBASE_INPUT_STREAM_BLOCK_DISTRIBUTION_ENABLED =
    "hbase.regionserver.inputstream.block.distribution.enabled";
  private static final boolean DEFAULT_HBASE_INPUT_STREAM_BLOCK_DISTRIBUTION_ENABLED = false;

  private static final String HBASE_INPUT_STREAM_BLOCK_DISTRIBUTION_CACHE_PERIOD =
    "hbase.regionserver.inputstream.block.distribution.cache.period";
  private static final int DEFAULT_HBASE_INPUT_STREAM_BLOCK_DISTRIBUTION_CACHE_PERIOD = 60_000;

  private final HdfsDataInputStream stream;
  private final StoreFileInfo fileInfo;
  private final int cachePeriodMs;

  private HDFSBlocksDistribution hdfsBlocksDistribution;
  private long lastCachedAt;

  public InputStreamBlockDistribution(HdfsDataInputStream stream, StoreFileInfo fileInfo)
    throws IOException {
    this.stream = stream;
    this.fileInfo = fileInfo;
    this.cachePeriodMs = fileInfo.getConf().getInt(
      HBASE_INPUT_STREAM_BLOCK_DISTRIBUTION_CACHE_PERIOD,
      DEFAULT_HBASE_INPUT_STREAM_BLOCK_DISTRIBUTION_CACHE_PERIOD);
    computeBlockDistribution();
  }

  /**
   * True if we should derive StoreFile HDFSBlockDistribution from the underlying input stream
   */
  public static boolean isEnabled(Configuration conf) {
    return conf.getBoolean(
      HBASE_INPUT_STREAM_BLOCK_DISTRIBUTION_ENABLED,
      DEFAULT_HBASE_INPUT_STREAM_BLOCK_DISTRIBUTION_ENABLED);
  }

  /**
   * Get the HDFSBlocksDistribution derived from the StoreFile input stream, re-computing if cache
   * is expired.
   */
  public synchronized HDFSBlocksDistribution getHDFSBlockDistribution() {
    if (EnvironmentEdgeManager.currentTime() - lastCachedAt > cachePeriodMs) {
      try {
        LOG.debug("Refreshing HDFSBlockDistribution for {}", fileInfo);
        computeBlockDistribution();
        if (LOG.isTraceEnabled()) {
          LOG.trace("New HDFSBlockDistribution for {}: {}", fileInfo, printDistribution());
        }
      } catch (IOException e) {
        LOG.warn("Failed to recompute block distribution for {}, falling back on last known value", fileInfo, e);
      }
    }
    return hdfsBlocksDistribution;
  }

  private void computeBlockDistribution() throws IOException {
    lastCachedAt = EnvironmentEdgeManager.currentTime();
    hdfsBlocksDistribution = FSUtils.computeHDFSBlocksDistribution(stream);
  }

  private String printDistribution() {
    StringBuilder builder = new StringBuilder();

    hdfsBlocksDistribution.getHostAndWeights().values()
      .forEach(hostAndWeight ->
        builder.append(", {host=").append(hostAndWeight.getHost())
          .append(", weight=").append(hostAndWeight.getWeight())
          .append("}"));

    return builder.substring(2);
  }
}
