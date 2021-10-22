/*
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

import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A chore for refreshing the HDFSBlockDistribution metrics of regions and their stores.
 * It's possible that blocks have moved after a store has been opened, for example if
 * a datanode process dies or decommissions, someone runs the hdfs balancer, or another process
 * moves blocks around. If enabled, this chore will periodically re-compute the
 * HDFSBlockDistributions of store files, so that hbase can report correct locality and make
 * correct decisions based on the locality.
 *
 * By default, only refreshes store files whose locality is less than 100%. If
 * {@link #REGIONSERVER_LOCALITY_FULL_REFRESH_PERIOD} is set will also do a full refresh
 * regardless of current locality on that interval. These are on different periods because
 * there will typically be many more local StoreFiles than non-local, so a full refresh puts more
 * load on the NameNode
 */
@InterfaceAudience.Private
public class LocalityRefreshChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(StorefileRefresherChore.class);

  /**
   * The period (in milliseconds) for refreshing locality, by default only
   * refreshing stores with < 100% locality.
   */
  public static final String REGIONSERVER_LOCALITY_REFRESH_PERIOD
    = "hbase.regionserver.locality.refresh.period";
  static final int DEFAULT_REGIONSERVER_LOCALITY_REFRESH_PERIOD = 0; //disabled by default

  /**
   * The period (in milliseconds) for doing a full refresh of locality for all store files
   */
  public static final String REGIONSERVER_LOCALITY_FULL_REFRESH_PERIOD
    = "hbase.regionserver.locality.full.refresh.period";
  public static final int DEFAULT_REGIONSERVER_LOCALITY_FULL_REFRESH_PERIOD = 0;

  private final int fullRefreshPeriod;
  private final HRegionServer regionServer;

  private long lastFullRefresh = EnvironmentEdgeManager.currentTime();

  public LocalityRefreshChore(int period, int fullRefreshPeriod, HRegionServer regionServer, Stoppable stoppable) {
    super("LocalityMetricsRefreshChore", stoppable, period);
    this.fullRefreshPeriod = fullRefreshPeriod;
    this.regionServer = regionServer;
  }

  @Override
  protected void chore() {
    long now = EnvironmentEdgeManager.currentTime();
    boolean fullRefresh = shouldFullRefresh(now);
    if (fullRefresh) {
      LOG.debug("Doing a full refresh of locality for all store files");
      lastFullRefresh = now;
    }

    for (HRegion region : regionServer.getRegions()) {
      for (HStore store : region.getStores()) {
        try {
          store.refreshHDFSBlockDistributions(regionServer.getServerName().getHostname(), fullRefresh);
        } catch (IOException e) {
          LOG.debug("Failed to refresh HDFSBlockDistributions for store {}", store, e);
        }
      }
    }
  }

  private boolean shouldFullRefresh(long now) {
    if (fullRefreshPeriod <= 0) {
      return false;
    }
    return now - lastFullRefresh > fullRefreshPeriod;
  }
}
