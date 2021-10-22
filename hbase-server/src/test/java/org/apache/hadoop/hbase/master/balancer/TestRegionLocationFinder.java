/**
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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestRegionLocationFinder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionLocationFinder.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;

  private final static TableName tableName = TableName.valueOf("table");
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private static Table table;
  private final static int ServerNum = 5;

  private static RegionLocationFinder finder = new RegionLocationFinder();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(ServerNum);
    table = TEST_UTIL.createTable(tableName, FAMILY, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    TEST_UTIL.waitTableAvailable(tableName, 1000);
    TEST_UTIL.loadTable(table, FAMILY);

    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (HRegion region : server.getRegions(tableName)) {
        region.flush(true);
      }
    }

    finder.setConf(TEST_UTIL.getConfiguration());
    finder.setServices(cluster.getMaster());
    finder.setClusterMetrics(cluster.getMaster().getClusterMetrics());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    table.close();
    TEST_UTIL.deleteTable(tableName);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testInternalGetTopBlockLocation() throws Exception {
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (HRegion region : server.getRegions(tableName)) {
        // get region's hdfs block distribution by region and RegionLocationFinder,
        // they should have same result
        HDFSBlocksDistribution blocksDistribution1 = region.getHDFSBlocksDistribution();
        HDFSBlocksDistribution blocksDistribution2 = finder.getBlockDistribution(region
            .getRegionInfo());
        assertEquals(blocksDistribution1.getUniqueBlocksTotalWeight(),
          blocksDistribution2.getUniqueBlocksTotalWeight());
        if (blocksDistribution1.getUniqueBlocksTotalWeight() != 0) {
          assertEquals(blocksDistribution1.getTopHosts().get(0), blocksDistribution2.getTopHosts()
              .get(0));
        }
      }
    }
  }

  @Test
  public void testMapHostNameToServerName() throws Exception {
    List<String> topHosts = new ArrayList<>();
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      String serverHost = server.getServerName().getHostname();
      if (!topHosts.contains(serverHost)) {
        topHosts.add(serverHost);
      }
    }
    List<ServerName> servers = finder.mapHostNameToServerName(topHosts);
    // mini cluster, all rs in one host
    assertEquals(1, topHosts.size());
    for (int i = 0; i < ServerNum; i++) {
      ServerName server = cluster.getRegionServer(i).getServerName();
      assertTrue(servers.contains(server));
    }
  }

  @Test
  public void testGetTopBlockLocations() throws Exception {
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (HRegion region : server.getRegions(tableName)) {
        List<ServerName> servers = finder.getTopBlockLocations(region
            .getRegionInfo());
        // test table may have empty region
        if (region.getHDFSBlocksDistribution().getUniqueBlocksTotalWeight() == 0) {
          continue;
        }
        List<String> topHosts = region.getHDFSBlocksDistribution().getTopHosts();
        // rs and datanode may have different host in local machine test
        if (!topHosts.contains(server.getServerName().getHostname())) {
          continue;
        }
        for (int j = 0; j < ServerNum; j++) {
          ServerName serverName = cluster.getRegionServer(j).getServerName();
          assertTrue(servers.contains(serverName));
        }
      }
    }
  }

  @Test
  public void testRefreshRegionsWithChangedLocality() throws IOException {
    HRegion testRegion = findFirstUserRegion();
    ServerName serverHoldingRegion =
      cluster.getServerHoldingRegion(tableName, testRegion.getRegionInfo().getRegionName());

    HDFSBlocksDistribution existingDistribution = finder.getBlockDistribution(testRegion.getRegionInfo());

    // no change in locality, so cached value should not change
    ClusterMetrics clusterMetrics = cluster.getMaster().getClusterMetrics();
    finder.setClusterMetrics(clusterMetrics);
    assertSame("expected HDFSBlockDistribution to be the same for region "
        + testRegion.getRegionInfo().getEncodedName(),
      existingDistribution,
      finder.getBlockDistribution(testRegion.getRegionInfo()));

    // changed locality, so should change now
    ClusterMetrics metricsWithChangedLocality = changeLocality(clusterMetrics, serverHoldingRegion, testRegion);
    finder.setClusterMetrics(metricsWithChangedLocality);
    assertNotSame("expected HDFSBlockDistribution to have changed for region "
        + testRegion.getRegionInfo().getEncodedName(),
      existingDistribution, finder.getBlockDistribution(testRegion.getRegionInfo()));
  }

  private HRegion findFirstUserRegion() {
    for (HRegion region : cluster.getRegions(tableName)) {
      if (!region.getRegionInfo().isMetaRegion()) {
        return region;
      }
    }
    throw new RuntimeException("Failed to find a user region for table " + tableName);
  }

  private ClusterMetrics changeLocality(ClusterMetrics clusterMetrics, ServerName serverName, HRegion region) {
    ClusterStatusProtos.ClusterStatus clusterStatus =
      ClusterMetricsBuilder.toClusterStatus(clusterMetrics);
    return ClusterMetricsBuilder.toClusterMetrics(clusterStatus
      .toBuilder()
        .clearLiveServers()
        .addAllLiveServers(clusterStatus.getLiveServersList().stream()
          .map(liveServerInfo -> {
            if (liveServerInfo.getServer().getHostName().equals(serverName.getHostname())) {
              return changeLocality(liveServerInfo, region);
            }
            return liveServerInfo;
          })
          .collect(Collectors.toList()))
      .build());
  }

  private ClusterStatusProtos.LiveServerInfo changeLocality(ClusterStatusProtos.LiveServerInfo serverInfo, HRegion forRegion) {
    return serverInfo.toBuilder()
      .setServerLoad(serverInfo.getServerLoad().toBuilder()
        .clearRegionLoads()
        .addAllRegionLoads(serverInfo.getServerLoad().getRegionLoadsList().stream()
          .map(regionLoad -> {
            byte[] regionName = regionLoad.getRegionSpecifier().getValue().toByteArray();
            if (Arrays.equals(regionName, forRegion.getRegionInfo().getRegionName())) {
              return regionLoad.toBuilder()
                .setDataLocality(regionLoad.getDataLocality() + 0.1f)
                .build();
            }
            return regionLoad;
          }).collect(Collectors.toList())))
      .build();
  }

  @Test
  public void testRefreshAndWait() throws Exception {
    finder.getCache().invalidateAll();
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      List<HRegion> regions = server.getRegions(tableName);
      if (regions.size() <= 0) {
        continue;
      }
      List<RegionInfo> regionInfos = new ArrayList<>(regions.size());
      for (HRegion region : regions) {
        regionInfos.add(region.getRegionInfo());
      }
      finder.refreshAndWait(regionInfos);
      for (RegionInfo regionInfo : regionInfos) {
        assertNotNull(finder.getCache().getIfPresent(regionInfo));
      }
    }


  }
}
