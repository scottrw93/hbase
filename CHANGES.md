# HBASE Changelog

<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Be careful doing manual edits in this file. Do not change format
# of release header or remove the below marker. This file is generated.
# DO NOT REMOVE THIS MARKER; FOR INTERPOLATING CHANGES!-->
## Release 2.4.6 - 2021-09-10



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-6908](https://issues.apache.org/jira/browse/HBASE-6908) | Pluggable Call BlockingQueue for HBaseServer |  Major | IPC/RPC |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25642](https://issues.apache.org/jira/browse/HBASE-25642) | Fix or stop warning about already cached block |  Major | BlockCache, Operability, regionserver |
| [HBASE-24652](https://issues.apache.org/jira/browse/HBASE-24652) | master-status UI make date type fields sortable |  Minor | master, Operability, UI, Usability |
| [HBASE-25680](https://issues.apache.org/jira/browse/HBASE-25680) | Non-idempotent test in TestReplicationHFileCleaner |  Minor | test |
| [HBASE-26179](https://issues.apache.org/jira/browse/HBASE-26179) | TestRequestTooBigException spends too much time to finish |  Major | test |
| [HBASE-26160](https://issues.apache.org/jira/browse/HBASE-26160) | Configurable disallowlist for live editing of loglevels |  Minor | . |
| [HBASE-25469](https://issues.apache.org/jira/browse/HBASE-25469) | Add detailed RIT info in JSON format for consumption as metrics |  Minor | master |
| [HBASE-26154](https://issues.apache.org/jira/browse/HBASE-26154) | Provide exception metric for quota exceeded and throttling |  Minor | . |
| [HBASE-26105](https://issues.apache.org/jira/browse/HBASE-26105) | Rectify the expired TODO comment in CombinedBC |  Trivial | BlockCache |
| [HBASE-26146](https://issues.apache.org/jira/browse/HBASE-26146) | Allow custom opts for hbck in hbase bin |  Minor | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-26106](https://issues.apache.org/jira/browse/HBASE-26106) | AbstractFSWALProvider#getArchivedLogPath doesn't look for wal file in all oldWALs directory. |  Critical | wal |
| [HBASE-26205](https://issues.apache.org/jira/browse/HBASE-26205) | TableMRUtil#initCredentialsForCluster should use specified conf for UserProvider |  Major | mapreduce |
| [HBASE-26210](https://issues.apache.org/jira/browse/HBASE-26210) | HBase Write should be doomed to hang when cell size exceeds InmemoryFlushSize for CompactingMemStore |  Critical | in-memory-compaction |
| [HBASE-26244](https://issues.apache.org/jira/browse/HBASE-26244) | Avoid trim the error stack trace when running UT with maven |  Major | . |
| [HBASE-25588](https://issues.apache.org/jira/browse/HBASE-25588) | Excessive logging of "hbase.zookeeper.useMulti is deprecated. Default to true always." |  Minor | logging, Operability, Replication |
| [HBASE-26232](https://issues.apache.org/jira/browse/HBASE-26232) | SEEK\_NEXT\_USING\_HINT is ignored on reversed Scans |  Critical | Filters, scan |
| [HBASE-26204](https://issues.apache.org/jira/browse/HBASE-26204) | VerifyReplication should obtain token for peerQuorumAddress too |  Major | . |
| [HBASE-26219](https://issues.apache.org/jira/browse/HBASE-26219) | Negative time is logged while waiting on regionservers |  Trivial | . |
| [HBASE-26087](https://issues.apache.org/jira/browse/HBASE-26087) | JVM crash when displaying RPC params by MonitoredRPCHandler |  Major | UI |
| [HBASE-24570](https://issues.apache.org/jira/browse/HBASE-24570) | connection#close throws NPE |  Minor | Client |
| [HBASE-26200](https://issues.apache.org/jira/browse/HBASE-26200) | Undo 'HBASE-25165 Change 'State time' in UI so sorts (#2508)' in favor of HBASE-24652 |  Major | UI |
| [HBASE-26196](https://issues.apache.org/jira/browse/HBASE-26196) | Support configuration override for remote cluster of HFileOutputFormat locality sensitive |  Major | mapreduce |
| [HBASE-26026](https://issues.apache.org/jira/browse/HBASE-26026) | HBase Write may be stuck forever when using CompactingMemStore |  Critical | in-memory-compaction |
| [HBASE-26155](https://issues.apache.org/jira/browse/HBASE-26155) | JVM crash when scan |  Major | Scanners |
| [HBASE-26176](https://issues.apache.org/jira/browse/HBASE-26176) | Correct regex in hbase-personality.sh |  Minor | build |
| [HBASE-26170](https://issues.apache.org/jira/browse/HBASE-26170) | handleTooBigRequest in NettyRpcServer didn't skip enough bytes |  Major | . |
| [HBASE-26142](https://issues.apache.org/jira/browse/HBASE-26142) | NullPointerException when set 'hbase.hregion.memstore.mslab.indexchunksize.percent' to zero |  Critical | . |
| [HBASE-26166](https://issues.apache.org/jira/browse/HBASE-26166) | table list in master ui has a minor bug |  Minor | UI |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-26185](https://issues.apache.org/jira/browse/HBASE-26185) | Fix TestMaster#testMoveRegionWhenNotInitialized with hbase.min.version.move.system.tables |  Minor | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-26189](https://issues.apache.org/jira/browse/HBASE-26189) | Reduce log level of CompactionProgress notice to DEBUG |  Minor | Compaction |
| [HBASE-26227](https://issues.apache.org/jira/browse/HBASE-26227) | Forward port HBASE-26223 test code to branch-2.4+ |  Major | test |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-26152](https://issues.apache.org/jira/browse/HBASE-26152) | Exclude javax.servlet:servlet-api in hbase-shaded-testing-util |  Major | . |


## Release 2.4.5 - 2021-07-31



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-26108](https://issues.apache.org/jira/browse/HBASE-26108) | add option to disable scanMetrics in TableSnapshotInputFormat |  Major | . |
| [HBASE-26025](https://issues.apache.org/jira/browse/HBASE-26025) | Add a flag to mark if the IOError can be solved by retry in thrift IOError |  Major | Thrift |
| [HBASE-25986](https://issues.apache.org/jira/browse/HBASE-25986) | Expose the NORMALIZARION\_ENABLED table descriptor through a property in hbase-site |  Minor | Normalizer |
| [HBASE-26012](https://issues.apache.org/jira/browse/HBASE-26012) | Improve logging and dequeue logic in DelayQueue |  Minor | . |
| [HBASE-26020](https://issues.apache.org/jira/browse/HBASE-26020) | Split TestWALEntryStream.testDifferentCounts out |  Major | Replication, test |
| [HBASE-25937](https://issues.apache.org/jira/browse/HBASE-25937) | Clarify UnknownRegionException |  Minor | Client |
| [HBASE-25998](https://issues.apache.org/jira/browse/HBASE-25998) | Revisit synchronization in SyncFuture |  Major | Performance, regionserver, wal |
| [HBASE-26000](https://issues.apache.org/jira/browse/HBASE-26000) | Optimize the display of ZK dump in the master web UI |  Minor | . |
| [HBASE-25995](https://issues.apache.org/jira/browse/HBASE-25995) | Change the method name for DoubleArrayCost.setCosts |  Major | Balancer |
| [HBASE-26002](https://issues.apache.org/jira/browse/HBASE-26002) | MultiRowMutationEndpoint should return the result of the conditional update |  Major | Coprocessors |
| [HBASE-25993](https://issues.apache.org/jira/browse/HBASE-25993) | Make excluded SSL cipher suites configurable for all Web UIs |  Major | . |
| [HBASE-25987](https://issues.apache.org/jira/browse/HBASE-25987) | Make SSL keystore type configurable for HBase ThriftServer |  Major | Thrift |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-26120](https://issues.apache.org/jira/browse/HBASE-26120) | New replication gets stuck or data loss when multiwal groups more than 10 |  Critical | Replication |
| [HBASE-26001](https://issues.apache.org/jira/browse/HBASE-26001) | When turn on access control, the cell level TTL of Increment and Append operations is invalid. |  Minor | Coprocessors |
| [HBASE-24984](https://issues.apache.org/jira/browse/HBASE-24984) | WAL corruption due to early DBBs re-use when Durability.ASYNC\_WAL is used with multi operation |  Critical | rpc, wal |
| [HBASE-26088](https://issues.apache.org/jira/browse/HBASE-26088) | conn.getBufferedMutator(tableName) leaks thread executors and other problems |  Critical | Client |
| [HBASE-25973](https://issues.apache.org/jira/browse/HBASE-25973) | Balancer should explain progress in a better way in log |  Major | Balancer |
| [HBASE-26083](https://issues.apache.org/jira/browse/HBASE-26083) | L1 miss metric is incorrect when using CombinedBlockCache |  Minor | BlockCache |
| [HBASE-26086](https://issues.apache.org/jira/browse/HBASE-26086) | TestHRegionReplayEvents do not pass in branch-2 and throws NullPointerException |  Minor | . |
| [HBASE-26036](https://issues.apache.org/jira/browse/HBASE-26036) | DBB released too early and dirty data for some operations |  Critical | rpc |
| [HBASE-26068](https://issues.apache.org/jira/browse/HBASE-26068) | The last assertion in TestHStore.testRefreshStoreFilesNotChanged is wrong |  Major | test |
| [HBASE-22923](https://issues.apache.org/jira/browse/HBASE-22923) | hbase:meta is assigned to localhost when we downgrade the hbase version |  Major | . |
| [HBASE-26030](https://issues.apache.org/jira/browse/HBASE-26030) | hbase-cleanup.sh did not clean the wal dir if hbase.wal.dir configured individually |  Major | scripts |
| [HBASE-26035](https://issues.apache.org/jira/browse/HBASE-26035) | Redundant null check in the compareTo function |  Minor | metrics, Performance |
| [HBASE-25902](https://issues.apache.org/jira/browse/HBASE-25902) | Add missing CFs in meta during HBase 1 to 2.3+ Upgrade |  Critical | meta, Operability |
| [HBASE-26028](https://issues.apache.org/jira/browse/HBASE-26028) | The view as json page shows exception when using TinyLfuBlockCache |  Major | UI |
| [HBASE-26039](https://issues.apache.org/jira/browse/HBASE-26039) | TestReplicationKillRS is useless after HBASE-23956 |  Major | Replication, test |
| [HBASE-25980](https://issues.apache.org/jira/browse/HBASE-25980) | Master table.jsp pointed at meta throws 500 when no all replicas are online |  Major | master, meta replicas, UI |
| [HBASE-26013](https://issues.apache.org/jira/browse/HBASE-26013) | Get operations readRows metrics becomes zero after HBASE-25677 |  Minor | metrics |
| [HBASE-25877](https://issues.apache.org/jira/browse/HBASE-25877) | Add access  check for compactionSwitch |  Major | security |
| [HBASE-25698](https://issues.apache.org/jira/browse/HBASE-25698) | Persistent IllegalReferenceCountException at scanner open when using TinyLfuBlockCache |  Major | BucketCache, HFile, Scanners |
| [HBASE-25984](https://issues.apache.org/jira/browse/HBASE-25984) | FSHLog WAL lockup with sync future reuse [RS deadlock] |  Critical | regionserver, wal |
| [HBASE-25997](https://issues.apache.org/jira/browse/HBASE-25997) | NettyRpcFrameDecoder decode request header wrong  when handleTooBigRequest |  Major | rpc |
| [HBASE-25967](https://issues.apache.org/jira/browse/HBASE-25967) | The readRequestsCount does not calculate when the outResults is empty |  Major | metrics |
| [HBASE-25981](https://issues.apache.org/jira/browse/HBASE-25981) | JVM crash when displaying regionserver UI |  Major | rpc, UI |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-26093](https://issues.apache.org/jira/browse/HBASE-26093) | Replication is stuck due to zero length wal file in oldWALs directory [master/branch-2] |  Major | . |
| [HBASE-24734](https://issues.apache.org/jira/browse/HBASE-24734) | RegionInfo#containsRange should support check meta table |  Major | HFile, MTTR |
| [HBASE-25739](https://issues.apache.org/jira/browse/HBASE-25739) | TableSkewCostFunction need to use aggregated deviation |  Major | Balancer, master |
| [HBASE-25992](https://issues.apache.org/jira/browse/HBASE-25992) | Polish the ReplicationSourceWALReader code for 2.x after HBASE-25596 |  Major | Replication |
| [HBASE-25989](https://issues.apache.org/jira/browse/HBASE-25989) | FanOutOneBlockAsyncDFSOutput using shaded protobuf in hdfs 3.3+ |  Major | . |
| [HBASE-25947](https://issues.apache.org/jira/browse/HBASE-25947) | Backport 'HBASE-25894 Improve the performance for region load and region count related cost functions' to branch-2.4 and branch-2.3 |  Major | Balancer, Performance |
| [HBASE-25969](https://issues.apache.org/jira/browse/HBASE-25969) | Cleanup netty-all transitive includes |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25934](https://issues.apache.org/jira/browse/HBASE-25934) | Add username for RegionScannerHolder |  Minor | . |
| [HBASE-26123](https://issues.apache.org/jira/browse/HBASE-26123) | Restore fields dropped by HBASE-25986 to public interfaces |  Major | . |
| [HBASE-25521](https://issues.apache.org/jira/browse/HBASE-25521) | Change ChoreService and ScheduledChore to IA.Private |  Major | util |
| [HBASE-26015](https://issues.apache.org/jira/browse/HBASE-26015) | Should implement getRegionServers(boolean) method in AsyncAdmin |  Major | Admin, Client |
| [HBASE-25918](https://issues.apache.org/jira/browse/HBASE-25918) | Upgrade hbase-thirdparty dependency to 3.5.1 |  Critical | dependencies |


## Release 2.4.4 - 2021-06-14



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25666](https://issues.apache.org/jira/browse/HBASE-25666) | Explain why balancer is skipping runs |  Major | Balancer, master, UI |
| [HBASE-25942](https://issues.apache.org/jira/browse/HBASE-25942) | Get rid of null regioninfo in wrapped connection exceptions |  Trivial | logging |
| [HBASE-25908](https://issues.apache.org/jira/browse/HBASE-25908) | Exclude jakarta.activation-api |  Major | hadoop3, shading |
| [HBASE-25933](https://issues.apache.org/jira/browse/HBASE-25933) | Log trace raw exception, instead of cause message in NettyRpcServerRequestDecoder |  Minor | . |
| [HBASE-25906](https://issues.apache.org/jira/browse/HBASE-25906) | UI of master-status to show recent history of balancer desicion |  Major | Balancer, master, UI |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25930](https://issues.apache.org/jira/browse/HBASE-25930) | Thrift does not support requests in Kerberos environment |  Major | Thrift |
| [HBASE-25929](https://issues.apache.org/jira/browse/HBASE-25929) | RegionServer JVM crash when compaction |  Critical | Compaction |
| [HBASE-25924](https://issues.apache.org/jira/browse/HBASE-25924) | Seeing a spike in uncleanlyClosedWALs metric. |  Major | Replication, wal |
| [HBASE-25932](https://issues.apache.org/jira/browse/HBASE-25932) | TestWALEntryStream#testCleanClosedWALs test is failing. |  Major | metrics, Replication, wal |
| [HBASE-25903](https://issues.apache.org/jira/browse/HBASE-25903) | ReadOnlyZKClient APIs - CompletableFuture.get() calls can cause threads to hang forver when ZK client create throws Non IOException |  Major | . |
| [HBASE-25927](https://issues.apache.org/jira/browse/HBASE-25927) | Fix the log messages by not stringifying the exceptions in log |  Minor | logging |
| [HBASE-25938](https://issues.apache.org/jira/browse/HBASE-25938) | The SnapshotOfRegionAssignmentFromMeta.initialize call in FavoredNodeLoadBalancer is just a dummy one |  Major | Balancer, FavoredNodes |
| [HBASE-25898](https://issues.apache.org/jira/browse/HBASE-25898) | RS getting aborted due to NPE in Replication WALEntryStream |  Critical | Replication |
| [HBASE-25875](https://issues.apache.org/jira/browse/HBASE-25875) | RegionServer failed to start due to IllegalThreadStateException in AuthenticationTokenSecretManager.start |  Major | . |
| [HBASE-25892](https://issues.apache.org/jira/browse/HBASE-25892) | 'False' should be 'True' in auditlog of listLabels |  Major | logging, security |
| [HBASE-25817](https://issues.apache.org/jira/browse/HBASE-25817) | Memory leak from thrift server hashMap |  Minor | Thrift |
| [HBASE-25848](https://issues.apache.org/jira/browse/HBASE-25848) | Add flexibility to backup replication in case replication filter throws an exception |  Major | . |
| [HBASE-25827](https://issues.apache.org/jira/browse/HBASE-25827) | Per Cell TTL tags get duplicated with increments causing tags length overflow |  Critical | regionserver |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25910](https://issues.apache.org/jira/browse/HBASE-25910) | Fix TestClusterPortAssignment.testClusterPortAssignment test and re-enable it. |  Minor | flakies, test |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25963](https://issues.apache.org/jira/browse/HBASE-25963) | HBaseCluster should be marked as IA.Public |  Major | API |
| [HBASE-25941](https://issues.apache.org/jira/browse/HBASE-25941) | TestRESTServerSSL fails because of jdk bug |  Major | test |
| [HBASE-25940](https://issues.apache.org/jira/browse/HBASE-25940) | Update Compression/TestCompressionTest: LZ4, SNAPPY, LZO |  Major | . |
| [HBASE-25791](https://issues.apache.org/jira/browse/HBASE-25791) | UI of master-status to show a recent history of  that why balancer  was rejected to run |  Major | Balancer, master, UI |


## Release 2.4.3 - 2021-05-21



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25751](https://issues.apache.org/jira/browse/HBASE-25751) | Add writable TimeToPurgeDeletes to ScanOptions |  Major | . |
| [HBASE-25587](https://issues.apache.org/jira/browse/HBASE-25587) | [hbck2] Schedule SCP for all unknown servers |  Major | hbase-operator-tools, hbck2 |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25860](https://issues.apache.org/jira/browse/HBASE-25860) | Add metric for successful wal roll requests. |  Major | metrics, wal |
| [HBASE-25754](https://issues.apache.org/jira/browse/HBASE-25754) | StripeCompactionPolicy should support compacting cold regions |  Minor | Compaction |
| [HBASE-25766](https://issues.apache.org/jira/browse/HBASE-25766) | Introduce RegionSplitRestriction that restricts the pattern of the split point |  Major | . |
| [HBASE-25798](https://issues.apache.org/jira/browse/HBASE-25798) | typo in MetricsAssertHelper |  Minor | . |
| [HBASE-25770](https://issues.apache.org/jira/browse/HBASE-25770) | Http InfoServers should honor gzip encoding when requested |  Major | UI |
| [HBASE-25776](https://issues.apache.org/jira/browse/HBASE-25776) | Use Class.asSubclass to fix the warning in StochasticLoadBalancer.loadCustomCostFunctions |  Minor | Balancer |
| [HBASE-25767](https://issues.apache.org/jira/browse/HBASE-25767) | CandidateGenerator.getRandomIterationOrder is too slow on large cluster |  Major | Balancer, Performance |
| [HBASE-25762](https://issues.apache.org/jira/browse/HBASE-25762) | Improvement for some debug-logging guards |  Minor | logging, Performance |
| [HBASE-25653](https://issues.apache.org/jira/browse/HBASE-25653) | Add units and round off region size to 2 digits after decimal |  Major | master, Normalizer |
| [HBASE-25759](https://issues.apache.org/jira/browse/HBASE-25759) | The master services field in LocalityBasedCostFunction is never used |  Major | Balancer |
| [HBASE-25747](https://issues.apache.org/jira/browse/HBASE-25747) | Remove unused getWriteAvailable method in OperationQuota |  Minor | Quotas |
| [HBASE-25558](https://issues.apache.org/jira/browse/HBASE-25558) | Adding audit log for execMasterService |  Major | . |
| [HBASE-25703](https://issues.apache.org/jira/browse/HBASE-25703) | Support conditional update in MultiRowMutationEndpoint |  Major | Coprocessors |
| [HBASE-25627](https://issues.apache.org/jira/browse/HBASE-25627) | HBase replication should have a metric to represent if the source is stuck getting initialized |  Major | Replication |
| [HBASE-25688](https://issues.apache.org/jira/browse/HBASE-25688) | Use CustomRequestLog instead of Slf4jRequestLog for jetty |  Major | logging, UI |
| [HBASE-25678](https://issues.apache.org/jira/browse/HBASE-25678) | Support nonce operations for Increment/Append in RowMutations and CheckAndMutate |  Major | . |
| [HBASE-25679](https://issues.apache.org/jira/browse/HBASE-25679) | Size of log queue metric is incorrect in branch-1/branch-2 |  Major | . |
| [HBASE-25518](https://issues.apache.org/jira/browse/HBASE-25518) | Support separate child regions to different region servers |  Major | . |
| [HBASE-25621](https://issues.apache.org/jira/browse/HBASE-25621) | Balancer should check region plan source to avoid misplace region groups |  Major | Balancer |
| [HBASE-25374](https://issues.apache.org/jira/browse/HBASE-25374) | Make REST Client connection and socket time out configurable |  Minor | REST |
| [HBASE-25597](https://issues.apache.org/jira/browse/HBASE-25597) | Add row info in Exception when cell size exceeds maxCellSize |  Minor | . |
| [HBASE-25660](https://issues.apache.org/jira/browse/HBASE-25660) | Print split policy in use on Region open (as well as split policy vitals) |  Trivial | . |
| [HBASE-25635](https://issues.apache.org/jira/browse/HBASE-25635) | CandidateGenerator may miss some region balance actions |  Major | Balancer |
| [HBASE-25636](https://issues.apache.org/jira/browse/HBASE-25636) | Expose HBCK report as metrics |  Minor | metrics |
| [HBASE-25548](https://issues.apache.org/jira/browse/HBASE-25548) | Optionally allow snapshots to preserve cluster's max filesize config by setting it into table descriptor |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25867](https://issues.apache.org/jira/browse/HBASE-25867) | Extra doc around ITBLL |  Minor | documentation |
| [HBASE-25859](https://issues.apache.org/jira/browse/HBASE-25859) | Reference class incorrectly parses the protobuf magic marker |  Minor | regionserver |
| [HBASE-25774](https://issues.apache.org/jira/browse/HBASE-25774) | ServerManager.getOnlineServer may miss some region servers when refreshing state in some procedure implementations |  Critical | Replication |
| [HBASE-25825](https://issues.apache.org/jira/browse/HBASE-25825) | RSGroupBasedLoadBalancer.onConfigurationChange should chain the request to internal balancer |  Major | Balancer |
| [HBASE-25792](https://issues.apache.org/jira/browse/HBASE-25792) | Filter out o.a.hadoop.thirdparty building shaded jars |  Major | shading |
| [HBASE-25806](https://issues.apache.org/jira/browse/HBASE-25806) | Backport the region location finder initialization fix in HBASE-25802 |  Major | Balancer |
| [HBASE-25735](https://issues.apache.org/jira/browse/HBASE-25735) | Add target Region to connection exceptions |  Major | rpc |
| [HBASE-25717](https://issues.apache.org/jira/browse/HBASE-25717) | RegionServer aborted due to ClassCastException |  Major | . |
| [HBASE-25743](https://issues.apache.org/jira/browse/HBASE-25743) | Retry REQUESTTIMEOUT KeeperExceptions from ZK |  Major | Zookeeper |
| [HBASE-25726](https://issues.apache.org/jira/browse/HBASE-25726) | MoveCostFunction is not included in the list of cost functions for StochasticLoadBalancer |  Major | Balancer |
| [HBASE-25692](https://issues.apache.org/jira/browse/HBASE-25692) | Failure to instantiate WALCellCodec leaks socket in replication |  Major | Replication |
| [HBASE-25568](https://issues.apache.org/jira/browse/HBASE-25568) | Upgrade Thrift jar to fix CVE-2020-13949 |  Critical | Thrift |
| [HBASE-25562](https://issues.apache.org/jira/browse/HBASE-25562) | ReplicationSourceWALReader log and handle exception immediately without retrying |  Major | Replication |
| [HBASE-25693](https://issues.apache.org/jira/browse/HBASE-25693) | NPE getting metrics from standby masters (MetricsMasterWrapperImpl.getMergePlanCount) |  Major | master |
| [HBASE-25685](https://issues.apache.org/jira/browse/HBASE-25685) | asyncprofiler2.0 no longer supports svg; wants html |  Major | . |
| [HBASE-25594](https://issues.apache.org/jira/browse/HBASE-25594) | graceful\_stop.sh fails to unload regions when ran at localhost |  Minor | . |
| [HBASE-25674](https://issues.apache.org/jira/browse/HBASE-25674) | RegionInfo.parseFrom(DataInputStream) sometimes fails to read the protobuf magic marker |  Minor | Client |
| [HBASE-25595](https://issues.apache.org/jira/browse/HBASE-25595) | TestLruBlockCache.testBackgroundEvictionThread is flaky |  Major | . |
| [HBASE-25662](https://issues.apache.org/jira/browse/HBASE-25662) | Fix spotbugs warning in RoundRobinTableInputFormat |  Major | findbugs |
| [HBASE-25657](https://issues.apache.org/jira/browse/HBASE-25657) | Fix spotbugs warnings after upgrading spotbugs to 4.x |  Major | findbugs |
| [HBASE-25646](https://issues.apache.org/jira/browse/HBASE-25646) | Possible Resource Leak in CatalogJanitor |  Major | master |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25691](https://issues.apache.org/jira/browse/HBASE-25691) | Test failure: TestVerifyBucketCacheFile.testRetrieveFromFile |  Major | test |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25876](https://issues.apache.org/jira/browse/HBASE-25876) | Add retry if we fail to read all bytes of the protobuf magic marker |  Trivial | io |
| [HBASE-25790](https://issues.apache.org/jira/browse/HBASE-25790) | NamedQueue 'BalancerRejection' for recent history of balancer skipping |  Major | Balancer, master |
| [HBASE-25854](https://issues.apache.org/jira/browse/HBASE-25854) | Remove redundant AM in-memory state changes in CatalogJanitor |  Major | . |
| [HBASE-25847](https://issues.apache.org/jira/browse/HBASE-25847) | More DEBUG and TRACE level logging in CatalogJanitor and HbckChore |  Minor | . |
| [HBASE-25838](https://issues.apache.org/jira/browse/HBASE-25838) | Use double instead of Double in StochasticLoadBalancer |  Major | Balancer, Performance |
| [HBASE-25835](https://issues.apache.org/jira/browse/HBASE-25835) | Ignore duplicate split requests from regionserver reports |  Major | . |
| [HBASE-25836](https://issues.apache.org/jira/browse/HBASE-25836) | RegionStates#getAssignmentsForBalancer should only care about OPEN or OPENING regions |  Major | . |
| [HBASE-25840](https://issues.apache.org/jira/browse/HBASE-25840) | CatalogJanitor warns about skipping gc of regions during RIT, but does not actually skip |  Minor | . |
| [HBASE-25775](https://issues.apache.org/jira/browse/HBASE-25775) | Use a special balancer to deal with maintenance mode |  Major | Balancer |
| [HBASE-25199](https://issues.apache.org/jira/browse/HBASE-25199) | Remove HStore#getStoreHomedir |  Minor | . |
| [HBASE-25696](https://issues.apache.org/jira/browse/HBASE-25696) | Need to initialize SLF4JBridgeHandler in jul-to-slf4j for redirecting jul to slf4j |  Major | logging |
| [HBASE-25695](https://issues.apache.org/jira/browse/HBASE-25695) | Link to the filter on hbase:meta from user tables panel on master page |  Major | UI |
| [HBASE-25629](https://issues.apache.org/jira/browse/HBASE-25629) | Reimplement TestCurrentHourProvider to not depend on unstable TZs |  Major | test |
| [HBASE-25671](https://issues.apache.org/jira/browse/HBASE-25671) | Backport HBASE-25608 to branch-2 |  Major | . |
| [HBASE-25677](https://issues.apache.org/jira/browse/HBASE-25677) | Server+table counters on each scan #nextRaw invocation becomes a bottleneck when heavy load |  Major | metrics |
| [HBASE-25667](https://issues.apache.org/jira/browse/HBASE-25667) | Remove RSGroup test addition made in parent; depends on functionality not in old branches |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25884](https://issues.apache.org/jira/browse/HBASE-25884) | NPE while getting Balancer decisions |  Major | . |
| [HBASE-25755](https://issues.apache.org/jira/browse/HBASE-25755) | Exclude tomcat-embed-core from libthrift |  Critical | dependencies, Thrift |
| [HBASE-25750](https://issues.apache.org/jira/browse/HBASE-25750) | Upgrade RpcControllerFactory and HBaseRpcController from Private to LimitedPrivate(COPROC,PHOENIX) |  Major | Coprocessors, phoenix, rpc |
| [HBASE-25734](https://issues.apache.org/jira/browse/HBASE-25734) | Backport HBASE-24305 to branch-2.4 |  Minor | . |
| [HBASE-25604](https://issues.apache.org/jira/browse/HBASE-25604) | Upgrade spotbugs to 4.x |  Major | build, findbugs |
| [HBASE-24305](https://issues.apache.org/jira/browse/HBASE-24305) | Handle deprecations in ServerName |  Minor | . |


## Release 2.4.2 - Unreleased (as of 2021-03-08)



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25460](https://issues.apache.org/jira/browse/HBASE-25460) | Expose drainingServers as cluster metric |  Major | metrics |
| [HBASE-25496](https://issues.apache.org/jira/browse/HBASE-25496) | add get\_namespace\_rsgroup command |  Major | . |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-23578](https://issues.apache.org/jira/browse/HBASE-23578) | [UI] Master UI shows long stack traces when table is broken |  Minor | master, UI |
| [HBASE-25492](https://issues.apache.org/jira/browse/HBASE-25492) | Create table with rsgroup info in branch-2 |  Major | rsgroup |
| [HBASE-25539](https://issues.apache.org/jira/browse/HBASE-25539) | Add metric for age of oldest wal. |  Major | metrics, regionserver |
| [HBASE-25574](https://issues.apache.org/jira/browse/HBASE-25574) | Revisit put/delete/increment/append related RegionObserver methods |  Major | Coprocessors |
| [HBASE-25541](https://issues.apache.org/jira/browse/HBASE-25541) | In WALEntryStream, set the current path to null while dequeing the log |  Major | . |
| [HBASE-23887](https://issues.apache.org/jira/browse/HBASE-23887) | New L1 cache : AdaptiveLRU |  Major | BlockCache, Performance |
| [HBASE-25534](https://issues.apache.org/jira/browse/HBASE-25534) | Honor TableDescriptor settings earlier in normalization |  Major | Normalizer |
| [HBASE-25507](https://issues.apache.org/jira/browse/HBASE-25507) | Leak of ESTABLISHED sockets when compaction encountered "java.io.IOException: Invalid HFile block magic" |  Major | Compaction |
| [HBASE-25542](https://issues.apache.org/jira/browse/HBASE-25542) | Add client detail to scan name so when lease expires, we have clue on who was scanning |  Major | scan |
| [HBASE-25528](https://issues.apache.org/jira/browse/HBASE-25528) | Dedicated merge dispatch threadpool on master |  Minor | master |
| [HBASE-25536](https://issues.apache.org/jira/browse/HBASE-25536) | Remove 0 length wal file from logQueue if it belongs to old sources. |  Major | Replication |
| [HBASE-25482](https://issues.apache.org/jira/browse/HBASE-25482) | Improve SimpleRegionNormalizer#getAverageRegionSizeMb |  Minor | Normalizer |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25626](https://issues.apache.org/jira/browse/HBASE-25626) | Possible Resource Leak in HeterogeneousRegionCountCostFunction |  Major | . |
| [HBASE-25644](https://issues.apache.org/jira/browse/HBASE-25644) | Scan#setSmall blindly sets ReadType as PREAD |  Critical | . |
| [HBASE-25609](https://issues.apache.org/jira/browse/HBASE-25609) | There is a problem with the SPLITS\_FILE in the HBase shell statement |  Minor | . |
| [HBASE-25385](https://issues.apache.org/jira/browse/HBASE-25385) | TestCurrentHourProvider fails if the latest timezone changes are not present |  Blocker | . |
| [HBASE-25596](https://issues.apache.org/jira/browse/HBASE-25596) | Fix NPE in ReplicationSourceManager as well as avoid permanently unreplicated data due to EOFException from WAL |  Critical | . |
| [HBASE-25367](https://issues.apache.org/jira/browse/HBASE-25367) | Sort broken after Change 'State time' in UI |  Major | UI |
| [HBASE-25421](https://issues.apache.org/jira/browse/HBASE-25421) | There is no limit on the column family length when creating a table |  Major | Client |
| [HBASE-25371](https://issues.apache.org/jira/browse/HBASE-25371) | When openRegion fails during initial verification(before initializing and setting seq num), exception is observed during region close. |  Major | Region Assignment |
| [HBASE-25611](https://issues.apache.org/jira/browse/HBASE-25611) | ExportSnapshot chmod flag uses value as decimal |  Major | . |
| [HBASE-25586](https://issues.apache.org/jira/browse/HBASE-25586) | Fix HBASE-22492 on branch-2 (SASL GapToken) |  Major | rpc |
| [HBASE-25598](https://issues.apache.org/jira/browse/HBASE-25598) | TestFromClientSide5.testScanMetrics is flaky |  Major | . |
| [HBASE-25556](https://issues.apache.org/jira/browse/HBASE-25556) | Frequent replication "Encountered a malformed edit" warnings |  Minor | Operability, Replication |
| [HBASE-25575](https://issues.apache.org/jira/browse/HBASE-25575) | Should validate Puts in RowMutations |  Minor | Client |
| [HBASE-25559](https://issues.apache.org/jira/browse/HBASE-25559) | Terminate threads of oldsources while RS is closing |  Major | . |
| [HBASE-25543](https://issues.apache.org/jira/browse/HBASE-25543) | When configuration "hadoop.security.authorization" is set to false,  the system will still try to authorize an RPC and raise AccessDeniedException |  Minor | IPC/RPC |
| [HBASE-25554](https://issues.apache.org/jira/browse/HBASE-25554) | NPE when init RegionMover |  Major | . |
| [HBASE-25523](https://issues.apache.org/jira/browse/HBASE-25523) | Region normalizer chore thread is getting killed |  Major | Normalizer |
| [HBASE-25533](https://issues.apache.org/jira/browse/HBASE-25533) |  The metadata of the table and family should not be an empty string |  Major | . |
| [HBASE-25478](https://issues.apache.org/jira/browse/HBASE-25478) | Implement retries when enabling tables in TestRegionReplicaReplicationEndpoint |  Minor | . |
| [HBASE-25513](https://issues.apache.org/jira/browse/HBASE-25513) | When the table is turned on normalize, the first region may not be merged even the size is 0 |  Major | Normalizer |
| [HBASE-25497](https://issues.apache.org/jira/browse/HBASE-25497) | move\_namespaces\_rsgroup should change hbase.rsgroup.name config in NamespaceDescriptor |  Major | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-24900](https://issues.apache.org/jira/browse/HBASE-24900) | Make retain assignment configurable during SCP |  Major | amv2 |
| [HBASE-25509](https://issues.apache.org/jira/browse/HBASE-25509) | ChoreService.cancelChore will not call ScheduledChore.cleanup which may lead to resource leak |  Major | util |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25553](https://issues.apache.org/jira/browse/HBASE-25553) | It is better for ReplicationTracker.getListOfRegionServers to return ServerName instead of String |  Major | . |
| [HBASE-25620](https://issues.apache.org/jira/browse/HBASE-25620) | Increase timeout value for pre commit |  Major | build, test |
| [HBASE-25615](https://issues.apache.org/jira/browse/HBASE-25615) | Upgrade java version in pre commit docker file |  Major | build |
| [HBASE-25601](https://issues.apache.org/jira/browse/HBASE-25601) | Remove search hadoop references in book |  Major | documentation |


## Release 2.4.1 - 2021-01-18



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-24620](https://issues.apache.org/jira/browse/HBASE-24620) | Add a ClusterManager which submits command to ZooKeeper and its Agent which picks and execute those Commands. |  Major | integration tests |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25329](https://issues.apache.org/jira/browse/HBASE-25329) | Dump region hashes in logs for the regions that are stuck in transition for more than a configured amount of time |  Minor | . |
| [HBASE-25475](https://issues.apache.org/jira/browse/HBASE-25475) | Improve unit test for HBASE-25445 : SplitWALRemoteProcedure failed to archive split WAL |  Minor | wal |
| [HBASE-25249](https://issues.apache.org/jira/browse/HBASE-25249) | Adding StoreContext |  Major | . |
| [HBASE-25449](https://issues.apache.org/jira/browse/HBASE-25449) | 'dfs.client.read.shortcircuit' should not be set in hbase-default.xml |  Major | conf |
| [HBASE-25476](https://issues.apache.org/jira/browse/HBASE-25476) | Enable error prone check in pre commit |  Major | build |
| [HBASE-25211](https://issues.apache.org/jira/browse/HBASE-25211) | Rack awareness in region\_mover |  Major | . |
| [HBASE-25483](https://issues.apache.org/jira/browse/HBASE-25483) | set the loadMeta log level to debug. |  Major | MTTR, Region Assignment |
| [HBASE-25435](https://issues.apache.org/jira/browse/HBASE-25435) | Slow metric value can be configured |  Minor | metrics |
| [HBASE-25318](https://issues.apache.org/jira/browse/HBASE-25318) | Configure where IntegrationTestImportTsv generates HFiles |  Minor | integration tests |
| [HBASE-24850](https://issues.apache.org/jira/browse/HBASE-24850) | CellComparator perf improvement |  Critical | Performance, scan |
| [HBASE-25425](https://issues.apache.org/jira/browse/HBASE-25425) | Some notes on RawCell |  Trivial | . |
| [HBASE-25420](https://issues.apache.org/jira/browse/HBASE-25420) | Some minor improvements in rpc implementation |  Minor | rpc |
| [HBASE-25246](https://issues.apache.org/jira/browse/HBASE-25246) | Backup/Restore hbase cell tags. |  Major | backup&restore |
| [HBASE-25328](https://issues.apache.org/jira/browse/HBASE-25328) | Add builder method to create Tags. |  Minor | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25356](https://issues.apache.org/jira/browse/HBASE-25356) | HBaseAdmin#getRegion() needs to filter out non-regionName and non-encodedRegionName |  Major | shell |
| [HBASE-25279](https://issues.apache.org/jira/browse/HBASE-25279) | Non-daemon thread in ZKWatcher |  Critical | Zookeeper |
| [HBASE-25504](https://issues.apache.org/jira/browse/HBASE-25504) | [branch-2.4] Restore method removed by HBASE-25277 to LP(CONFIG) coprocessors |  Major | compatibility, Coprocessors, security |
| [HBASE-25503](https://issues.apache.org/jira/browse/HBASE-25503) | HBase code download is failing on windows with invalid path error |  Major | . |
| [HBASE-24813](https://issues.apache.org/jira/browse/HBASE-24813) | ReplicationSource should clear buffer usage on ReplicationSourceManager upon termination |  Major | Replication |
| [HBASE-25459](https://issues.apache.org/jira/browse/HBASE-25459) | WAL can't be cleaned in some scenes |  Major | . |
| [HBASE-25434](https://issues.apache.org/jira/browse/HBASE-25434) | SlowDelete & SlowPut metric value should use updateDelete & updatePut |  Major | regionserver |
| [HBASE-25441](https://issues.apache.org/jira/browse/HBASE-25441) | add security check for some APIs in RSRpcServices |  Critical | . |
| [HBASE-25432](https://issues.apache.org/jira/browse/HBASE-25432) | we should add security checks for setTableStateInMeta and fixMeta |  Blocker | . |
| [HBASE-25445](https://issues.apache.org/jira/browse/HBASE-25445) | Old WALs archive fails in procedure based WAL split |  Critical | wal |
| [HBASE-25287](https://issues.apache.org/jira/browse/HBASE-25287) | Forgetting to unbuffer streams results in many CLOSE\_WAIT sockets when loading files |  Major | . |
| [HBASE-25447](https://issues.apache.org/jira/browse/HBASE-25447) | remoteProc is suspended due to OOM ERROR |  Major | proc-v2 |
| [HBASE-24755](https://issues.apache.org/jira/browse/HBASE-24755) | [LOG][RSGroup]Error message is confusing while adding a offline RS to rsgroup |  Major | rsgroup |
| [HBASE-25463](https://issues.apache.org/jira/browse/HBASE-25463) | Fix comment error |  Minor | shell |
| [HBASE-25457](https://issues.apache.org/jira/browse/HBASE-25457) | Possible race in AsyncConnectionImpl between getChoreService and close |  Major | Client |
| [HBASE-25456](https://issues.apache.org/jira/browse/HBASE-25456) | setRegionStateInMeta need security check |  Critical | . |
| [HBASE-25383](https://issues.apache.org/jira/browse/HBASE-25383) | HBase doesn't update and remove the peer config from hbase.replication.source.custom.walentryfilters if the config is already set on the peer. |  Major | . |
| [HBASE-25404](https://issues.apache.org/jira/browse/HBASE-25404) | Procedures table Id under master web UI gets word break to single character |  Minor | UI |
| [HBASE-25277](https://issues.apache.org/jira/browse/HBASE-25277) | postScannerFilterRow impacts Scan performance a lot in HBase 2.x |  Critical | Coprocessors, scan |
| [HBASE-25365](https://issues.apache.org/jira/browse/HBASE-25365) | The log in move\_servers\_rsgroup is incorrect |  Minor | . |
| [HBASE-25372](https://issues.apache.org/jira/browse/HBASE-25372) | Fix typo in ban-jersey section of the enforcer plugin in pom.xml |  Major | build |
| [HBASE-25361](https://issues.apache.org/jira/browse/HBASE-25361) | [Flakey Tests] branch-2 TestMetaRegionLocationCache.testStandByMetaLocations |  Major | flakies |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25502](https://issues.apache.org/jira/browse/HBASE-25502) | IntegrationTestMTTR fails with TableNotFoundException |  Major | integration tests |
| [HBASE-25334](https://issues.apache.org/jira/browse/HBASE-25334) | TestRSGroupsFallback.testFallback is flaky |  Major | . |
| [HBASE-25370](https://issues.apache.org/jira/browse/HBASE-25370) | Fix flaky test TestClassFinder#testClassFinderDefaultsToOwnPackage |  Major | test |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25293](https://issues.apache.org/jira/browse/HBASE-25293) | Followup jira to address the client handling issue when chaning from meta replica to non-meta-replica at the server side. |  Minor | . |
| [HBASE-25353](https://issues.apache.org/jira/browse/HBASE-25353) | [Flakey Tests] branch-2 TestShutdownBackupMaster |  Major | flakies |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25333](https://issues.apache.org/jira/browse/HBASE-25333) | Add maven enforcer rule to ban VisibleForTesting imports |  Major | build, pom |
| [HBASE-25452](https://issues.apache.org/jira/browse/HBASE-25452) | Use MatcherAssert.assertThat instead of org.junit.Assert.assertThat |  Major | test |
| [HBASE-25400](https://issues.apache.org/jira/browse/HBASE-25400) | [Flakey Tests] branch-2 TestRegionMoveAndAbandon |  Major | . |
| [HBASE-25389](https://issues.apache.org/jira/browse/HBASE-25389) | [Flakey Tests] branch-2 TestMetaShutdownHandler |  Major | flakies |


## Release 2.4.0 - Unreleased (as of 2020-12-03)



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25242](https://issues.apache.org/jira/browse/HBASE-25242) | Add Increment/Append support to RowMutations |  Critical | Client, regionserver |
| [HBASE-25278](https://issues.apache.org/jira/browse/HBASE-25278) | Add option to toggle CACHE\_BLOCKS in count.rb |  Minor | shell |
| [HBASE-18070](https://issues.apache.org/jira/browse/HBASE-18070) | Enable memstore replication for meta replica |  Critical | . |
| [HBASE-24528](https://issues.apache.org/jira/browse/HBASE-24528) | Improve balancer decision observability |  Major | Admin, Balancer, Operability, shell, UI |
| [HBASE-24776](https://issues.apache.org/jira/browse/HBASE-24776) | [hbtop] Support Batch mode |  Major | hbtop |
| [HBASE-24602](https://issues.apache.org/jira/browse/HBASE-24602) | Add Increment and Append support to CheckAndMutate |  Major | . |
| [HBASE-24760](https://issues.apache.org/jira/browse/HBASE-24760) | Add a config hbase.rsgroup.fallback.enable for RSGroup fallback feature |  Major | rsgroup |
| [HBASE-24694](https://issues.apache.org/jira/browse/HBASE-24694) | Support flush a single column family of table |  Major | . |
| [HBASE-24289](https://issues.apache.org/jira/browse/HBASE-24289) | Heterogeneous Storage for Date Tiered Compaction |  Major | Compaction |
| [HBASE-24038](https://issues.apache.org/jira/browse/HBASE-24038) | Add a metric to show the locality of ssd in table.jsp |  Major | metrics |
| [HBASE-8458](https://issues.apache.org/jira/browse/HBASE-8458) | Support for batch version of checkAndMutate() |  Major | Client, regionserver |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25339](https://issues.apache.org/jira/browse/HBASE-25339) | Method parameter and member variable are duplicated in checkSplittable() of SplitTableRegionProcedure |  Minor | . |
| [HBASE-25237](https://issues.apache.org/jira/browse/HBASE-25237) | 'hbase master stop' shuts down the cluster, not the master only |  Major | . |
| [HBASE-25324](https://issues.apache.org/jira/browse/HBASE-25324) | Remove unnecessary array to list conversion in SplitLogManager |  Minor | . |
| [HBASE-25263](https://issues.apache.org/jira/browse/HBASE-25263) | Change encryption key generation algorithm used in the HBase shell |  Major | encryption, shell |
| [HBASE-25323](https://issues.apache.org/jira/browse/HBASE-25323) | Fix potential NPE when the zookeeper path of RegionServerTracker does not exist when start |  Minor | . |
| [HBASE-25281](https://issues.apache.org/jira/browse/HBASE-25281) | Bulkload split hfile too many times due to unreasonable split point |  Minor | tooling |
| [HBASE-25325](https://issues.apache.org/jira/browse/HBASE-25325) | Remove unused class ClusterSchemaException |  Minor | . |
| [HBASE-25213](https://issues.apache.org/jira/browse/HBASE-25213) | Should request Compaction when bulkLoadHFiles is done |  Minor | . |
| [HBASE-24877](https://issues.apache.org/jira/browse/HBASE-24877) | Add option to avoid aborting RS process upon uncaught exceptions happen on replication source |  Major | Replication |
| [HBASE-24664](https://issues.apache.org/jira/browse/HBASE-24664) | Some changing of split region by overall region size rather than only one store size |  Major | regionserver |
| [HBASE-25026](https://issues.apache.org/jira/browse/HBASE-25026) | Create a metric to track full region scans RPCs |  Minor | . |
| [HBASE-25289](https://issues.apache.org/jira/browse/HBASE-25289) | [testing] Clean up resources after tests in rsgroup\_shell\_test.rb |  Major | rsgroup, test |
| [HBASE-25261](https://issues.apache.org/jira/browse/HBASE-25261) | Upgrade Bootstrap to 3.4.1 |  Major | security, UI |
| [HBASE-25272](https://issues.apache.org/jira/browse/HBASE-25272) | Support scan on a specific replica |  Critical | Client, scan |
| [HBASE-25267](https://issues.apache.org/jira/browse/HBASE-25267) | Add SSL keystore type and truststore related configs for HBase RESTServer |  Major | REST |
| [HBASE-25181](https://issues.apache.org/jira/browse/HBASE-25181) | Add options for disabling column family encryption and choosing hash algorithm for wrapped encryption keys. |  Major | encryption |
| [HBASE-25254](https://issues.apache.org/jira/browse/HBASE-25254) | Rewrite TestMultiLogThreshold to remove the LogDelegate in RSRpcServices |  Major | logging, test |
| [HBASE-25252](https://issues.apache.org/jira/browse/HBASE-25252) | Move HMaster inner classes out |  Minor | master |
| [HBASE-25240](https://issues.apache.org/jira/browse/HBASE-25240) | gson format of RpcServer.logResponse is abnormal |  Minor | . |
| [HBASE-25210](https://issues.apache.org/jira/browse/HBASE-25210) | RegionInfo.isOffline is now a duplication with RegionInfo.isSplit |  Major | meta |
| [HBASE-25212](https://issues.apache.org/jira/browse/HBASE-25212) | Optionally abort requests in progress after deciding a region should close |  Major | regionserver |
| [HBASE-24859](https://issues.apache.org/jira/browse/HBASE-24859) | Optimize in-memory representation of mapreduce TableSplit objects |  Major | mapreduce |
| [HBASE-24967](https://issues.apache.org/jira/browse/HBASE-24967) | The table.jsp cost long time to load if the table include closed regions |  Major | UI |
| [HBASE-25167](https://issues.apache.org/jira/browse/HBASE-25167) | Normalizer support for hot config reloading |  Major | master, Normalizer |
| [HBASE-24419](https://issues.apache.org/jira/browse/HBASE-24419) | Normalizer merge plans should consider more than 2 regions when possible |  Major | master, Normalizer |
| [HBASE-25224](https://issues.apache.org/jira/browse/HBASE-25224) | Maximize sleep for checking meta and namespace regions availability |  Major | master |
| [HBASE-25223](https://issues.apache.org/jira/browse/HBASE-25223) | Use try-with-resources statement in snapshot package |  Minor | . |
| [HBASE-25201](https://issues.apache.org/jira/browse/HBASE-25201) | YouAreDeadException should be moved to hbase-server module |  Major | Client |
| [HBASE-25193](https://issues.apache.org/jira/browse/HBASE-25193) | Add support for row prefix and type in the WAL Pretty Printer and some minor fixes |  Minor | wal |
| [HBASE-25128](https://issues.apache.org/jira/browse/HBASE-25128) | RSGroupInfo's toString() and hashCode() does not take into account configuration map. |  Minor | rsgroup |
| [HBASE-24628](https://issues.apache.org/jira/browse/HBASE-24628) | Region normalizer now respects a rate limit |  Major | Normalizer |
| [HBASE-25179](https://issues.apache.org/jira/browse/HBASE-25179) | Assert format is incorrect in HFilePerformanceEvaluation class. |  Minor | Performance, test |
| [HBASE-25065](https://issues.apache.org/jira/browse/HBASE-25065) | WAL archival to be done by a separate thread |  Major | wal |
| [HBASE-14067](https://issues.apache.org/jira/browse/HBASE-14067) | bundle ruby files for hbase shell into a jar. |  Major | shell |
| [HBASE-24875](https://issues.apache.org/jira/browse/HBASE-24875) | Remove the force param for unassign since it dose not take effect any more |  Major | Client |
| [HBASE-24025](https://issues.apache.org/jira/browse/HBASE-24025) | Improve performance of move\_servers\_rsgroup and move\_tables\_rsgroup by using async region move API |  Major | rsgroup |
| [HBASE-25160](https://issues.apache.org/jira/browse/HBASE-25160) | Refactor AccessController and VisibilityController |  Major | . |
| [HBASE-25146](https://issues.apache.org/jira/browse/HBASE-25146) | Add extra logging at info level to HFileCorruptionChecker in order to report progress |  Major | hbck, hbck2 |
| [HBASE-24054](https://issues.apache.org/jira/browse/HBASE-24054) | The Jetty's version number leak occurred while using the thrift service |  Minor | . |
| [HBASE-25091](https://issues.apache.org/jira/browse/HBASE-25091) | Move LogComparator from ReplicationSource to AbstractFSWALProvider#.WALsStartTimeComparator |  Minor | . |
| [HBASE-24981](https://issues.apache.org/jira/browse/HBASE-24981) | Enable table replication fails from 1.x to 2.x if table already exist at peer. |  Major | Replication |
| [HBASE-25109](https://issues.apache.org/jira/browse/HBASE-25109) | Add MR Counters to WALPlayer; currently hard to tell if it is doing anything |  Major | . |
| [HBASE-25082](https://issues.apache.org/jira/browse/HBASE-25082) | Per table WAL metrics: appendCount and appendSize |  Major | metrics |
| [HBASE-25079](https://issues.apache.org/jira/browse/HBASE-25079) | Upgrade Bootstrap to 3.3.7 |  Major | security, UI |
| [HBASE-24976](https://issues.apache.org/jira/browse/HBASE-24976) | REST Server failes to start without any error message |  Major | REST |
| [HBASE-25066](https://issues.apache.org/jira/browse/HBASE-25066) | Use FutureUtils.rethrow in AsyncTableResultScanner to better catch the stack trace |  Major | Client, Scanners |
| [HBASE-25069](https://issues.apache.org/jira/browse/HBASE-25069) |  Display region name instead of encoded region name in HBCK report page. |  Minor | hbck |
| [HBASE-24991](https://issues.apache.org/jira/browse/HBASE-24991) | Replace MovedRegionsCleaner with guava cache |  Minor | . |
| [HBASE-25057](https://issues.apache.org/jira/browse/HBASE-25057) | Fix typo "memeber" |  Trivial | documentation |
| [HBASE-24764](https://issues.apache.org/jira/browse/HBASE-24764) | Add support of adding base peer configs via hbase-site.xml for all replication peers. |  Minor | Replication |
| [HBASE-25037](https://issues.apache.org/jira/browse/HBASE-25037) | Lots of thread pool are changed to non daemon after HBASE-24750 which causes trouble when shutting down |  Major | . |
| [HBASE-24831](https://issues.apache.org/jira/browse/HBASE-24831) | Avoid invoke Counter using reflection  in SnapshotInputFormat |  Major | . |
| [HBASE-25002](https://issues.apache.org/jira/browse/HBASE-25002) | Create simple pattern matching query for retrieving metrics matching the pattern |  Major | . |
| [HBASE-25022](https://issues.apache.org/jira/browse/HBASE-25022) | Remove 'hbase.testing.nocluster' config |  Major | test |
| [HBASE-25006](https://issues.apache.org/jira/browse/HBASE-25006) | Make the cost functions optional for StochastoicBalancer |  Major | . |
| [HBASE-24974](https://issues.apache.org/jira/browse/HBASE-24974) | Provide a flexibility to print only row key and filter for multiple tables in the WALPrettyPrinter |  Minor | wal |
| [HBASE-24994](https://issues.apache.org/jira/browse/HBASE-24994) | Add hedgedReadOpsInCurThread metric |  Minor | metrics |
| [HBASE-25005](https://issues.apache.org/jira/browse/HBASE-25005) | Refactor CatalogJanitor |  Major | master, meta |
| [HBASE-24992](https://issues.apache.org/jira/browse/HBASE-24992) | log after Generator success when running ITBLL |  Trivial | . |
| [HBASE-24937](https://issues.apache.org/jira/browse/HBASE-24937) | table.rb use LocalDateTime to replace Instant |  Minor | shell |
| [HBASE-24940](https://issues.apache.org/jira/browse/HBASE-24940) | runCatalogJanitor() API should return -1 to indicate already running status |  Major | . |
| [HBASE-24973](https://issues.apache.org/jira/browse/HBASE-24973) | Remove read point parameter in method StoreFlush#performFlush and StoreFlush#createScanner |  Minor | . |
| [HBASE-24569](https://issues.apache.org/jira/browse/HBASE-24569) | Get hostAndWeights in addition using localhost if it is null in local mode |  Minor | regionserver |
| [HBASE-24949](https://issues.apache.org/jira/browse/HBASE-24949) | Optimize FSTableDescriptors.get to not always go to fs when cache miss |  Major | master |
| [HBASE-24898](https://issues.apache.org/jira/browse/HBASE-24898) | Use EnvironmentEdge.currentTime() instead of System.currentTimeMillis() in CurrentHourProvider |  Major | tooling |
| [HBASE-24942](https://issues.apache.org/jira/browse/HBASE-24942) | MergeTableRegionsProcedure should not call clean merge region |  Major | proc-v2, Region Assignment |
| [HBASE-24811](https://issues.apache.org/jira/browse/HBASE-24811) | Use class access static field or method |  Minor | . |
| [HBASE-24686](https://issues.apache.org/jira/browse/HBASE-24686) | [LOG] Log improvement in Connection#close |  Major | Client, logging |
| [HBASE-24912](https://issues.apache.org/jira/browse/HBASE-24912) | Enlarge MemstoreFlusherChore/CompactionChecker period for unit test |  Major | . |
| [HBASE-24627](https://issues.apache.org/jira/browse/HBASE-24627) | Normalize one table at a time |  Major | Normalizer |
| [HBASE-24872](https://issues.apache.org/jira/browse/HBASE-24872) | refactor valueOf PoolType |  Minor | Client |
| [HBASE-24854](https://issues.apache.org/jira/browse/HBASE-24854) | Correct the help content of assign and unassign commands in hbase shell |  Minor | shell |
| [HBASE-24750](https://issues.apache.org/jira/browse/HBASE-24750) | All executor service should start using guava ThreadFactory |  Major | . |
| [HBASE-24709](https://issues.apache.org/jira/browse/HBASE-24709) | Support MoveCostFunction use a lower multiplier in offpeak hours |  Major | Balancer |
| [HBASE-24824](https://issues.apache.org/jira/browse/HBASE-24824) | Add more stats in PE for read replica |  Minor | PE, read replicas |
| [HBASE-21721](https://issues.apache.org/jira/browse/HBASE-21721) | FSHLog : reduce write#syncs() times |  Major | . |
| [HBASE-24404](https://issues.apache.org/jira/browse/HBASE-24404) | Support flush a single column family of region |  Major | shell |
| [HBASE-24826](https://issues.apache.org/jira/browse/HBASE-24826) | Add some comments for processlist in hbase shell |  Minor | shell |
| [HBASE-24659](https://issues.apache.org/jira/browse/HBASE-24659) | Calculate FIXED\_OVERHEAD automatically |  Major | . |
| [HBASE-24827](https://issues.apache.org/jira/browse/HBASE-24827) | BackPort HBASE-11554 Remove Reusable poolmap Rpc client type. |  Major | Client |
| [HBASE-24823](https://issues.apache.org/jira/browse/HBASE-24823) | Port HBASE-22762 Print the delta between phases in the split/merge/compact/flush transaction journals to master branch |  Minor | . |
| [HBASE-24795](https://issues.apache.org/jira/browse/HBASE-24795) | RegionMover should deal with unknown (split/merged) regions |  Major | . |
| [HBASE-24821](https://issues.apache.org/jira/browse/HBASE-24821) | Simplify the logic of getRegionInfo in TestFlushFromClient to reduce redundancy code |  Minor | test |
| [HBASE-24704](https://issues.apache.org/jira/browse/HBASE-24704) | Make the Table Schema easier to view even there are multiple families |  Major | UI |
| [HBASE-24695](https://issues.apache.org/jira/browse/HBASE-24695) | FSHLog - close the current WAL file in a background thread |  Major | . |
| [HBASE-24803](https://issues.apache.org/jira/browse/HBASE-24803) | Unify hbase-shell ::Shell::Commands::Command#help behavior |  Minor | shell |
| [HBASE-11686](https://issues.apache.org/jira/browse/HBASE-11686) | Shell code should create a binding / irb workspace instead of polluting the root namespace |  Minor | shell |
| [HBASE-20226](https://issues.apache.org/jira/browse/HBASE-20226) | Performance Improvement Taking Large Snapshots In Remote Filesystems |  Minor | snapshots |
| [HBASE-24669](https://issues.apache.org/jira/browse/HBASE-24669) | Logging of ppid should be consistent across all occurrences |  Minor | Operability, proc-v2 |
| [HBASE-24757](https://issues.apache.org/jira/browse/HBASE-24757) | ReplicationSink should limit the batch rowcount for batch mutations based on hbase.rpc.rows.warning.threshold |  Major | . |
| [HBASE-24777](https://issues.apache.org/jira/browse/HBASE-24777) | InfoServer support ipv6 host and port |  Minor | UI |
| [HBASE-24758](https://issues.apache.org/jira/browse/HBASE-24758) | Avoid flooding replication source RSes logs when no sinks are available |  Major | Replication |
| [HBASE-24743](https://issues.apache.org/jira/browse/HBASE-24743) | Reject to add a peer which replicate to itself earlier |  Major | . |
| [HBASE-24696](https://issues.apache.org/jira/browse/HBASE-24696) | Include JVM information on Web UI under "Software Attributes" |  Minor | UI |
| [HBASE-24747](https://issues.apache.org/jira/browse/HBASE-24747) | Log an ERROR if HBaseSaslRpcServer initialisation fails with an uncaught exception |  Major | . |
| [HBASE-24586](https://issues.apache.org/jira/browse/HBASE-24586) | Add table level locality in table.jsp |  Major | UI |
| [HBASE-24663](https://issues.apache.org/jira/browse/HBASE-24663) | Add procedure process time statistics UI |  Major | . |
| [HBASE-24653](https://issues.apache.org/jira/browse/HBASE-24653) | Show snapshot owner on Master WebUI |  Major | . |
| [HBASE-24431](https://issues.apache.org/jira/browse/HBASE-24431) | RSGroupInfo add configuration map to store something extra |  Major | rsgroup |
| [HBASE-24671](https://issues.apache.org/jira/browse/HBASE-24671) | Add excludefile and designatedfile options to graceful\_stop.sh |  Major | . |
| [HBASE-24560](https://issues.apache.org/jira/browse/HBASE-24560) | Add a new option of designatedfile in RegionMover |  Major | . |
| [HBASE-24382](https://issues.apache.org/jira/browse/HBASE-24382) | Flush partial stores of region filtered by seqId when archive wal due to too many wals |  Major | wal |
| [HBASE-24208](https://issues.apache.org/jira/browse/HBASE-24208) | Remove RS entry from zk draining servers node after RS been stopped |  Major | . |
| [HBASE-24456](https://issues.apache.org/jira/browse/HBASE-24456) | Immutable Scan as unmodifiable subclass or wrapper of Scan |  Major | . |
| [HBASE-24471](https://issues.apache.org/jira/browse/HBASE-24471) | The way we bootstrap meta table is confusing |  Major | master, meta, proc-v2 |
| [HBASE-24350](https://issues.apache.org/jira/browse/HBASE-24350) | HBase table level replication metrics for shippedBytes are always 0 |  Major | Replication |
| [HBASE-24311](https://issues.apache.org/jira/browse/HBASE-24311) | Add more details in MultiVersionConcurrencyControl STUCK log message |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25355](https://issues.apache.org/jira/browse/HBASE-25355) | [Documentation]  Fix spelling error |  Trivial | documentation |
| [HBASE-25230](https://issues.apache.org/jira/browse/HBASE-25230) | Embedded zookeeper server not clean up the old data |  Minor | Zookeeper |
| [HBASE-25349](https://issues.apache.org/jira/browse/HBASE-25349) | [Flakey Tests] branch-2 TestRefreshRecoveredReplication.testReplicationRefreshSource:141 Waiting timed out after [60,000] msec |  Major | flakies |
| [HBASE-25332](https://issues.apache.org/jira/browse/HBASE-25332) | one NPE |  Major | Zookeeper |
| [HBASE-25345](https://issues.apache.org/jira/browse/HBASE-25345) | [Flakey Tests] branch-2 TestReadReplicas#testVerifySecondaryAbilityToReadWithOnFiles |  Major | test |
| [HBASE-25307](https://issues.apache.org/jira/browse/HBASE-25307) | ThreadLocal pooling leads to NullPointerException |  Major | Client |
| [HBASE-25341](https://issues.apache.org/jira/browse/HBASE-25341) | Fix ErrorProne error which causes nightly to fail |  Major | test |
| [HBASE-25330](https://issues.apache.org/jira/browse/HBASE-25330) | RSGroupInfoManagerImpl#moveServers return is not set of servers moved |  Major | rsgroup |
| [HBASE-25321](https://issues.apache.org/jira/browse/HBASE-25321) | The sort icons not shown after Upgrade JQuery to 3.5.1 |  Major | UI |
| [HBASE-24268](https://issues.apache.org/jira/browse/HBASE-24268) | REST and Thrift server do not handle the "doAs" parameter case insensitively |  Minor | REST, Thrift |
| [HBASE-25050](https://issues.apache.org/jira/browse/HBASE-25050) | We initialize Filesystems more than once. |  Minor | . |
| [HBASE-25311](https://issues.apache.org/jira/browse/HBASE-25311) | ui throws NPE |  Major | . |
| [HBASE-25306](https://issues.apache.org/jira/browse/HBASE-25306) | The log in SimpleLoadBalancer#onConfigurationChange is wrong |  Major | . |
| [HBASE-25300](https://issues.apache.org/jira/browse/HBASE-25300) | 'Unknown table hbase:quota' happens when desc table in shell if quota disabled |  Major | shell |
| [HBASE-25255](https://issues.apache.org/jira/browse/HBASE-25255) | Master fails to initialize when creating rs group table |  Critical | master, rsgroup |
| [HBASE-25275](https://issues.apache.org/jira/browse/HBASE-25275) | Upgrade asciidoctor |  Blocker | website |
| [HBASE-25276](https://issues.apache.org/jira/browse/HBASE-25276) | Need to throw the original exception in HRegion#openHRegion |  Major | . |
| [HBASE-20598](https://issues.apache.org/jira/browse/HBASE-20598) | Upgrade to JRuby 9.2 |  Major | dependencies, shell |
| [HBASE-25216](https://issues.apache.org/jira/browse/HBASE-25216) | The client zk syncer should deal with meta replica count change |  Major | master, Zookeeper |
| [HBASE-25238](https://issues.apache.org/jira/browse/HBASE-25238) | Upgrading HBase from 2.2.0 to 2.3.x fails because of “Message missing required fields: state” |  Critical | . |
| [HBASE-25234](https://issues.apache.org/jira/browse/HBASE-25234) | [Upgrade]Incompatibility in reading RS report from 2.1 RS when Master is upgraded to a version containing HBASE-21406 |  Major | . |
| [HBASE-25053](https://issues.apache.org/jira/browse/HBASE-25053) | WAL replay should ignore 0-length files |  Major | master, regionserver |
| [HBASE-25090](https://issues.apache.org/jira/browse/HBASE-25090) | CompactionConfiguration logs unrealistic store file sizes |  Minor | Compaction |
| [HBASE-24977](https://issues.apache.org/jira/browse/HBASE-24977) | Meta table shouldn't be modified as read only |  Major | meta |
| [HBASE-25176](https://issues.apache.org/jira/browse/HBASE-25176) | MasterStoppedException should be moved to hbase-client module |  Major | Client |
| [HBASE-25206](https://issues.apache.org/jira/browse/HBASE-25206) | Data loss can happen if a cloned table loses original split region(delete table) |  Major | proc-v2, Region Assignment, snapshots |
| [HBASE-25207](https://issues.apache.org/jira/browse/HBASE-25207) | Revisit the implementation and usage of RegionStates.include |  Major | Region Assignment |
| [HBASE-25186](https://issues.apache.org/jira/browse/HBASE-25186) | TestMasterRegionOnTwoFileSystems is failing after HBASE-25065 |  Blocker | master |
| [HBASE-25204](https://issues.apache.org/jira/browse/HBASE-25204) | Nightly job failed as  the name of jdk and maven changed |  Major | . |
| [HBASE-25093](https://issues.apache.org/jira/browse/HBASE-25093) | the RSGroupBasedLoadBalancer#retainAssignment throws NPE |  Major | rsgroup |
| [HBASE-25117](https://issues.apache.org/jira/browse/HBASE-25117) | ReplicationSourceShipper thread can not be finished |  Major | . |
| [HBASE-25168](https://issues.apache.org/jira/browse/HBASE-25168) | Unify WAL name timestamp parsers |  Major | . |
| [HBASE-23834](https://issues.apache.org/jira/browse/HBASE-23834) | HBase fails to run on Hadoop 3.3.0/3.2.2/3.1.4 due to jetty version mismatch |  Major | dependencies |
| [HBASE-25165](https://issues.apache.org/jira/browse/HBASE-25165) | Change 'State time' in UI so sorts |  Minor | UI |
| [HBASE-25048](https://issues.apache.org/jira/browse/HBASE-25048) | [HBCK2] Bypassed parent procedures are not updated in store |  Major | hbck2, proc-v2 |
| [HBASE-25147](https://issues.apache.org/jira/browse/HBASE-25147) | Should store the regionNames field in state data for ReopenTableRegionsProcedure |  Major | proc-v2 |
| [HBASE-25115](https://issues.apache.org/jira/browse/HBASE-25115) | HFilePrettyPrinter can't seek to the row which is the first row of a hfile |  Major | HFile, tooling |
| [HBASE-25135](https://issues.apache.org/jira/browse/HBASE-25135) | Convert the internal seperator while emitting the memstore read metrics to # |  Minor | . |
| [HBASE-24665](https://issues.apache.org/jira/browse/HBASE-24665) | MultiWAL :  Avoid rolling of ALL WALs when one of the WAL needs a roll |  Major | wal |
| [HBASE-25096](https://issues.apache.org/jira/browse/HBASE-25096) | WAL size in RegionServer UI is wrong |  Major | . |
| [HBASE-25077](https://issues.apache.org/jira/browse/HBASE-25077) | hbck.jsp page loading fails, logs NPE in master log. |  Major | hbck |
| [HBASE-25088](https://issues.apache.org/jira/browse/HBASE-25088) | CatalogFamilyFormat/MetaTableAccessor.parseRegionInfoFromRegionName incorrectly setEndKey to regionId |  Critical | meta |
| [HBASE-25097](https://issues.apache.org/jira/browse/HBASE-25097) | Wrong RIT page number in Master UI |  Minor | UI |
| [HBASE-24896](https://issues.apache.org/jira/browse/HBASE-24896) | 'Stuck' in static initialization creating RegionInfo instance |  Major | . |
| [HBASE-24956](https://issues.apache.org/jira/browse/HBASE-24956) | ConnectionManager#locateRegionInMeta waits for user region lock indefinitely. |  Major | Client |
| [HBASE-24481](https://issues.apache.org/jira/browse/HBASE-24481) | HBase Rest: Request for region detail of a table which doesn't exits is success(200 success code) instead of 404 |  Minor | . |
| [HBASE-25047](https://issues.apache.org/jira/browse/HBASE-25047) | WAL split edits number is negative in RegionServerUI |  Minor | UI, wal |
| [HBASE-25021](https://issues.apache.org/jira/browse/HBASE-25021) | Nightly job should skip hadoop-2 integration test for master |  Major | build, scripts |
| [HBASE-25012](https://issues.apache.org/jira/browse/HBASE-25012) | HBASE-24359 causes replication missed log of some RemoteException |  Major | Replication |
| [HBASE-25009](https://issues.apache.org/jira/browse/HBASE-25009) | Hbck chore logs wrong message when loading regions from RS report |  Minor | . |
| [HBASE-25014](https://issues.apache.org/jira/browse/HBASE-25014) | ScheduledChore is never triggered when initalDelay \> 1.5\*period |  Major | . |
| [HBASE-25016](https://issues.apache.org/jira/browse/HBASE-25016) | Should close ResultScanner in MetaTableAccessor.scanByRegionEncodedName |  Critical | master, meta |
| [HBASE-24958](https://issues.apache.org/jira/browse/HBASE-24958) | CompactingMemStore.timeOfOldestEdit error update |  Critical | regionserver |
| [HBASE-24995](https://issues.apache.org/jira/browse/HBASE-24995) | MetaFixer fails to fix overlaps when multiple tables have overlaps |  Major | hbck2 |
| [HBASE-24719](https://issues.apache.org/jira/browse/HBASE-24719) | Renaming invalid rsgroup throws NPE instead of proper error message |  Major | . |
| [HBASE-19352](https://issues.apache.org/jira/browse/HBASE-19352) | Port HADOOP-10379: Protect authentication cookies with the HttpOnly and Secure flags |  Major | . |
| [HBASE-24971](https://issues.apache.org/jira/browse/HBASE-24971) | Upgrade JQuery to 3.5.1 |  Major | security, UI |
| [HBASE-24968](https://issues.apache.org/jira/browse/HBASE-24968) | One of static initializers of CellComparatorImpl referring to subclass MetaCellComparator |  Major | . |
| [HBASE-24916](https://issues.apache.org/jira/browse/HBASE-24916) | Region hole contains wrong regions pair when hole is created by first region deletion |  Major | hbck2 |
| [HBASE-24885](https://issues.apache.org/jira/browse/HBASE-24885) | STUCK RIT by hbck2 assigns |  Major | hbck2, Region Assignment |
| [HBASE-24926](https://issues.apache.org/jira/browse/HBASE-24926) | Should call setFailure in MergeTableRegionsProcedure when isMergeable returns false |  Major | master, proc-v2 |
| [HBASE-24884](https://issues.apache.org/jira/browse/HBASE-24884) | BulkLoadHFilesTool/LoadIncrementalHFiles should accept -D options from command line parameters |  Minor | . |
| [HBASE-24583](https://issues.apache.org/jira/browse/HBASE-24583) | Normalizer can't actually merge empty regions when neighbor is larger than average size |  Major | master, Normalizer |
| [HBASE-24844](https://issues.apache.org/jira/browse/HBASE-24844) | Exception on standalone (master) shutdown |  Minor | Zookeeper |
| [HBASE-24856](https://issues.apache.org/jira/browse/HBASE-24856) | Fix error prone error in FlushTableSubprocedure |  Major | . |
| [HBASE-24838](https://issues.apache.org/jira/browse/HBASE-24838) | The pre commit job fails to archive surefire reports |  Critical | build, scripts |
| [HBASE-23157](https://issues.apache.org/jira/browse/HBASE-23157) | WAL unflushed seqId tracking may wrong when Durability.ASYNC\_WAL is used |  Major | regionserver, wal |
| [HBASE-24625](https://issues.apache.org/jira/browse/HBASE-24625) | AsyncFSWAL.getLogFileSizeIfBeingWritten does not return the expected synced file length. |  Critical | Replication, wal |
| [HBASE-24830](https://issues.apache.org/jira/browse/HBASE-24830) | Some tests involving RS crash fail with NullPointerException after HBASE-24632 in branch-2 |  Major | . |
| [HBASE-24788](https://issues.apache.org/jira/browse/HBASE-24788) | Fix the connection leaks on getting hbase admin from unclosed connection |  Major | mapreduce |
| [HBASE-24805](https://issues.apache.org/jira/browse/HBASE-24805) | HBaseTestingUtility.getConnection should be threadsafe |  Major | test |
| [HBASE-24808](https://issues.apache.org/jira/browse/HBASE-24808) | skip empty log cleaner delegate class names (WAS =\> cleaner.CleanerChore: Can NOT create CleanerDelegate= ClassNotFoundException) |  Trivial | . |
| [HBASE-24767](https://issues.apache.org/jira/browse/HBASE-24767) | Change default to false for HBASE-15519 per-user metrics |  Major | metrics |
| [HBASE-24713](https://issues.apache.org/jira/browse/HBASE-24713) | RS startup with FSHLog throws NPE after HBASE-21751 |  Minor | wal |
| [HBASE-24794](https://issues.apache.org/jira/browse/HBASE-24794) | hbase.rowlock.wait.duration should not be \<= 0 |  Minor | regionserver |
| [HBASE-24797](https://issues.apache.org/jira/browse/HBASE-24797) | Move log code out of loop |  Minor | Normalizer |
| [HBASE-24752](https://issues.apache.org/jira/browse/HBASE-24752) | NPE/500 accessing webui on master startup |  Minor | master |
| [HBASE-24766](https://issues.apache.org/jira/browse/HBASE-24766) | Document Remote Procedure Execution |  Major | documentation |
| [HBASE-11676](https://issues.apache.org/jira/browse/HBASE-11676) | Scan FORMATTER is not applied for columns using non-printable name in shell |  Minor | shell |
| [HBASE-24738](https://issues.apache.org/jira/browse/HBASE-24738) | [Shell] processlist command fails with ERROR: Unexpected end of file from server when SSL enabled |  Major | shell |
| [HBASE-24675](https://issues.apache.org/jira/browse/HBASE-24675) | On Master restart all servers are assigned to default rsgroup. |  Major | rsgroup |
| [HBASE-22146](https://issues.apache.org/jira/browse/HBASE-22146) | SpaceQuotaViolationPolicy Disable is not working in Namespace level |  Major | . |
| [HBASE-24742](https://issues.apache.org/jira/browse/HBASE-24742) | Improve performance of SKIP vs SEEK logic |  Major | Performance, regionserver |
| [HBASE-24710](https://issues.apache.org/jira/browse/HBASE-24710) | Incorrect checksum calculation in saveVersion.sh |  Major | scripts |
| [HBASE-24714](https://issues.apache.org/jira/browse/HBASE-24714) | Error message is displayed in the UI of table's compaction state if any region of that table is not open. |  Major | Compaction, UI |
| [HBASE-24748](https://issues.apache.org/jira/browse/HBASE-24748) | Add hbase.master.balancer.stochastic.moveCost.offpeak to doc as support dynamically change |  Minor | documentation |
| [HBASE-24746](https://issues.apache.org/jira/browse/HBASE-24746) | The sort icons overlap the col name in master UI |  Major | UI |
| [HBASE-24721](https://issues.apache.org/jira/browse/HBASE-24721) | rename\_rsgroup overwriting the existing rsgroup. |  Major | . |
| [HBASE-24615](https://issues.apache.org/jira/browse/HBASE-24615) | MutableRangeHistogram#updateSnapshotRangeMetrics doesn't calculate the distribution for last bucket. |  Major | metrics |
| [HBASE-24705](https://issues.apache.org/jira/browse/HBASE-24705) | MetaFixer#fixHoles() does not include the case for read replicas (i.e, replica regions are not created) |  Major | read replicas |
| [HBASE-24720](https://issues.apache.org/jira/browse/HBASE-24720) | Meta replicas not cleaned when disabled |  Minor | read replicas |
| [HBASE-24693](https://issues.apache.org/jira/browse/HBASE-24693) | regioninfo#isLast() has a logic error |  Minor | . |
| [HBASE-23744](https://issues.apache.org/jira/browse/HBASE-23744) | FastPathBalancedQueueRpcExecutor should enforce queue length of 0 |  Minor | . |
| [HBASE-22738](https://issues.apache.org/jira/browse/HBASE-22738) | Fallback to default group to choose RS when there are no RS in current group |  Major | rsgroup |
| [HBASE-23126](https://issues.apache.org/jira/browse/HBASE-23126) | IntegrationTestRSGroup is useless now |  Major | rsgroup |
| [HBASE-24518](https://issues.apache.org/jira/browse/HBASE-24518) | waitForNamespaceOnline() should return false if any region is offline |  Major | . |
| [HBASE-24564](https://issues.apache.org/jira/browse/HBASE-24564) | Make RS abort call idempotent |  Major | regionserver |
| [HBASE-24340](https://issues.apache.org/jira/browse/HBASE-24340) | PerformanceEvaluation options should not mandate any specific order |  Minor | . |
| [HBASE-24130](https://issues.apache.org/jira/browse/HBASE-24130) | rat plugin complains about having an unlicensed file. |  Minor | . |
| [HBASE-24017](https://issues.apache.org/jira/browse/HBASE-24017) | Turn down flakey rerun rate on all but hot branches |  Major | . |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-24015](https://issues.apache.org/jira/browse/HBASE-24015) | Coverage for Assign and Unassign of Regions on RegionServer on failure |  Major | amv2 |
| [HBASE-25156](https://issues.apache.org/jira/browse/HBASE-25156) | TestMasterFailover.testSimpleMasterFailover is flaky |  Major | test |
| [HBASE-24979](https://issues.apache.org/jira/browse/HBASE-24979) | Include batch mutatations in client operation timeout tests |  Major | . |
| [HBASE-24894](https://issues.apache.org/jira/browse/HBASE-24894) | [Flakey Test] TestStochasticLoadBalancer.testMoveCostMultiplier |  Major | Balancer, master, test |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25127](https://issues.apache.org/jira/browse/HBASE-25127) | Enhance PerformanceEvaluation to profile meta replica performance. |  Major | . |
| [HBASE-25284](https://issues.apache.org/jira/browse/HBASE-25284) | Check-in "Enable memstore replication..." design |  Major | . |
| [HBASE-25126](https://issues.apache.org/jira/browse/HBASE-25126) | Add load balance logic in hbase-client to distribute read load over meta replica regions. |  Major | . |
| [HBASE-25291](https://issues.apache.org/jira/browse/HBASE-25291) | Document how to enable the meta replica load balance mode for the client and clean up around hbase:meta read replicas |  Major | . |
| [HBASE-25253](https://issues.apache.org/jira/browse/HBASE-25253) | Deprecated master carrys regions related methods and configs |  Major | Balancer, master |
| [HBASE-25203](https://issues.apache.org/jira/browse/HBASE-25203) | Change the reference url to flaky list in our jenkins jobs |  Major | flakies, jenkins |
| [HBASE-25194](https://issues.apache.org/jira/browse/HBASE-25194) | Do not publish workspace in flaky find job |  Major | jenkins |
| [HBASE-25169](https://issues.apache.org/jira/browse/HBASE-25169) | Update documentation about meta region replica |  Major | documentation |
| [HBASE-25164](https://issues.apache.org/jira/browse/HBASE-25164) | Make ModifyTableProcedure support changing meta replica count |  Major | meta, read replicas |
| [HBASE-25162](https://issues.apache.org/jira/browse/HBASE-25162) | Make flaky tests run more aggressively |  Major | jenkins, scripts, test |
| [HBASE-25163](https://issues.apache.org/jira/browse/HBASE-25163) | Increase the timeout value for nightly jobs |  Major | jenkins, scripts, test |
| [HBASE-22976](https://issues.apache.org/jira/browse/HBASE-22976) | [HBCK2] Add RecoveredEditsPlayer |  Major | hbck2, walplayer |
| [HBASE-25124](https://issues.apache.org/jira/browse/HBASE-25124) | Support changing region replica count without disabling table |  Major | meta, proc-v2 |
| [HBASE-23959](https://issues.apache.org/jira/browse/HBASE-23959) | Fix javadoc for JDK11 |  Major | . |
| [HBASE-25151](https://issues.apache.org/jira/browse/HBASE-25151) | warmupRegion frustrates registering WALs on the catalog replicationsource |  Major | read replicas |
| [HBASE-25154](https://issues.apache.org/jira/browse/HBASE-25154) | Set java.io.tmpdir to project build directory to avoid writing std\*deferred files to /tmp |  Major | build, test |
| [HBASE-25121](https://issues.apache.org/jira/browse/HBASE-25121) | Refactor MetaTableAccessor.addRegionsToMeta and its usage places |  Major | meta |
| [HBASE-25055](https://issues.apache.org/jira/browse/HBASE-25055) | Add ReplicationSource for meta WALs; add enable/disable when hbase:meta assigned to RS |  Major | . |
| [HBASE-25133](https://issues.apache.org/jira/browse/HBASE-25133) | Migrate HBase Nightly jenkins job from Hadoop to hbase |  Major | jenkins, scripts |
| [HBASE-25132](https://issues.apache.org/jira/browse/HBASE-25132) | Migrate flaky test jenkins job from Hadoop to hbase |  Major | jenkins, scripts |
| [HBASE-25103](https://issues.apache.org/jira/browse/HBASE-25103) | Remove ZNodePaths.metaReplicaZNodes |  Major | . |
| [HBASE-25107](https://issues.apache.org/jira/browse/HBASE-25107) | Migrate flaky reporting jenkins job from Hadoop to hbase |  Major | jenkins, scripts |
| [HBASE-25068](https://issues.apache.org/jira/browse/HBASE-25068) | Pass WALFactory to Replication so it knows of all WALProviders, not just default/user-space |  Minor | . |
| [HBASE-25067](https://issues.apache.org/jira/browse/HBASE-25067) | Edit of log messages around async WAL Replication; checkstyle fixes; and a bugfix |  Major | . |
| [HBASE-24857](https://issues.apache.org/jira/browse/HBASE-24857) |  Fix several problems when starting webUI |  Minor | canary, UI |
| [HBASE-24964](https://issues.apache.org/jira/browse/HBASE-24964) | Remove MetaTableAccessor.tableExists |  Major | meta |
| [HBASE-24765](https://issues.apache.org/jira/browse/HBASE-24765) | Dynamic master discovery |  Major | Client |
| [HBASE-24945](https://issues.apache.org/jira/browse/HBASE-24945) | Remove MetaTableAccessor.getRegionCount |  Major | mapreduce, meta |
| [HBASE-24944](https://issues.apache.org/jira/browse/HBASE-24944) | Remove MetaTableAccessor.getTableRegionsAndLocations in hbase-rest module |  Major | meta, REST |
| [HBASE-24918](https://issues.apache.org/jira/browse/HBASE-24918) | Make RegionInfo#UNDEFINED IA.Private |  Major | . |
| [HBASE-24806](https://issues.apache.org/jira/browse/HBASE-24806) | Small Updates to Functionality of Shell IRB Workspace |  Major | shell |
| [HBASE-24876](https://issues.apache.org/jira/browse/HBASE-24876) | Fix the flaky job url in hbase-personality.sh |  Major | . |
| [HBASE-24841](https://issues.apache.org/jira/browse/HBASE-24841) | Change the jenkins job urls in our jenkinsfile |  Major | build, scripts |
| [HBASE-24680](https://issues.apache.org/jira/browse/HBASE-24680) | Refactor the checkAndMutate code on the server side |  Major | . |
| [HBASE-24817](https://issues.apache.org/jira/browse/HBASE-24817) | Allow configuring WALEntry filters on ReplicationSource |  Major | Replication, wal |
| [HBASE-24632](https://issues.apache.org/jira/browse/HBASE-24632) | Enable procedure-based log splitting as default in hbase3 |  Major | wal |
| [HBASE-24718](https://issues.apache.org/jira/browse/HBASE-24718) | Generic NamedQueue framework for recent in-memory history (refactor slowlog) |  Major | . |
| [HBASE-24698](https://issues.apache.org/jira/browse/HBASE-24698) | Turn OFF Canary WebUI as default |  Major | canary |
| [HBASE-24650](https://issues.apache.org/jira/browse/HBASE-24650) | Change the return types of the new checkAndMutate methods introduced in HBASE-8458 |  Major | Client |
| [HBASE-24013](https://issues.apache.org/jira/browse/HBASE-24013) | Bump branch-2 version to 2.4.0-SNAPSHOT |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [HBASE-25099](https://issues.apache.org/jira/browse/HBASE-25099) | Change meta replica count by altering meta table descriptor |  Major | meta, read replicas |
| [HBASE-25320](https://issues.apache.org/jira/browse/HBASE-25320) | Upgrade hbase-thirdparty dependency to 3.4.1 |  Blocker | dependencies |
| [HBASE-24640](https://issues.apache.org/jira/browse/HBASE-24640) | Purge use of VisibleForTesting |  Major | community |
| [HBASE-24081](https://issues.apache.org/jira/browse/HBASE-24081) | Provide documentation for running Yetus with HBase |  Major | documentation |
| [HBASE-24667](https://issues.apache.org/jira/browse/HBASE-24667) | Rename configs that support atypical DNS set ups to put them in hbase.unsafe |  Major | conf, Operability |
| [HBASE-25228](https://issues.apache.org/jira/browse/HBASE-25228) | Delete dev-support/jenkins\_precommit\_jira\_yetus.sh |  Minor | build |
| [HBASE-24200](https://issues.apache.org/jira/browse/HBASE-24200) | Upgrade to Yetus 0.12.0 |  Minor | build |
| [HBASE-25120](https://issues.apache.org/jira/browse/HBASE-25120) | Remove the deprecated annotation for MetaTableAccessor.getScanForTableName |  Major | meta |
| [HBASE-25073](https://issues.apache.org/jira/browse/HBASE-25073) | Should not use XXXService.Interface.class.getSimpleName as stub key prefix in AsyncConnectionImpl |  Major | Client |
| [HBASE-25072](https://issues.apache.org/jira/browse/HBASE-25072) | Remove the unnecessary System.out.println in MasterRegistry |  Minor | Client |
| [HBASE-25004](https://issues.apache.org/jira/browse/HBASE-25004) | Log RegionTooBusyException details |  Major | . |
| [HBASE-24993](https://issues.apache.org/jira/browse/HBASE-24993) | Remove OfflineMetaRebuildTestCore |  Major | test |
| [HBASE-24809](https://issues.apache.org/jira/browse/HBASE-24809) | Yetus IA Javadoc links are no longer available |  Minor | . |
| [HBASE-14847](https://issues.apache.org/jira/browse/HBASE-14847) | Add FIFO compaction section to HBase book |  Major | documentation |
| [HBASE-24843](https://issues.apache.org/jira/browse/HBASE-24843) | Sort the constants in \`hbase\_constants.rb\` |  Minor | shell |
| [HBASE-24835](https://issues.apache.org/jira/browse/HBASE-24835) | Normalizer should log a successful run at INFO level |  Minor | Normalizer |
| [HBASE-24779](https://issues.apache.org/jira/browse/HBASE-24779) | Improve insight into replication WAL readers hung on checkQuota |  Minor | Replication |
| [HBASE-24662](https://issues.apache.org/jira/browse/HBASE-24662) | Update DumpClusterStatusAction to notice changes in region server count |  Major | integration tests |
| [HBASE-24658](https://issues.apache.org/jira/browse/HBASE-24658) | Update PolicyBasedChaosMonkey to handle uncaught exceptions |  Minor | integration tests |
| [HBASE-24648](https://issues.apache.org/jira/browse/HBASE-24648) | Remove the legacy 'forceSplit' related code at region server side |  Major | regionserver |
| [HBASE-24492](https://issues.apache.org/jira/browse/HBASE-24492) | ProtobufLogReader.readNext does not need looping |  Minor | Replication, wal |
| [HBASE-22033](https://issues.apache.org/jira/browse/HBASE-22033) | Update to maven-javadoc-plugin 3.2.0 and switch to non-forking aggregate goals |  Major | build, website |


## Release 2.3.0

[CHANGES.md at rel/2.3.0 (e0e1382)](https://github.com/apache/hbase/blob/rel/2.3.0/CHANGES.md)

## Release 2.2.0

[CHANGES.md at rel/2.2.0 (3ec6932)](https://github.com/apache/hbase/blob/rel/2.2.0/CHANGES.md)

## Release 2.1.0

[CHANGES.md at rel/2.1.0 (e1673bb)](https://github.com/apache/hbase/blob/rel/2.1.0/CHANGES.md)

## Release 2.0.0

[CHANGES.md at rel/2.0.0 (7483b11)](https://github.com/apache/hbase/blob/rel/2.0.0/CHANGES.md)

## Release 1.0.0

[CHANGES.txt at rel/1.0.0 (6c98bff)](https://github.com/apache/hbase/blob/rel/1.0.0/CHANGES.txt)
