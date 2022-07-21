/*
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
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.io.hfile.FirstLevelBlockCache;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestCombinedBlockCacheVictimHandler {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCombinedBlockCacheVictimHandler.class);

  private static final int BLOCK_SIZE = 16 * 1024;

  CombinedBlockCache cache;

  @Before
  public void setUp() throws Exception {
    FirstLevelBlockCache l1 = new LruBlockCache(10 * BLOCK_SIZE, BLOCK_SIZE, false);
    // make 2 bigger so it can hold everything from both if need be
    BucketCache l2 = new BucketCache("offheap", l1.getMaxSize() * 10, BLOCK_SIZE,
      new int[] { BLOCK_SIZE + 1024 }, 1, 100, null);
    cache = new CombinedBlockCache(l1, l2, true);
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.shutdown();
    }
  }

  /**
   * Tests that victim cache handling properly evicts META blocks from L1 to L2 when appropriate,
   * and caches back to L1 when accessed in L2.
   */
  @Test
  public void testWithVictimHandler() throws IOException {
    HFileBlock metaBlock = createBlock(BlockType.META);
    HFileBlock dataBlock = createBlock(BlockType.DATA);

    cache("initial-meta", metaBlock);
    cache("initial-data", dataBlock);

    assertEquals(1, cache.getFirstLevelCache().getBlockCount());
    assertEquals(1, cache.getSecondLevelCache().getBlockCount());

    List<String> hfiles = new ArrayList<>();

    // fill up the L1 cache
    while (cache.getStats().getEvictedCount() == 0) {
      HFileBlock nextMetaBlock = createBlock(BlockType.META);
      String hfileName = "next-meta-" + hfiles.size();
      hfiles.add(hfileName);
      cache(hfileName, nextMetaBlock);
      // going to keep requesting initial-meta so it doesn't get evicted as we fill up cache
      assertNotNull(getBlockWithCaching("initial-meta", BlockType.META));
    }

    // an eviction occurred, but all blocks should still be in the combined cache
    assertEquals(hfiles.size() + 2, cache.getBlockCount());
    // l2 should have more than the original 1, indicating it took the evictions
    assertTrue(cache.getSecondLevelCache().getBlockCount() > 1);

    // we're going to cause some evictions and re-caches, so record the block count snow
    long initialL1Count = cache.getFirstLevelCache().getBlockCount();
    long initialL2Count = cache.getSecondLevelCache().getBlockCount();

    // we expand the l1. when we check all blocks exist below, this should cache all of the
    // meta blocks back in the l1
    cache.getFirstLevelCache().setMaxSize(cache.getCurrentSize() * 2);

    // further check all blocks exist in the overall cache, and refill L1
    for (String hfile : hfiles) {
      assertNotNull(getBlockWithCaching(hfile, BlockType.META));
    }
    assertNotNull(getBlockWithCaching("initial-meta", BlockType.META));
    assertNotNull(getBlockWithCaching("initial-data", BlockType.DATA));

    // meta blocks should have shifted back to l1, but l2 should remain unchanged
    assertTrue(cache.getFirstLevelCache().getBlockCount() > initialL1Count);
    assertEquals(initialL2Count, cache.getSecondLevelCache().getBlockCount());

    initialL1Count = cache.getFirstLevelCache().getBlockCount();
    initialL2Count = cache.getSecondLevelCache().getBlockCount();

    cache.getFirstLevelCache().setMaxSize(cache.getFirstLevelCache().getCurrentSize() - BLOCK_SIZE);

    // check that it shifted back again
    assertTrue(cache.getFirstLevelCache().getBlockCount() < initialL1Count);
    assertTrue(cache.getSecondLevelCache().getBlockCount() > initialL2Count);

    // this should cause eviction from both levels
    for (String hfile : hfiles) {
      cache.evictBlocksByHfileName(hfile);
    }

    // check that we end up back at our original state
    assertEquals(1, cache.getFirstLevelCache().getBlockCount());
    assertEquals(0, cache.getFirstLevelCache().getDataBlockCount());
    assertEquals(1, cache.getSecondLevelCache().getBlockCount());
    assertEquals(1, cache.getSecondLevelCache().getDataBlockCount());
  }

  private void cache(String hfileName, HFileBlock block) {
    BlockCacheKey key = new BlockCacheKey(hfileName, 0);
    int length = block.getOnDiskSizeWithHeader();
    ByteBuffer actualBuffer = ByteBuffer.allocate(length);
    ByteBuffer blockBuffer = ByteBuffer.allocate(length);
    block.serialize(blockBuffer, true);
    CacheTestUtils.getBlockAndAssertEquals(cache, key, block, actualBuffer, blockBuffer);
  }

  private Cacheable getBlockWithCaching(String hfile, BlockType blockType) {
    return cache.getBlock(new BlockCacheKey(hfile, 0), true, false, false);
  }

  private HFileBlock createBlock(BlockType blockType) {
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    ByteBuffer buf1 = ByteBuffer.allocate(size);
    HFileContext meta = new HFileContextBuilder().build();
    ByteBuffAllocator allocator = ByteBuffAllocator.HEAP;
    return new HFileBlock(blockType, size, size, -1, ByteBuff.wrap(buf1), HFileBlock.FILL_HEADER,
      -1, 52, -1, meta, allocator);

  }
}
