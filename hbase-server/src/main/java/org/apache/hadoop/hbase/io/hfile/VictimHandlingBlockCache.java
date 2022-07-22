package org.apache.hadoop.hbase.io.hfile;

import org.apache.yetus.audience.InterfaceAudience;
import java.util.function.BiConsumer;

@InterfaceAudience.Private
public interface VictimHandlingBlockCache extends BlockCache {

  Cacheable getBlockWithPromotion(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics, BiConsumer<Cacheable, BlockPriority> promotionCallback);
}
