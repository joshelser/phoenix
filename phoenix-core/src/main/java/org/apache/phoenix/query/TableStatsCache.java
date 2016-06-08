/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.stats.PTableStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * "Client-side" cache for storing {@link PTableStats} for Phoenix tables. Intended to decouple
 * Phoenix from a specific version of Guava's cache.
 */
public class TableStatsCache {
    private static final Logger logger = LoggerFactory.getLogger(TableStatsCache.class);

    private final Cache<ImmutableBytesPtr, PTableStats> cache;

    public TableStatsCache(Configuration config) {
      // Expire table stats cache entries after 
        final long halfStatsUpdateFreq = config.getLong(
                QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS) / 2;
        // Maximum number of entries (tables) to store in the cache at one time
        final long maxTableStatsCacheEntries = config.getLong(
                QueryServices.STATS_MAX_CACHE_ENTRIES,
                QueryServicesOptions.DEFAULT_STATS_MAX_CACHE_ENTRIES);
        cache = CacheBuilder.newBuilder()
                .maximumSize(maxTableStatsCacheEntries)
                .expireAfterAccess(halfStatsUpdateFreq, TimeUnit.MILLISECONDS)
                .removalListener(new PhoenixStatsCacheRemovalListener())
                .build();
    }

    /**
     * Returns the underlying cache. Try to use the provided methods instead of accessing the cache
     * directly.
     */
    Cache<ImmutableBytesPtr, PTableStats> getCache() {
        return cache;
    }

    /**
     * Returns the PTableStats for the given <code>tableName</code, using the provided
     * <code>valueLoader</code> if no such mapping exists.
     *
     * @see com.google.common.cache.Cache#get(Object, Callable)
     */
    public PTableStats get(ImmutableBytesPtr tableName, Callable<? extends PTableStats> valueLoader)
            throws ExecutionException {
        return getCache().get(tableName, valueLoader);
    }

    /**
     * Cache the given <code>stats</code> to the cache for the given <code>table</code>.
     *
     * @see com.google.common.cache.Cache#put(Object, Object)
     */
    public void put(PTable table, PTableStats stats) {
        put(Objects.requireNonNull(table).getName().getBytesPtr(), stats);
    }

    /**
     * Cache the given <code>stats</code> to the cache for the given <code>tableName</code>.
     *
     * @see com.google.common.cache.Cache#put(Object, Object)
     */
    public void put(ImmutableBytesPtr tableName, PTableStats stats) {
        getCache().put(Objects.requireNonNull(tableName), Objects.requireNonNull(stats));
    }

    /**
     * Removes the mapping for <code>tableName</code> if it exists.
     *
     * @see com.google.common.cache.Cache#invalidate(Object)
     */
    public void invalidate(ImmutableBytesPtr tableName) {
        getCache().invalidate(Objects.requireNonNull(tableName));
    }

    /**
     * Removes all mappings from the cache.
     *
     * @see com.google.common.cache.Cache#invalidateAll()
     */
    public void invalidateAll() {
        getCache().invalidateAll();
    }

    /**
     * A {@link RemovalListener} implementation to track evictions from the table stats cache.
     */
    static class PhoenixStatsCacheRemovalListener implements
            RemovalListener<ImmutableBytesPtr, PTableStats> {
        @Override
        public void onRemoval(RemovalNotification<ImmutableBytesPtr, PTableStats> notification) {
            final RemovalCause cause = notification.getCause();
            if (wasEvicted(cause)) {
                ImmutableBytesPtr ptr = notification.getKey();
                String tableName = new String(ptr.get(), ptr.getOffset(), ptr.getLength());
                logger.trace("Cached stats for {} with size={}bytes was evicted due to cause={}",
                    new Object[] {tableName, notification.getValue().getEstimatedSize(), cause});
            }
        }

        boolean wasEvicted(RemovalCause cause) {
            // This is actually a method on RemovalCause but isn't exposed
            return RemovalCause.EXPLICIT != cause && RemovalCause.REPLACED != cause;
        }
    }
}
