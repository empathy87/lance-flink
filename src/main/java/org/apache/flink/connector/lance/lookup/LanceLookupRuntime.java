/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connector.lance.lookup;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.lookup.LanceLookupConfig.PartialCacheConfig;

import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

/** Builds the {@link LookupRuntimeProvider} for a Lance dimension table. */
public final class LanceLookupRuntime {

  private LanceLookupRuntime() {}

  public static LookupRuntimeProvider build(
      LanceOptions options,
      LanceLookupConfig lookupConfig,
      RowType producedRowType,
      LanceLookupKeys keys,
      @Nullable String pushedFilter) {
    LanceLookupFunction function =
        new LanceLookupFunction(
            options.getPath(),
            keys.columnNames(),
            keys.types(),
            producedRowType.getFieldNames(),
            producedRowType,
            options.getReadBatchSize(),
            lookupConfig.allowFullScan(),
            pushedFilter);

    if (lookupConfig.cacheType() == LookupCacheType.PARTIAL) {
      PartialCacheConfig partial =
          lookupConfig
              .partialCacheConfig()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "lookup.cache = PARTIAL but no PartialCacheConfig was parsed."));
      return PartialCachingLookupProvider.of(function, createPartialCache(partial));
    }
    return LookupFunctionProvider.of(function);
  }

  private static LookupCache createPartialCache(PartialCacheConfig config) {
    // TODO: Add lookup retry support before accepting lookup.max-retries.
    DefaultLookupCache.Builder builder =
        DefaultLookupCache.newBuilder().cacheMissingKey(config.cacheMissingKey());

    if (config.maxRows() != null) {
      builder.maximumSize(config.maxRows());
    }
    if (config.expireAfterWrite() != null) {
      builder.expireAfterWrite(config.expireAfterWrite());
    }
    if (config.expireAfterAccess() != null) {
      builder.expireAfterAccess(config.expireAfterAccess());
    }
    return builder.build();
  }
}
