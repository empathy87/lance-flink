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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** Parsed lookup-join options. */
public final class LanceLookupConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final boolean allowFullScan;
  private final LookupCacheType cacheType;
  @Nullable private final PartialCacheConfig partialCacheConfig;

  private LanceLookupConfig(
      boolean allowFullScan,
      LookupCacheType cacheType,
      @Nullable PartialCacheConfig partialCacheConfig) {
    this.allowFullScan = allowFullScan;
    this.cacheType = Objects.requireNonNull(cacheType, "cacheType");
    this.partialCacheConfig = partialCacheConfig;
  }

  public static LanceLookupConfig fromConfig(ReadableConfig config) {
    boolean allowFullScan = config.get(LanceLookupOptions.ALLOW_FULL_SCAN);
    LookupCacheType cacheType = config.get(LookupOptions.CACHE_TYPE);

    return switch (cacheType) {
      case NONE -> new LanceLookupConfig(allowFullScan, cacheType, null);
      case PARTIAL ->
          new LanceLookupConfig(allowFullScan, cacheType, PartialCacheConfig.from(config));
      case FULL ->
          throw new IllegalArgumentException(
              "lookup.cache = FULL is not supported by the Lance connector. Use NONE or PARTIAL.");
    };
  }

  public boolean allowFullScan() {
    return allowFullScan;
  }

  public LookupCacheType cacheType() {
    return cacheType;
  }

  public Optional<PartialCacheConfig> partialCacheConfig() {
    return Optional.ofNullable(partialCacheConfig);
  }

  /** Parsed partial-cache options. */
  public record PartialCacheConfig(
      @Nullable Long maxRows,
      @Nullable Duration expireAfterWrite,
      @Nullable Duration expireAfterAccess,
      boolean cacheMissingKey)
      implements Serializable {

    private static final long serialVersionUID = 1L;

    static PartialCacheConfig from(ReadableConfig config) {
      return new PartialCacheConfig(
          config.getOptional(LookupOptions.PARTIAL_CACHE_MAX_ROWS).orElse(null),
          config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE).orElse(null),
          config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS).orElse(null),
          config.get(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY));
    }
  }
}
