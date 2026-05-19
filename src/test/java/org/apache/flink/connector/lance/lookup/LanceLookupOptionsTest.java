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

import org.apache.flink.connector.lance.lookup.LanceLookupConfig.PartialCacheConfig;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceLookupOptionsTest {

  @Test
  void allowFullScanDefaultsToFalse() {
    assertThat(LanceLookupOptions.ALLOW_FULL_SCAN.key()).isEqualTo("lookup.allow-full-scan");
    assertThat(LanceLookupOptions.ALLOW_FULL_SCAN.defaultValue()).isFalse();
  }

  @Test
  void emptyConfigParsesAsDefaults() {
    LanceLookupConfig config = LanceLookupConfig.fromConfig(new Configuration());
    assertThat(config.allowFullScan()).isFalse();
    assertThat(config.cacheType()).isEqualTo(LookupCacheType.NONE);
    assertThat(config.partialCacheConfig()).isEmpty();
  }

  @Test
  void allowFullScanIsParsed() {
    Configuration cfg = new Configuration();
    cfg.set(LanceLookupOptions.ALLOW_FULL_SCAN, true);
    assertThat(LanceLookupConfig.fromConfig(cfg).allowFullScan()).isTrue();
  }

  @Test
  void cacheNoneCarriesNoPartialConfig() {
    Configuration cfg = new Configuration();
    cfg.set(LookupOptions.CACHE_TYPE, LookupCacheType.NONE);
    LanceLookupConfig config = LanceLookupConfig.fromConfig(cfg);
    assertThat(config.cacheType()).isEqualTo(LookupCacheType.NONE);
    assertThat(config.partialCacheConfig()).isEmpty();
  }

  @Test
  void cachePartialParsesEachField() {
    Configuration cfg = new Configuration();
    cfg.set(LookupOptions.CACHE_TYPE, LookupCacheType.PARTIAL);
    cfg.set(LookupOptions.PARTIAL_CACHE_MAX_ROWS, 100_000L);
    cfg.set(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE, Duration.ofMinutes(10));
    cfg.set(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS, Duration.ofMinutes(1));
    cfg.set(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY, false);

    LanceLookupConfig config = LanceLookupConfig.fromConfig(cfg);
    assertThat(config.cacheType()).isEqualTo(LookupCacheType.PARTIAL);

    PartialCacheConfig partial = config.partialCacheConfig().orElseThrow();
    assertThat(partial.maxRows()).isEqualTo(100_000L);
    assertThat(partial.expireAfterWrite()).isEqualTo(Duration.ofMinutes(10));
    assertThat(partial.expireAfterAccess()).isEqualTo(Duration.ofMinutes(1));
    assertThat(partial.cacheMissingKey()).isFalse();
  }

  @Test
  void cachePartialUsesDefaultsForMissingFields() {
    Configuration cfg = new Configuration();
    cfg.set(LookupOptions.CACHE_TYPE, LookupCacheType.PARTIAL);
    cfg.set(LookupOptions.PARTIAL_CACHE_MAX_ROWS, 1024L);

    PartialCacheConfig partial =
        LanceLookupConfig.fromConfig(cfg).partialCacheConfig().orElseThrow();
    assertThat(partial.maxRows()).isEqualTo(1024L);
    assertThat(partial.expireAfterWrite()).isNull();
    assertThat(partial.expireAfterAccess()).isNull();
    // PARTIAL_CACHE_CACHE_MISSING_KEY has a Flink-side default; parsed value reflects it.
    assertThat(partial.cacheMissingKey())
        .isEqualTo(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY.defaultValue());
  }

  @Test
  void fullCacheTypeIsRejectedAtParseTime() {
    Configuration cfg = new Configuration();
    cfg.set(LookupOptions.CACHE_TYPE, LookupCacheType.FULL);

    assertThatThrownBy(() -> LanceLookupConfig.fromConfig(cfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("lookup.cache = FULL is not supported");
  }

  @Test
  void configNeverExposesACacheInstance() {
    Method[] methods = LanceLookupConfig.class.getDeclaredMethods();
    boolean leaksCache =
        Arrays.stream(methods).anyMatch(m -> LookupCache.class.isAssignableFrom(m.getReturnType()));
    assertThat(leaksCache).isFalse();
  }

  @Test
  void maxRetriesIsNotAccepted() {
    // lookup.max-retries is not implemented (no retrying delegator) — the factory must not
    // silently accept it.
    assertThat(LanceLookupOptions.ALL_OPTIONS).doesNotContain(LookupOptions.MAX_RETRIES);
  }
}
