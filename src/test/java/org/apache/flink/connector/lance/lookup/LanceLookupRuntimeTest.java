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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LanceLookupRuntimeTest {

  @Test
  void cacheNoneYieldsPlainLookupFunctionProvider() {
    LookupRuntimeProvider provider =
        LanceLookupRuntime.build(
            options(),
            LanceLookupConfig.fromConfig(new Configuration()),
            producedRowType(),
            keys(),
            null);

    assertThat(provider).isInstanceOf(LookupFunctionProvider.class);
    assertThat(provider).isNotInstanceOf(PartialCachingLookupProvider.class);
  }

  @Test
  void cachePartialYieldsPartialCachingLookupProvider() {
    LookupRuntimeProvider provider =
        LanceLookupRuntime.build(options(), partialCacheConfig(), producedRowType(), keys(), null);

    assertThat(provider).isInstanceOf(PartialCachingLookupProvider.class);
    assertThat(((PartialCachingLookupProvider) provider).getCache()).isNotNull();
  }

  @Test
  void eachBuildProducesFreshCacheInstance() {
    // Two providers built from the same parsed config must hold distinct cache instances —
    // cache state must never bleed across LookupRuntimeProvider constructions.
    LanceLookupConfig parsed = partialCacheConfig();

    PartialCachingLookupProvider first =
        (PartialCachingLookupProvider)
            LanceLookupRuntime.build(options(), parsed, producedRowType(), keys(), null);
    PartialCachingLookupProvider second =
        (PartialCachingLookupProvider)
            LanceLookupRuntime.build(options(), parsed, producedRowType(), keys(), null);

    assertThat(first.getCache()).isNotSameAs(second.getCache());
    assertThat(first.getCache()).isEqualTo(second.getCache());
  }

  private static LanceOptions options() {
    return LanceOptions.builder().path("/tmp/does-not-exist").build();
  }

  private static RowType producedRowType() {
    return RowType.of(
        new LogicalType[] {new BigIntType(), new VarCharType()}, new String[] {"id", "name"});
  }

  private static LanceLookupKeys keys() {
    return new LanceLookupKeys(List.of("id"), List.<LogicalType>of(new BigIntType()));
  }

  private static LanceLookupConfig partialCacheConfig() {
    Configuration cfg = new Configuration();
    cfg.set(LookupOptions.CACHE_TYPE, LookupCacheType.PARTIAL);
    cfg.set(LookupOptions.PARTIAL_CACHE_MAX_ROWS, 1024L);
    return LanceLookupConfig.fromConfig(cfg);
  }
}
