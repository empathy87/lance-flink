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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Lookup-join options supported by the Lance connector. */
public final class LanceLookupOptions {

  public static final ConfigOption<Boolean> ALLOW_FULL_SCAN =
      ConfigOptions.key("lookup.allow-full-scan")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Allows lookup join to run without scalar indexes on lookup key columns. "
                  + "This is unsafe for large tables because every lookup may scan table fragments.");

  /** Lance-specific lookup options. */
  public static final Set<ConfigOption<?>> LANCE_OPTIONS = Set.of(ALLOW_FULL_SCAN);

  // TODO: Add lookup.max-retries once lookup calls are wrapped with retry handling.
  /** Supported standard Flink lookup options. */
  public static final Set<ConfigOption<?>> STANDARD_OPTIONS =
      Set.of(
          LookupOptions.CACHE_TYPE,
          LookupOptions.PARTIAL_CACHE_MAX_ROWS,
          LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
          LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
          LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);

  /** All lookup options supported by the Lance connector. */
  public static final Set<ConfigOption<?>> ALL_OPTIONS =
      Stream.concat(LANCE_OPTIONS.stream(), STANDARD_OPTIONS.stream())
          .collect(Collectors.toUnmodifiableSet());

  private LanceLookupOptions() {}
}
