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
import org.apache.flink.connector.lance.source.scan.LanceScanOptions;
import org.apache.flink.connector.lance.table.LanceDynamicTableSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavioural tests for {@link LanceLookupFunction} against a real on-disk Lance dataset.
 *
 * <p>Datasets are seeded via Flink SQL {@code INSERT INTO} so the test stays format-agnostic and
 * also exercises the existing append-sink path; the lookup function then runs against the resulting
 * dataset directly. Tests that rely on scalar indexes live in the SQL integration test — these
 * focus on the fail-fast/full-scan/null-key/multi-match contract.
 */
class LanceLookupFunctionTest {

  private static final RowType ROW_TYPE =
      new RowType(
          List.of(
              new RowType.RowField("id", new BigIntType()),
              new RowType.RowField("name", new VarCharType(64))));

  @TempDir Path tempDir;

  private String datasetPath;

  @BeforeEach
  void setUp() throws Exception {
    datasetPath = tempDir.resolve("dataset").toUri().toString();
    TableEnvironment env =
        TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    env.executeSql(
        "CREATE TABLE t (id BIGINT, name STRING) WITH ('connector'='lance', 'path'='"
            + datasetPath
            + "')");
    // Non-unique key (id=2) so the multi-match contract can be verified directly.
    env.executeSql("INSERT INTO t VALUES (1, 'one'), (2, 'two-a'), (2, 'two-b'), (3, 'three')")
        .await();
  }

  @Test
  void missingScalarIndexFailsFastByDefault() throws Exception {
    LanceLookupFunction function = newFunction(false);
    try {
      assertThatThrownBy(() -> function.open(new FunctionContext(null)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("scalar index")
          .hasMessageContaining("[id]")
          .hasMessageContaining("lookup.allow-full-scan");
    } finally {
      function.close();
    }
  }

  @Test
  void fullScanFallbackReturnsMatchesWithoutIndex() throws Exception {
    LanceLookupFunction function = newFunction(true);
    function.open(new FunctionContext(null));
    try {
      Collection<RowData> results = function.lookup(keyRow(1L));
      List<String> names =
          results.stream().map(r -> r.getString(1).toString()).collect(Collectors.toList());
      assertThat(names).containsExactly("one");
    } finally {
      function.close();
    }
  }

  @Test
  void multipleMatchesAreAllReturned() throws Exception {
    LanceLookupFunction function = newFunction(true);
    function.open(new FunctionContext(null));
    try {
      Collection<RowData> results = function.lookup(keyRow(2L));
      List<String> names =
          results.stream()
              .map(r -> r.getString(1).toString())
              .sorted()
              .collect(Collectors.toList());
      assertThat(names).containsExactly("two-a", "two-b");
    } finally {
      function.close();
    }
  }

  @Test
  void unknownKeyReturnsEmpty() throws Exception {
    LanceLookupFunction function = newFunction(true);
    function.open(new FunctionContext(null));
    try {
      assertThat(function.lookup(keyRow(999L))).isEmpty();
    } finally {
      function.close();
    }
  }

  @Test
  void nullKeyShortCircuitsToEmpty() throws Exception {
    LanceLookupFunction function = newFunction(true);
    function.open(new FunctionContext(null));
    try {
      GenericRowData nullKey = new GenericRowData(1);
      nullKey.setField(0, null);
      assertThat(function.lookup(nullKey)).isEmpty();
    } finally {
      function.close();
    }
  }

  @Test
  void pushedFilterIsCombinedWithKeyFilter() throws Exception {
    // The pushed-filter clause restricts id=2 to only the row whose name = 'two-a'. Without
    // combining the filter, both id=2 rows would come back.
    LanceLookupFunction function = newFunction(true, "name = 'two-a'");
    function.open(new FunctionContext(null));
    try {
      List<String> names =
          function.lookup(keyRow(2L)).stream()
              .map(r -> r.getString(1).toString())
              .collect(Collectors.toList());
      assertThat(names).containsExactly("two-a");

      // A pushed filter that excludes every matching row of the key must return empty.
      function.close();
      LanceLookupFunction filtered = newFunction(true, "name = 'nonexistent'");
      filtered.open(new FunctionContext(null));
      try {
        assertThat(filtered.lookup(keyRow(1L))).isEmpty();
      } finally {
        filtered.close();
      }
    } finally {
      function.close();
    }
  }

  @Test
  void combineFiltersWrapsBothSidesInParentheses() {
    // Direct pin on the format. Without the outer parens, "(a AND b) OR c" would silently drift
    // to "a AND b OR c" — parsed as "a AND (b OR c)" under SQL precedence. The lookup runtime
    // depends on this exact shape when ANDing a composite-key filter with a pushed disjunction.
    String combined =
        LanceLookupFunction.combineFilters("k1 = 1 AND k2 = 2", "region = 'EU' OR region = 'US'");
    assertThat(combined).isEqualTo("(k1 = 1 AND k2 = 2) AND (region = 'EU' OR region = 'US')");
  }

  @Test
  void combineFiltersReturnsKeyFilterWhenNoPushedFilter() {
    assertThat(LanceLookupFunction.combineFilters("id = 7", null)).isEqualTo("id = 7");
  }

  @Test
  void combineFiltersPreservesEmbeddedSingleQuoteEscaping() {
    // Both halves carry single-quoted literals with doubled-quote escapes; the combiner must not
    // mangle them. The Lance filter parser treats '' inside '...' as a literal single quote, so
    // O'Brien round-trips correctly through both clauses.
    String keyFilter = "name = 'O''Brien'";
    String pushedFilter = "tag = 'a''b'";
    assertThat(LanceLookupFunction.combineFilters(keyFilter, pushedFilter))
        .isEqualTo("(name = 'O''Brien') AND (tag = 'a''b')");
  }

  @Test
  void pushedFilterWithEmbeddedQuoteCombinedWithQuotedKeyReturnsCorrectResults() throws Exception {
    // End-to-end pin: a string lookup key carrying a single quote AND a pushed filter carrying
    // its own quoted literal must both survive escaping. Without correct escaping either side
    // would corrupt the combined filter and either over-match or fail to parse on the Lance side.
    String localDataset = tempDir.resolve("dataset-quoted").toUri().toString();
    TableEnvironment env =
        TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    env.executeSql(
        "CREATE TABLE t (id BIGINT, name STRING) WITH ('connector'='lance', 'path'='"
            + localDataset
            + "')");
    env.executeSql("INSERT INTO t VALUES (1, 'O''Brien'), (2, 'O''Brien'), (3, 'Smith')").await();

    LanceLookupFunction function =
        new LanceLookupFunction(
            localDataset,
            List.of("name"),
            List.of(new VarCharType(64)),
            List.of("id", "name"),
            ROW_TYPE,
            1024,
            true,
            "name = 'O''Brien' AND id = 2");
    function.open(new FunctionContext(null));
    try {
      GenericRowData key = new GenericRowData(1);
      key.setField(0, StringData.fromString("O'Brien"));
      Collection<RowData> rows = function.lookup(key);
      assertThat(rows).hasSize(1);
      assertThat(rows.iterator().next().getLong(0)).isEqualTo(2L);
    } finally {
      function.close();
    }
  }

  @Test
  void projectedColumnOrderingMatchesProducedRowType() throws Exception {
    // The function is constructed with a produced row type that reorders columns to [name, id];
    // the Lance scanner is told to project [name, id] in that order and the converter reads back
    // against the same row type. Returned RowData must therefore carry name at position 0 and id
    // at position 1, regardless of the physical [id, name] schema order on disk.
    RowType reorderedRowType =
        new RowType(
            List.of(
                new RowType.RowField("name", new VarCharType(64)),
                new RowType.RowField("id", new BigIntType())));
    LanceLookupFunction function =
        new LanceLookupFunction(
            datasetPath,
            List.of("id"),
            List.of(new BigIntType()),
            List.of("name", "id"),
            reorderedRowType,
            1024,
            true,
            null);
    function.open(new FunctionContext(null));
    try {
      Collection<RowData> rows = function.lookup(keyRow(2L));
      assertThat(rows).hasSize(2);
      for (RowData row : rows) {
        // Position 0 in the produced row is name (STRING); position 1 is id (BIGINT). If the
        // converter were reading against the physical [id, name] schema, getLong(0) would return
        // the id value and getString(1) the name — but the asserts below would fail because the
        // string position would carry a numeric.
        assertThat(row.getString(0).toString()).startsWith("two-");
        assertThat(row.getLong(1)).isEqualTo(2L);
      }
    } finally {
      function.close();
    }
  }

  @Test
  void applyProjectionThreadsProducedRowTypeIntoLookupFunction() {
    // Cross-package wiring pin: LanceDynamicTableSource.applyProjection must thread the produced
    // row type into the LanceLookupFunction so it can project the right columns and resolve key
    // names against the (potentially reordered) produced schema.
    LanceDynamicTableSource source =
        LanceDynamicTableSource.forBatch(
            LanceOptions.builder().path("/tmp/lance").build(),
            LanceScanOptions.latest(),
            LanceLookupConfig.fromConfig(new Configuration()),
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("email", DataTypes.STRING())));
    DataType produced =
        DataTypes.ROW(
            DataTypes.FIELD("email", DataTypes.STRING()),
            DataTypes.FIELD("id", DataTypes.BIGINT()));
    source.applyProjection(new int[][] {{2}, {0}}, produced);

    LookupFunctionProvider provider =
        (LookupFunctionProvider) source.getLookupRuntimeProvider(keysContext(new int[][] {{1}}));
    LanceLookupFunction function = (LanceLookupFunction) provider.createLookupFunction();

    assertThat(function.keyColumns()).containsExactly("id");
    assertThat(function.projectedColumns()).containsExactly("email", "id");
    assertThat(function.producedRowType().getFieldNames()).containsExactly("email", "id");
  }

  private static LookupTableSource.LookupContext keysContext(int[][] keys) {
    return new LookupTableSource.LookupContext() {
      @Override
      public int[][] getKeys() {
        return keys;
      }

      @Override
      public <T> org.apache.flink.api.common.typeinfo.TypeInformation<T> createTypeInformation(
          DataType producedDataType) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> org.apache.flink.api.common.typeinfo.TypeInformation<T> createTypeInformation(
          org.apache.flink.table.types.logical.LogicalType producedLogicalType) {
        throw new UnsupportedOperationException();
      }

      @Override
      public org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter
          createDataStructureConverter(DataType producedDataType) {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Test
  void accessorsReflectConstructionArguments() {
    // The accessors exist for tests to pin the function's wiring without reflection. Pin that
    // every constructor arg is reachable so a future refactor cannot quietly drop one.
    String pushedFilter = "region = 'EU'";
    LanceLookupFunction function =
        new LanceLookupFunction(
            datasetPath,
            List.of("id"),
            List.of(new BigIntType()),
            List.of("id", "name"),
            ROW_TYPE,
            512,
            true,
            pushedFilter);
    assertThat(function.keyColumns()).containsExactly("id");
    assertThat(function.projectedColumns()).containsExactly("id", "name");
    assertThat(function.producedRowType()).isSameAs(ROW_TYPE);
    assertThat(function.pushedFilter()).isEqualTo(pushedFilter);
    assertThat(function.allowFullScan()).isTrue();
    // Blank pushed filter must collapse to null so combineFilters short-circuits the empty case.
    LanceLookupFunction blankFiltered =
        new LanceLookupFunction(
            datasetPath,
            List.of("id"),
            List.of(new BigIntType()),
            List.of("id", "name"),
            ROW_TYPE,
            512,
            false,
            "   ");
    assertThat(blankFiltered.pushedFilter()).isNull();
  }

  @Test
  void stringKeyEscapingDoesNotMatchInjectionPayload() throws Exception {
    LanceLookupFunction function =
        new LanceLookupFunction(
            datasetPath,
            List.of("name"),
            List.of(new VarCharType(64)),
            List.of("id", "name"),
            ROW_TYPE,
            1024,
            true,
            null);
    function.open(new FunctionContext(null));
    try {
      // The injection-style payload must be treated as a single literal, so the lookup misses
      // (no row has this exact string), confirming the embedded quote was escaped, not honored
      // as a closing quote.
      GenericRowData payload = new GenericRowData(1);
      payload.setField(0, StringData.fromString("two-a' OR 1=1 --"));
      assertThat(function.lookup(payload)).isEmpty();

      // Sanity check: a legitimate string value still matches.
      GenericRowData good = new GenericRowData(1);
      good.setField(0, StringData.fromString("one"));
      assertThat(function.lookup(good)).hasSize(1);
    } finally {
      function.close();
    }
  }

  private LanceLookupFunction newFunction(boolean allowFullScan) {
    return newFunction(allowFullScan, null);
  }

  private LanceLookupFunction newFunction(boolean allowFullScan, String pushedFilter) {
    return new LanceLookupFunction(
        datasetPath,
        List.of("id"),
        List.of(new BigIntType()),
        List.of("id", "name"),
        ROW_TYPE,
        1024,
        allowFullScan,
        pushedFilter);
  }

  private static GenericRowData keyRow(long id) {
    GenericRowData row = new GenericRowData(1);
    row.setField(0, id);
    return row;
  }
}
