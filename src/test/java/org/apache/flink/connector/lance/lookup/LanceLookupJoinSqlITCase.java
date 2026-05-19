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

import org.apache.flink.connector.lance.LanceDatasetOpener;
import org.apache.flink.connector.lance.table.LanceNamespaceCatalog;

import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.BTreeIndexParams;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end SQL tests for the Lance lookup join: catalog-table integration, dynamic OPTIONS hints,
 * scalar-index requirement, partial-cache options, and rejection contract.
 *
 * <p>Each test seeds a small Lance dataset via a batch {@link TableEnvironment}, optionally creates
 * a BTREE scalar index on the key column, then runs the lookup join through a streaming {@link
 * StreamTableEnvironment}. Bounded VALUES tables on the probe side terminate the streaming job.
 *
 * <p>These tests pin user-visible correctness. In Flink 1.19, the planner does not push filters,
 * projections, or limits into a lookup-side source — predicates and projections run in the Calc
 * above the lookup. Individual tests below avoid restating that and just assert the externally
 * observable result.
 */
@Execution(ExecutionMode.SAME_THREAD)
class LanceLookupJoinSqlITCase {

  @TempDir Path tempDir;

  @Test
  void basicLookupJoinEnrichment() throws Exception {
    String customersPath = newDatasetPath("customers-basic");
    seedCustomersWithIndex(customersPath, List.of(row(1L, "Alice"), row(2L, "Bob")));

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(createLanceTable("customers", customersPath));
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1), (11, 2), (12, 1)) AS t(event_id, customer_id)");

    List<Row> rows = collect(env, joinQuery(""));
    Map<Integer, String> byEvent = rowsByEvent(rows);
    assertThat(byEvent)
        .containsOnly(Map.entry(10, "Alice"), Map.entry(11, "Bob"), Map.entry(12, "Alice"));
  }

  @Test
  void missingScalarIndexFailsByDefault() throws Exception {
    String customersPath = newDatasetPath("customers-no-index");
    seedCustomers(customersPath, List.of(row(1L, "Alice"), row(2L, "Bob")));
    // Note: no index created — the job should fail when the lookup function opens.

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(createLanceTable("customers", customersPath));
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1)) AS t(event_id, customer_id)");

    assertThatThrownBy(() -> collect(env, joinQuery("")))
        .rootCause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("scalar index")
        .hasMessageContaining("[id]")
        .hasMessageContaining("lookup.allow-full-scan");
  }

  @Test
  void allowFullScanHintEnablesLookupWithoutIndex() throws Exception {
    String customersPath = newDatasetPath("customers-allow-full");
    seedCustomers(customersPath, List.of(row(1L, "Alice"), row(2L, "Bob")));

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(createLanceTable("customers", customersPath));
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1), (11, 2)) AS t(event_id, customer_id)");

    String hint = "/*+ OPTIONS('lookup.allow-full-scan' = 'true') */";
    List<Row> rows = collect(env, joinQuery(hint));
    assertThat(rowsByEvent(rows)).containsOnly(Map.entry(10, "Alice"), Map.entry(11, "Bob"));
  }

  @Test
  void partialCacheOptionsAreAcceptedThroughHint() throws Exception {
    String customersPath = newDatasetPath("customers-cache");
    seedCustomersWithIndex(customersPath, List.of(row(1L, "Alice"), row(2L, "Bob")));

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(createLanceTable("customers", customersPath));
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1), (11, 1), (12, 2)) AS t(event_id, customer_id)");

    String hint =
        "/*+ OPTIONS("
            + "'lookup.cache' = 'PARTIAL',"
            + "'lookup.partial-cache.max-rows' = '1024',"
            + "'lookup.partial-cache.expire-after-write' = '10 min',"
            + "'lookup.partial-cache.expire-after-access' = '1 min',"
            + "'lookup.partial-cache.cache-missing-key' = 'false'"
            + ") */";
    List<Row> rows = collect(env, joinQuery(hint));
    assertThat(rowsByEvent(rows))
        .containsOnly(Map.entry(10, "Alice"), Map.entry(11, "Alice"), Map.entry(12, "Bob"));
  }

  @Test
  void multipleMatchesAreAllReturned() throws Exception {
    String customersPath = newDatasetPath("customers-multimatch");
    seedCustomersWithIndex(
        customersPath, List.of(row(1L, "Alice-1"), row(1L, "Alice-2"), row(2L, "Bob")));

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(createLanceTable("customers", customersPath));
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1)) AS t(event_id, customer_id)");

    List<Row> rows = collect(env, joinQuery(""));
    List<String> names =
        rows.stream().map(r -> (String) r.getField(1)).sorted().collect(Collectors.toList());
    assertThat(names).containsExactly("Alice-1", "Alice-2");
  }

  @Test
  void nullKeyDropsTheRow() throws Exception {
    String customersPath = newDatasetPath("customers-nullkey");
    seedCustomersWithIndex(customersPath, List.of(row(1L, "Alice")));

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(createLanceTable("customers", customersPath));
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, CAST(NULL AS BIGINT)), (11, 1)) AS t(event_id, customer_id)");

    List<Row> rows = collect(env, joinQuery(""));
    assertThat(rowsByEvent(rows)).containsOnly(Map.entry(11, "Alice"));
  }

  @Test
  void stringKeyEscapingSurvivesEndToEnd() throws Exception {
    String customersPath = newDatasetPath("customers-string-key");
    TableEnvironment batch = batchEnv();
    batch.executeSql(
        "CREATE TABLE c (id STRING, name STRING) WITH ('connector'='lance', 'path'='"
            + customersPath
            + "')");
    batch.executeSql("INSERT INTO c VALUES ('O''Brien', 'Patrick'), ('Smith', 'Sue')").await();
    createBTreeIndex(customersPath, "id");

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(
        "CREATE TABLE customers (id STRING, name STRING) WITH ('connector'='lance', 'path'='"
            + customersPath
            + "')");
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 'O''Brien'), (11, 'Smith')) AS t(event_id, customer_id)");

    List<Row> rows =
        collect(
            env,
            "SELECT e.event_id, c.name FROM events AS e"
                + " JOIN customers FOR SYSTEM_TIME AS OF e.proc_time AS c"
                + " ON e.customer_id = c.id");
    assertThat(rowsByEvent(rows)).containsOnly(Map.entry(10, "Patrick"), Map.entry(11, "Sue"));
  }

  @Test
  void rightSideFilterCombinedWithLookupKey() throws Exception {
    // WHERE c.region = 'EU' narrows the dimension side — result must include only EU customers.
    String customersPath = newDatasetPath("customers-filter-combined");
    TableEnvironment batch = batchEnv();
    batch.executeSql(
        "CREATE TABLE c (id BIGINT, name STRING, region STRING) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "')");
    batch
        .executeSql("INSERT INTO c VALUES (1, 'Alice', 'EU'), (2, 'Bob', 'US'), (3, 'Cleo', 'EU')")
        .await();
    createBTreeIndex(customersPath, "id");

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(
        "CREATE TABLE customers (id BIGINT, name STRING, region STRING) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "')");
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1), (11, 2), (12, 3)) AS t(event_id, customer_id)");

    List<Row> rows =
        collect(
            env,
            "SELECT e.event_id, c.name FROM events AS e"
                + " JOIN customers FOR SYSTEM_TIME AS OF e.proc_time AS c"
                + " ON e.customer_id = c.id"
                + " WHERE c.region = 'EU'");
    assertThat(rowsByEvent(rows)).containsOnly(Map.entry(10, "Alice"), Map.entry(12, "Cleo"));
  }

  @Test
  void rightSideBooleanFilterCombinedWithLookupKey() throws Exception {
    // WHERE c.active = TRUE narrows the dimension side — result must include only active rows.
    String customersPath = newDatasetPath("customers-active");
    TableEnvironment batch = batchEnv();
    batch.executeSql(
        "CREATE TABLE c (id BIGINT, name STRING, active BOOLEAN) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "')");
    batch
        .executeSql("INSERT INTO c VALUES (1, 'Alice', TRUE), (2, 'Bob', FALSE), (3, 'Cleo', TRUE)")
        .await();
    createBTreeIndex(customersPath, "id");

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(
        "CREATE TABLE customers (id BIGINT, name STRING, active BOOLEAN) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "')");
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1), (11, 2), (12, 3)) AS t(event_id, customer_id)");

    List<Row> rows =
        collect(
            env,
            "SELECT e.event_id, c.name FROM events AS e"
                + " JOIN customers FOR SYSTEM_TIME AS OF e.proc_time AS c"
                + " ON e.customer_id = c.id"
                + " WHERE c.active = TRUE");
    assertThat(rowsByEvent(rows)).containsOnly(Map.entry(10, "Alice"), Map.entry(12, "Cleo"));
  }

  @Test
  void scanVersionOnLookupTableIsRejected() throws Exception {
    String customersPath = newDatasetPath("customers-scan-version");
    seedCustomersWithIndex(customersPath, List.of(row(1L, "Alice")));

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(
        "CREATE TABLE customers (id BIGINT, name STRING) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "', 'scan.version'='1')");
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1)) AS t(event_id, customer_id)");

    assertThatThrownBy(() -> collect(env, joinQuery("")))
        .rootCause()
        .hasMessageContaining("does not honor time-travel scan options");
  }

  @Test
  void continuousSourceIsRejectedAsLookup() throws Exception {
    String customersPath = newDatasetPath("customers-continuous");
    seedCustomersWithIndex(customersPath, List.of(row(1L, "Alice")));

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(
        "CREATE TABLE customers (id BIGINT, name STRING) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "', 'scan.mode'='continuous')");
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1)) AS t(event_id, customer_id)");

    assertThatThrownBy(() -> collect(env, joinQuery("")))
        .rootCause()
        .hasMessageContaining("Lookup join is not supported on Lance tables")
        .hasMessageContaining("scan.mode = continuous");
  }

  @Test
  void catalogManagedTableLookupJoinWithDynamicOptionsHint() throws Exception {
    // Mirrors the catalog flow in the spec: the user creates 'customers' through the Lance
    // catalog (no 'path' / 'connector' options in the DDL) and joins against it through a
    // dynamic OPTIONS hint at query time.
    String warehouse = tempDir.resolve("warehouse").toUri().toString();

    TableEnvironment batch = batchEnv();
    batch.executeSql(
        "CREATE CATALOG my_catalog WITH ('type' = 'lance', 'warehouse' = '" + warehouse + "')");
    batch.executeSql("USE CATALOG my_catalog");
    batch.executeSql("CREATE TABLE customers (id BIGINT, name STRING)");
    batch.executeSql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").await();

    // Resolve the catalog-managed dataset path via the existing catalog table so we can attach a
    // BTREE index before the streaming join probes.
    LanceNamespaceCatalog catalog =
        (LanceNamespaceCatalog) batch.getCatalog("my_catalog").orElseThrow();
    String catalogPath =
        catalog.getTable(new ObjectPath("default", "customers")).getOptions().get("path");
    createBTreeIndex(catalogPath, "id");

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(
        "CREATE CATALOG my_catalog WITH ('type' = 'lance', 'warehouse' = '" + warehouse + "')");
    env.executeSql("USE CATALOG my_catalog");
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1), (11, 2)) AS t(event_id, customer_id)");

    String hint =
        "/*+ OPTIONS('lookup.cache' = 'PARTIAL', 'lookup.partial-cache.max-rows' = '128') */";
    List<Row> rows =
        collect(
            env,
            "SELECT e.event_id, c.name FROM events AS e"
                + " JOIN customers "
                + hint
                + " FOR SYSTEM_TIME AS OF e.proc_time AS c"
                + " ON e.customer_id = c.id");
    assertThat(rowsByEvent(rows)).containsOnly(Map.entry(10, "Alice"), Map.entry(11, "Bob"));
  }

  @Test
  void metadataTableRejectsLookup() throws Exception {
    String customersPath = newDatasetPath("customers-metadata");
    seedCustomersWithIndex(customersPath, List.of(row(1L, "Alice")));

    // Register the base 'customers' table through the catalog, then JOIN against its $snapshots
    // metadata view. The metadata source must reject the lookup at runtime.
    String warehouse = tempDir.resolve("warehouse-metadata").toUri().toString();
    TableEnvironment batch = batchEnv();
    batch.executeSql(
        "CREATE CATALOG my_catalog WITH ('type' = 'lance', 'warehouse' = '" + warehouse + "')");
    batch.executeSql("USE CATALOG my_catalog");
    batch.executeSql("CREATE TABLE customers (id BIGINT, name STRING)");
    batch.executeSql("INSERT INTO customers VALUES (1, 'Alice')").await();

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(
        "CREATE CATALOG my_catalog WITH ('type' = 'lance', 'warehouse' = '" + warehouse + "')");
    env.executeSql("USE CATALOG my_catalog");
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (CAST(1 AS BIGINT))) AS t(version_id)");

    assertThatThrownBy(
            () ->
                collect(
                    env,
                    "SELECT e.version_id, c.version_id FROM events AS e"
                        + " JOIN `customers$snapshots` FOR SYSTEM_TIME AS OF e.proc_time AS c"
                        + " ON e.version_id = c.version_id"))
        .rootCause()
        .hasMessageContaining("metadata-type = snapshots")
        .hasMessageContaining("not supported");
  }

  @Test
  void projectionPushdownWithNonKeyColumnsSelected() throws Exception {
    // SELECT only c.email — result must match each event to its customer email.
    String customersPath = newDatasetPath("customers-projection");
    TableEnvironment batch = batchEnv();
    batch.executeSql(
        "CREATE TABLE c (id BIGINT, name STRING, email STRING) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "')");
    batch
        .executeSql(
            "INSERT INTO c VALUES (1, 'Alice', 'alice@example.com'),"
                + " (2, 'Bob', 'bob@example.com')")
        .await();
    createBTreeIndex(customersPath, "id");

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(
        "CREATE TABLE customers (id BIGINT, name STRING, email STRING) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "')");
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1), (11, 2)) AS t(event_id, customer_id)");

    List<Row> rows =
        collect(
            env,
            "SELECT e.event_id, c.email FROM events AS e"
                + " JOIN customers FOR SYSTEM_TIME AS OF e.proc_time AS c"
                + " ON e.customer_id = c.id");
    assertThat(rowsByEvent(rows))
        .containsOnly(Map.entry(10, "alice@example.com"), Map.entry(11, "bob@example.com"));
  }

  @Test
  void projectionPushdownWhenLookupKeyIsNotInSelectList() throws Exception {
    // Select list excludes c.id; the lookup runtime still needs it as a key.
    String customersPath = newDatasetPath("customers-projection-no-key");
    TableEnvironment batch = batchEnv();
    batch.executeSql(
        "CREATE TABLE c (id BIGINT, name STRING, region STRING) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "')");
    batch.executeSql("INSERT INTO c VALUES (1, 'Alice', 'EU'), (2, 'Bob', 'US')").await();
    createBTreeIndex(customersPath, "id");

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(
        "CREATE TABLE customers (id BIGINT, name STRING, region STRING) WITH ("
            + "'connector'='lance', 'path'='"
            + customersPath
            + "')");
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1), (11, 2)) AS t(event_id, customer_id)");

    List<Row> rows =
        collect(
            env,
            "SELECT e.event_id, c.region, c.name FROM events AS e"
                + " JOIN customers FOR SYSTEM_TIME AS OF e.proc_time AS c"
                + " ON e.customer_id = c.id");
    assertThat(rows)
        .extracting(r -> r.getField(0), r -> r.getField(1), r -> r.getField(2))
        .containsExactlyInAnyOrder(
            org.assertj.core.groups.Tuple.tuple(10, "EU", "Alice"),
            org.assertj.core.groups.Tuple.tuple(11, "US", "Bob"));
  }

  @Test
  void allowFullScanWithIndexStillReturnsCorrectResults() throws Exception {
    // The hint allows fallback to scan-plus-filter when no index is present; it must not force a
    // scan when an index exists. The externally observable result must be identical to the
    // indexed-only path.
    String customersPath = newDatasetPath("customers-allow-with-index");
    seedCustomersWithIndex(customersPath, List.of(row(1L, "Alice"), row(2L, "Bob")));

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(createLanceTable("customers", customersPath));
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1), (11, 2)) AS t(event_id, customer_id)");

    String hint = "/*+ OPTIONS('lookup.allow-full-scan' = 'true') */";
    List<Row> rows = collect(env, joinQuery(hint));
    assertThat(rowsByEvent(rows)).containsOnly(Map.entry(10, "Alice"), Map.entry(11, "Bob"));
  }

  @Test
  void fullCacheTypeIsRejected() throws Exception {
    String customersPath = newDatasetPath("customers-full-cache");
    seedCustomersWithIndex(customersPath, List.of(row(1L, "Alice")));

    StreamTableEnvironment env = streamingEnv();
    env.executeSql(createLanceTable("customers", customersPath));
    env.executeSql(
        "CREATE TEMPORARY VIEW events AS SELECT *, PROCTIME() AS proc_time"
            + " FROM (VALUES (10, 1)) AS t(event_id, customer_id)");

    String hint = "/*+ OPTIONS('lookup.cache' = 'FULL') */";
    assertThatThrownBy(() -> collect(env, joinQuery(hint)))
        .rootCause()
        .hasMessageContaining("lookup.cache = FULL is not supported");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private String newDatasetPath(String name) {
    return tempDir.resolve(name).toUri().toString();
  }

  private static String createLanceTable(String name, String path) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT, name STRING) WITH ('connector'='lance', 'path'='"
        + path
        + "')";
  }

  private static String joinQuery(String hint) {
    return "SELECT e.event_id, c.name FROM events AS e"
        + " JOIN customers "
        + hint
        + " FOR SYSTEM_TIME AS OF e.proc_time AS c"
        + " ON e.customer_id = c.id";
  }

  private static Row row(long id, String name) {
    return Row.of(id, name);
  }

  private static void seedCustomers(String path, List<Row> rows) throws Exception {
    TableEnvironment batch = batchEnv();
    batch.executeSql(
        "CREATE TABLE c (id BIGINT, name STRING) WITH ('connector'='lance', 'path'='"
            + path
            + "')");
    StringBuilder values = new StringBuilder("INSERT INTO c VALUES ");
    for (int i = 0; i < rows.size(); i++) {
      Row r = rows.get(i);
      if (i > 0) values.append(", ");
      values.append("(").append(r.getField(0)).append(", '").append(r.getField(1)).append("')");
    }
    batch.executeSql(values.toString()).await();
  }

  private static void seedCustomersWithIndex(String path, List<Row> rows) throws Exception {
    seedCustomers(path, rows);
    createBTreeIndex(path, "id");
  }

  private static void createBTreeIndex(String path, String column) {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset dataset = LanceDatasetOpener.open(allocator, path)) {
      IndexParams params =
          IndexParams.builder().setScalarIndexParams(BTreeIndexParams.builder().build()).build();
      IndexOptions opts = IndexOptions.builder(List.of(column), IndexType.BTREE, params).build();
      dataset.createIndex(opts);

      // Sanity check that the index is queryable for exact equality — protects against silent
      // regressions in the Lance SDK before the join even runs.
      assertThat(
              dataset.describeIndices(
                  new IndexCriteria.Builder()
                      .forColumn(column)
                      .mustSupportExactEquality(true)
                      .build()))
          .as("Lance must report the freshly-created index as exact-equality capable")
          .isNotEmpty();
    }
  }

  private static TableEnvironment batchEnv() {
    return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
  }

  private static StreamTableEnvironment streamingEnv() {
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    streamEnv.setParallelism(1);
    StreamTableEnvironment env =
        StreamTableEnvironment.create(
            streamEnv, EnvironmentSettings.newInstance().inStreamingMode().build());
    env.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
    return env;
  }

  private static List<Row> collect(StreamTableEnvironment env, String sql) throws Exception {
    TableResult result = env.executeSql(sql);
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> it = result.collect()) {
      while (it.hasNext()) {
        rows.add(it.next());
      }
    }
    return rows;
  }

  private static Map<Integer, String> rowsByEvent(List<Row> rows) {
    return rows.stream()
        .collect(Collectors.toMap(r -> (Integer) r.getField(0), r -> (String) r.getField(1)));
  }
}
