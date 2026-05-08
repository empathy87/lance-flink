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
package org.apache.flink.connector.lance.source;

import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.Version;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL integration tests for the FLIP-27 Lance source. Each test seeds a real Lance dataset via
 * {@code INSERT INTO} and then queries it back, exercising the per-fragment scan path along with
 * projection pushdown, filter pushdown via {@code LanceFilterExpressionConverter}, and Flink-level
 * limit / aggregation on top of the source.
 */
class LanceSourceSqlITCase {

  @TempDir Path tempDir;

  @Test
  void testSelectAllReadsAllFragments() throws Exception {
    String datasetUri = tempDir.resolve("ds-select-all").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));

    // Two INSERTs → two commits → at least two Lance fragments, so per-fragment scanning is
    // actually exercised in parallel rather than collapsing into a single split.
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await();
    tableEnv.executeSql("INSERT INTO t VALUES (4, 'd'), (5, 'e')").await();

    Map<Long, String> seen = new HashMap<>();
    for (Row row : collectRows(tableEnv.executeSql("SELECT id, name FROM t"))) {
      seen.put((Long) row.getField(0), (String) row.getField(1));
    }
    assertThat(seen)
        .containsOnly(
            Map.entry(1L, "a"),
            Map.entry(2L, "b"),
            Map.entry(3L, "c"),
            Map.entry(4L, "d"),
            Map.entry(5L, "e"));
  }

  @Test
  void testSelectWithFilterPushDown() throws Exception {
    String datasetUri = tempDir.resolve("ds-filter").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')").await();

    List<Row> rows = collectRows(tableEnv.executeSql("SELECT id, name FROM t WHERE id > 2"));
    Map<Long, String> seen = new HashMap<>();
    for (Row row : rows) {
      seen.put((Long) row.getField(0), (String) row.getField(1));
    }
    assertThat(seen).containsOnly(Map.entry(3L, "c"), Map.entry(4L, "d"));
  }

  @Test
  void testSelectWithAndFilterAndQuotedLiteral() throws Exception {
    String datasetUri = tempDir.resolve("ds-and-filter").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv
        .executeSql("INSERT INTO t VALUES " + "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'b'), (5, 'd''e')")
        .await();

    // Exercises LanceFilterExpressionConverter end-to-end:
    //   AND of two comparisons + string literal that survives single-quote escaping.
    List<Row> rows =
        collectRows(tableEnv.executeSql("SELECT id FROM t WHERE id > 1 AND name = 'b'"));
    assertThat(rows).extracting(r -> r.getField(0)).containsExactlyInAnyOrder(2L, 4L);

    rows = collectRows(tableEnv.executeSql("SELECT id FROM t WHERE name = 'd''e'"));
    assertThat(rows).extracting(r -> r.getField(0)).containsExactlyInAnyOrder(5L);
  }

  @Test
  void testSelectWithLimit() throws Exception {
    String datasetUri = tempDir.resolve("ds-limit").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv
        .executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
        .await();

    List<Row> rows = collectRows(tableEnv.executeSql("SELECT id, name FROM t LIMIT 3"));
    assertThat(rows).hasSize(3);
  }

  @Test
  void testReadAtSnapshotId() throws Exception {
    String datasetUri = tempDir.resolve("ds-snapshot-id").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b')").await();
    long v1 = latestVersion(datasetUri);
    tableEnv.executeSql("INSERT INTO t VALUES (3, 'c')").await();
    long v2 = latestVersion(datasetUri);
    assertThat(v2).isGreaterThan(v1);

    // Default read sees both batches.
    assertThat(collectRows(tableEnv.executeSql("SELECT id FROM t"))).hasSize(3);

    String reader = createTableWithOption("rt", datasetUri, "scan.snapshot-id", String.valueOf(v1));
    tableEnv.executeSql(reader);
    Map<Long, String> rows = rowsAsMap(tableEnv.executeSql("SELECT id, name FROM rt"));
    assertThat(rows).containsOnly(Map.entry(1L, "a"), Map.entry(2L, "b"));
  }

  @Test
  void testReadAtTagName() throws Exception {
    String datasetUri = tempDir.resolve("ds-tag").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();
    long v1 = latestVersion(datasetUri);
    createTag(datasetUri, "first", v1);
    tableEnv.executeSql("INSERT INTO t VALUES (2, 'b'), (3, 'c')").await();

    String reader = createTableWithOption("rt", datasetUri, "scan.tag-name", "first");
    tableEnv.executeSql(reader);
    Map<Long, String> rows = rowsAsMap(tableEnv.executeSql("SELECT id, name FROM rt"));
    assertThat(rows).containsOnly(Map.entry(1L, "a"));
  }

  @Test
  void testReadAtTimestampMillis() throws Exception {
    String datasetUri = tempDir.resolve("ds-ts-millis").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();
    long v1 = latestVersion(datasetUri);
    long v1Millis = versionDataMillis(datasetUri, v1);
    Thread.sleep(10);
    tableEnv.executeSql("INSERT INTO t VALUES (2, 'b'), (3, 'c')").await();

    String reader =
        createTableWithOption("rt", datasetUri, "scan.timestamp-millis", String.valueOf(v1Millis));
    tableEnv.executeSql(reader);
    Map<Long, String> rows = rowsAsMap(tableEnv.executeSql("SELECT id, name FROM rt"));
    assertThat(rows).containsOnly(Map.entry(1L, "a"));
  }

  @Test
  void testReadAtTimestampStringRejectsTooEarly() throws Exception {
    String datasetUri = tempDir.resolve("ds-ts-too-early").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();

    String reader =
        createTableWithOption("rt", datasetUri, "scan.timestamp", "1970-01-01 00:00:00");
    tableEnv.executeSql(reader);
    assertThatThrownBy(() -> collectRows(tableEnv.executeSql("SELECT id FROM rt")))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .rootCause()
        .hasMessageContaining("scan.timestamp");
  }

  @Test
  void testScanOptionsMutualExclusion() throws Exception {
    String datasetUri = tempDir.resolve("ds-mutex").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();

    // Validation happens when the source is built for query, not at DDL time.
    tableEnv.executeSql(
        "CREATE TABLE rt (id BIGINT, name STRING) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(datasetUri)
            + ", 'scan.snapshot-id' = '1', "
            + "'scan.tag-name' = 'whatever')");
    assertThatThrownBy(() -> tableEnv.executeSql("SELECT id FROM rt"))
        .isInstanceOf(ValidationException.class)
        .rootCause()
        .hasMessageContaining("Only one Lance time-travel option may be set")
        .hasMessageContaining("scan.snapshot-id")
        .hasMessageContaining("scan.tag-name");
  }

  @Test
  void testReadAtScanVersionAlias() throws Exception {
    String datasetUri = tempDir.resolve("ds-scan-version").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();
    long v1 = latestVersion(datasetUri);
    tableEnv.executeSql("INSERT INTO t VALUES (2, 'b')").await();

    String reader = createTableWithOption("rt", datasetUri, "scan.version", String.valueOf(v1));
    tableEnv.executeSql(reader);
    Map<Long, String> rows = rowsAsMap(tableEnv.executeSql("SELECT id, name FROM rt"));
    assertThat(rows).containsOnly(Map.entry(1L, "a"));
  }

  @Test
  void testScanVersionAndSnapshotIdAreMutuallyExclusive() throws Exception {
    String datasetUri = tempDir.resolve("ds-version-vs-snapshot").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();

    tableEnv.executeSql(
        "CREATE TABLE rt (id BIGINT, name STRING) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(datasetUri)
            + ", 'scan.version' = '1', "
            + "'scan.snapshot-id' = '1')");
    assertThatThrownBy(() -> tableEnv.executeSql("SELECT id FROM rt"))
        .isInstanceOf(ValidationException.class)
        .rootCause()
        .hasMessageContaining("Only one Lance time-travel option may be set")
        .hasMessageContaining("scan.version")
        .hasMessageContaining("scan.snapshot-id");
  }

  @Test
  void testHintBasedSnapshotId() throws Exception {
    String datasetUri = tempDir.resolve("ds-hint").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.getConfig().set("table.dynamic-table-options.enabled", "true");
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();
    long v1 = latestVersion(datasetUri);
    tableEnv.executeSql("INSERT INTO t VALUES (2, 'b')").await();

    String hint = "/*+ OPTIONS('scan.snapshot-id'='" + v1 + "') */";
    Map<Long, String> rows = rowsAsMap(tableEnv.executeSql("SELECT id, name FROM t " + hint));
    assertThat(rows).containsOnly(Map.entry(1L, "a"));
  }

  @Test
  void testGroupBy() throws Exception {
    String datasetUri = tempDir.resolve("ds-aggregate").toUri().toString();
    TableEnvironment tableEnv = batchEnv();
    tableEnv.executeSql(createTable("t", datasetUri));
    tableEnv
        .executeSql(
            "INSERT INTO t VALUES " + "(1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'b'), (6, 'c')")
        .await();

    Map<String, Long> counts = new HashMap<>();
    for (Row row : collectRows(tableEnv.executeSql("SELECT name, COUNT(*) FROM t GROUP BY name"))) {
      counts.put((String) row.getField(0), (Long) row.getField(1));
    }
    assertThat(counts).containsOnly(Map.entry("a", 2L), Map.entry("b", 3L), Map.entry("c", 1L));
  }

  private static TableEnvironment batchEnv() {
    return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
  }

  private static String createTable(String name, String datasetUri) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT, name STRING) WITH ("
        + "'connector' = 'lance', "
        + "'path' = "
        + sql(datasetUri)
        + ")";
  }

  private static String createTableWithOption(
      String name, String datasetUri, String optionKey, String optionValue) {
    return "CREATE TABLE "
        + name
        + " (id BIGINT, name STRING) WITH ("
        + "'connector' = 'lance', "
        + "'path' = "
        + sql(datasetUri)
        + ", "
        + sql(optionKey)
        + " = "
        + sql(optionValue)
        + ")";
  }

  private static long latestVersion(String datasetUri) {
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      return ds.version();
    }
  }

  private static long versionDataMillis(String datasetUri, long version) {
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset ds =
            Dataset.open()
                .readOptions(new ReadOptions.Builder().setVersion(version).build())
                .allocator(alloc)
                .uri(datasetUri)
                .build()) {
      for (Version v : ds.listVersions()) {
        if (v.getId() == version) {
          return v.getDataTime().toInstant().toEpochMilli();
        }
      }
      throw new IllegalStateException("Version " + version + " not found in " + datasetUri);
    }
  }

  private static void createTag(String datasetUri, String tagName, long version) {
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      ds.tags().create(tagName, version);
    }
  }

  private static Map<Long, String> rowsAsMap(TableResult result) throws Exception {
    Map<Long, String> rows = new HashMap<>();
    for (Row row : collectRows(result)) {
      rows.put((Long) row.getField(0), (String) row.getField(1));
    }
    return rows;
  }

  private static List<Row> collectRows(TableResult result) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> it = result.collect()) {
      while (it.hasNext()) {
        rows.add(it.next());
      }
    }
    return rows;
  }

  private static String sql(String value) {
    return "'" + value.replace("'", "''") + "'";
  }
}
