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
package org.apache.flink.connector.lance.table;

import org.lance.Dataset;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end SQL tests for the {@code <table>$<suffix>} metadata-table feature. The namespace
 * catalog synthesizes the {@link org.apache.flink.table.catalog.CatalogTable} so users query {@code
 * SELECT * FROM t$snapshots} with no DDL beyond the base table.
 */
class LanceMetadataTablesITCase {

  @TempDir Path tempDir;

  @Test
  void snapshotsListsCommittedVersionsInAscendingOrder() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT, name STRING)");
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();
    tableEnv.executeSql("INSERT INTO t VALUES (2, 'b')").await();
    tableEnv.executeSql("INSERT INTO t VALUES (3, 'c')").await();

    List<Row> rows =
        collectRows(
            tableEnv.executeSql("SELECT version_id FROM `t$snapshots` ORDER BY version_id"));
    assertThat(rows.size()).isGreaterThanOrEqualTo(3);
    assertThat(rows).extracting(r -> (Long) r.getField(0)).isSorted();
  }

  @Test
  void snapshotsCarriesCommitTimeAndMetadata() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT, name STRING)");
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'x')").await();

    List<Row> rows =
        collectRows(
            tableEnv.executeSql(
                "SELECT version_id, commit_time, metadata FROM `t$snapshots` LIMIT 1"));
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getField(1)).isNotNull();
    assertThat(rows.get(0).getField(2)).isNotNull();
  }

  @Test
  void tagsReturnsCreatedTags() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT, name STRING)");
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();
    String datasetUri = describeTablePath(tableEnv, "t");
    long v1 = latestVersion(datasetUri);
    createTag(datasetUri, "release-1", v1);
    tableEnv.executeSql("INSERT INTO t VALUES (2, 'b')").await();
    long v2 = latestVersion(datasetUri);
    createTag(datasetUri, "release-2", v2);

    List<Row> rows =
        collectRows(
            tableEnv.executeSql("SELECT tag_name, version_id FROM `t$tags` ORDER BY tag_name"));
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getField(0)).isEqualTo("release-1");
    assertThat(rows.get(0).getField(1)).isEqualTo(v1);
    assertThat(rows.get(1).getField(0)).isEqualTo("release-2");
    assertThat(rows.get(1).getField(1)).isEqualTo(v2);
  }

  @Test
  void branchesIsEmptyForFreshTable() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT, name STRING)");
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();

    List<Row> rows = collectRows(tableEnv.executeSql("SELECT * FROM `t$branches`"));
    assertThat(rows).isEmpty();
  }

  @Test
  void fragmentsSumsToTotalRowCount() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT, name STRING)");
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b')").await();
    tableEnv.executeSql("INSERT INTO t VALUES (3, 'c')").await();

    List<Row> rows =
        collectRows(
            tableEnv.executeSql(
                "SELECT fragment_id, num_rows, has_deletion_file"
                    + " FROM `t$fragments` ORDER BY fragment_id"));
    assertThat(rows.size()).isGreaterThanOrEqualTo(2);
    long totalRows = 0L;
    for (Row r : rows) {
      totalRows += (Long) r.getField(1);
      assertThat((Boolean) r.getField(2)).isFalse();
    }
    assertThat(totalRows).isEqualTo(3L);
  }

  @Test
  void optionsReflectsBaseTableConnectorAndPath() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT, name STRING)");
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();

    Map<String, String> options = new HashMap<>();
    for (Row r :
        collectRows(
            tableEnv.executeSql(
                "SELECT option_key, option_value FROM `t$options` ORDER BY option_key"))) {
      options.put((String) r.getField(0), (String) r.getField(1));
    }
    assertThat(options).containsKey("connector").containsKey("path");
    assertThat(options).doesNotContainKey("metadata-type");
    assertThat(options.get("connector")).isEqualTo("lance");
  }

  @Test
  void unknownSuffixFallsThroughAndFailsAsMissingTable() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT, name STRING)");
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();

    assertThatThrownBy(() -> tableEnv.executeSql("SELECT * FROM `t$nope`"))
        .hasMessageContaining("not found");
  }

  @Test
  void timeTravelOnMetadataTableIsRejected() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT, name STRING)");
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a')").await();

    assertThatThrownBy(
            () ->
                tableEnv.executeSql(
                    "SELECT * FROM `t$snapshots` FOR SYSTEM_TIME AS OF TIMESTAMP '2030-01-01 00:00:00'"))
        .hasMessageContaining("Time travel is not supported on Lance metadata tables");
  }

  @Test
  void metadataTableResolvesWhenBaseTableExists() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE my_table (id BIGINT)");
    tableEnv.executeSql("INSERT INTO my_table VALUES (1)").await();

    List<Row> rows =
        collectRows(tableEnv.executeSql("SELECT version_id FROM `my_table$snapshots`"));
    assertThat(rows).isNotEmpty();

    LanceNamespaceCatalog catalog = (LanceNamespaceCatalog) tableEnv.getCatalog("c").orElseThrow();
    assertThat(catalog.tableExists(new ObjectPath("default", "my_table$snapshots"))).isTrue();
  }

  @Test
  void unknownSuffixDoesNotResolveThroughBaseTable() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE my_table (id BIGINT)");
    tableEnv.executeSql("INSERT INTO my_table VALUES (1)").await();

    // `my_table` exists, but `my_table$foobar` is not a known metadata suffix. The catalog must
    // not strip the suffix back to `my_table`; the SELECT must fail rather than silently return
    // `my_table`'s rows.
    assertThatThrownBy(() -> tableEnv.executeSql("SELECT * FROM `my_table$foobar`"))
        .hasStackTraceContaining("my_table$foobar");
  }

  @Test
  void cannotCreateMetadataNamedTable() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT)");

    assertThatThrownBy(() -> tableEnv.executeSql("CREATE TABLE `t$snapshots` (version_id BIGINT)"))
        .hasStackTraceContaining("Cannot create Lance metadata table");
  }

  @Test
  void cannotDropMetadataTable() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT)");
    tableEnv.executeSql("INSERT INTO t VALUES (1)").await();

    assertThatThrownBy(() -> tableEnv.executeSql("DROP TABLE `t$snapshots`"))
        .hasStackTraceContaining("Cannot drop Lance metadata table");
  }

  @Test
  void cannotRenameMetadataTable() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT)");
    tableEnv.executeSql("INSERT INTO t VALUES (1)").await();

    assertThatThrownBy(() -> tableEnv.executeSql("ALTER TABLE `t$snapshots` RENAME TO foo"))
        .hasStackTraceContaining("Cannot rename Lance metadata table");
  }

  @Test
  void cannotRenameToReservedMetadataName() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE foo (id BIGINT)");
    tableEnv.executeSql("INSERT INTO foo VALUES (1)").await();

    assertThatThrownBy(() -> tableEnv.executeSql("ALTER TABLE foo RENAME TO `bar$snapshots`"))
        .hasStackTraceContaining("reserved metadata name");
  }

  @Test
  void cannotAlterMetadataTable() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT)");
    tableEnv.executeSql("INSERT INTO t VALUES (1)").await();

    assertThatThrownBy(() -> tableEnv.executeSql("ALTER TABLE `t$snapshots` ADD x INT"))
        .hasStackTraceContaining("Cannot alter Lance metadata table");
  }

  @Test
  void optionsReturnsRowsSortedByKey() throws Exception {
    TableEnvironment tableEnv = catalogEnv();
    tableEnv.executeSql("CREATE TABLE t (id BIGINT)");
    tableEnv.executeSql("INSERT INTO t VALUES (1)").await();

    // No ORDER BY — the metadata reader itself must emit rows sorted by option_key.
    List<Row> rows = collectRows(tableEnv.executeSql("SELECT option_key FROM `t$options`"));
    List<String> keys = new ArrayList<>();
    for (Row r : rows) {
      keys.add((String) r.getField(0));
    }
    List<String> sorted = new ArrayList<>(keys);
    java.util.Collections.sort(sorted);
    assertThat(keys).isEqualTo(sorted);
  }

  @Test
  void rawDdlRejectsMetadataTypeCombinedWithScanOption() {
    TableEnvironment tableEnv =
        TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    String warehouse = tempDir.resolve("warehouse").toUri().toString();
    // Use the default catalog so we can write raw DDL with the lance connector directly.
    String ddl =
        "CREATE TABLE meta (`version_id` BIGINT) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(warehouse)
            + ", "
            + "'metadata-type' = 'snapshots', "
            + "'scan.version' = '1')";
    tableEnv.executeSql(ddl);
    assertThatThrownBy(() -> tableEnv.executeSql("SELECT * FROM meta"))
        .isInstanceOf(ValidationException.class)
        .rootCause()
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("not supported on metadata tables");
  }

  private TableEnvironment catalogEnv() {
    TableEnvironment tableEnv =
        TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    String warehouse = tempDir.resolve("warehouse").toUri().toString();
    tableEnv.executeSql(
        "CREATE CATALOG c WITH ('type' = 'lance', 'warehouse' = " + sql(warehouse) + ")");
    tableEnv.executeSql("USE CATALOG c");
    return tableEnv;
  }

  private static String describeTablePath(TableEnvironment tableEnv, String tableName)
      throws Exception {
    LanceNamespaceCatalog catalog = (LanceNamespaceCatalog) tableEnv.getCatalog("c").orElseThrow();
    LanceNamespace ns = catalogNamespace(catalog);
    DescribeTableResponse resp =
        ns.describeTable(new DescribeTableRequest().id(Arrays.asList("default", tableName)));
    return resp.getLocation();
  }

  private static LanceNamespace catalogNamespace(LanceNamespaceCatalog catalog) throws Exception {
    Field f = LanceNamespaceCatalog.class.getDeclaredField("namespace");
    f.setAccessible(true);
    return (LanceNamespace) f.get(catalog);
  }

  private static long latestVersion(String datasetUri) {
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      return ds.version();
    }
  }

  private static void createTag(String datasetUri, String tagName, long version) {
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      ds.tags().create(tagName, version);
    }
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
