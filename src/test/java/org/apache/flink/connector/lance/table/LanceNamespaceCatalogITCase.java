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

import org.apache.flink.connector.lance.source.scan.LanceScanOptions;

import org.lance.Dataset;
import org.lance.Version;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end SQL scenario for the LanceNamespace-backed catalog. */
class LanceNamespaceCatalogITCase {

  @TempDir Path tempDir;

  @Test
  void testCreateInsertAndSelectWithNamespaceCatalog() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    String warehouseUri = tempDir.toUri().toString();

    tableEnv.executeSql(
        "CREATE CATALOG my_catalog WITH ("
            + "'type' = 'lance', "
            + "'warehouse' = "
            + sqlString(warehouseUri)
            + ")");

    tableEnv.executeSql("USE CATALOG my_catalog");

    tableEnv.executeSql("CREATE TABLE word_count (word STRING, cnt BIGINT)");

    List<String> tables = collectFirstColumnAsStrings(tableEnv.executeSql("SHOW TABLES"));
    assertThat(tables).contains("word_count");

    TableResult insertResult =
        tableEnv.executeSql("INSERT INTO word_count VALUES ('a', CAST(10 AS BIGINT))");
    insertResult.await();

    List<Row> rows = collectRows(tableEnv.executeSql("SELECT * FROM word_count"));

    assertThat(rows).containsExactly(Row.of("a", 10L));
  }

  @Test
  void testWordCountUpsertWithPrimaryKey() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    String warehouseUri = tempDir.toUri().toString();

    tableEnv.executeSql(
        "CREATE CATALOG my_catalog WITH ("
            + "'type' = 'lance', "
            + "'warehouse' = "
            + sqlString(warehouseUri)
            + ")");

    tableEnv.executeSql("USE CATALOG my_catalog");

    // Paimon-style PK DDL — the constraint must reach the dynamic sink so the
    // aggregation's retract stream can be normalized into upserts.
    tableEnv.executeSql(
        "CREATE TABLE word_count (word STRING PRIMARY KEY NOT ENFORCED, cnt BIGINT)");

    // Source rows with duplicate words; GROUP BY produces an upsert stream.
    tableEnv.executeSql(
        "CREATE TEMPORARY VIEW word_source AS "
            + "SELECT word FROM (VALUES ('hello'), ('world'), ('hello'), ('flink'), ('hello'), "
            + "('world')) AS t(word)");

    tableEnv
        .executeSql("INSERT INTO word_count SELECT word, COUNT(*) FROM word_source GROUP BY word")
        .await();

    Map<String, Long> counts = new HashMap<>();
    for (Row row : collectRows(tableEnv.executeSql("SELECT word, cnt FROM word_count"))) {
      counts.put((String) row.getField(0), (Long) row.getField(1));
    }
    assertThat(counts).containsOnly(entry("hello", 3L), entry("world", 2L), entry("flink", 1L));

    // Re-running the upsert with a different distribution must overwrite by key.
    tableEnv.executeSql(
        "CREATE TEMPORARY VIEW word_source2 AS "
            + "SELECT word FROM (VALUES ('hello'), ('flink'), ('flink'), ('flink')) AS t(word)");
    tableEnv
        .executeSql("INSERT INTO word_count SELECT word, COUNT(*) FROM word_source2 GROUP BY word")
        .await();

    counts.clear();
    for (Row row : collectRows(tableEnv.executeSql("SELECT word, cnt FROM word_count"))) {
      counts.put((String) row.getField(0), (Long) row.getField(1));
    }
    // hello and flink were re-emitted with new counts, world was not in the second batch and
    // therefore stays at its previous value.
    assertThat(counts).containsOnly(entry("hello", 1L), entry("world", 2L), entry("flink", 3L));
  }

  @Test
  void testCreateTableAsSelectPreservesNotNull() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");

    tableEnv.executeSql(
        "CREATE TABLE word_count (word STRING PRIMARY KEY NOT ENFORCED, cnt BIGINT)");
    tableEnv.executeSql("INSERT INTO word_count VALUES ('a', CAST(1 AS BIGINT))").await();

    // Plain CTAS: the PK is dropped (Flink CTAS does not propagate constraints) but column
    // nullability is preserved end-to-end through Arrow — matches Paimon parity.
    tableEnv.executeSql("CREATE TABLE word_count_as AS SELECT * FROM word_count").await();

    List<Row> rows = collectRows(tableEnv.executeSql("SELECT word, cnt FROM word_count_as"));
    assertThat(rows).containsExactly(Row.of("a", 1L));

    ResolvedSchema schema = tableEnv.from("word_count_as").getResolvedSchema();
    assertThat(schema.getPrimaryKey()).isEmpty();
    assertThat(nullabilityOf(schema, "word")).isFalse();
    assertThat(nullabilityOf(schema, "cnt")).isTrue();
  }

  @Test
  void testCreateTableAsSelectWithPrimaryKeyOption() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");

    tableEnv.executeSql(
        "CREATE TABLE word_count (word STRING PRIMARY KEY NOT ENFORCED, cnt BIGINT)");
    tableEnv.executeSql("INSERT INTO word_count VALUES ('a', CAST(1 AS BIGINT))").await();

    // The 'primary-key' option recovers the PK that CTAS would otherwise drop and promotes
    // its columns to NOT NULL — Paimon-compatible recovery path.
    tableEnv
        .executeSql(
            "CREATE TABLE word_count_copy WITH ('primary-key' = 'word') "
                + "AS SELECT * FROM word_count")
        .await();

    List<Row> rows = collectRows(tableEnv.executeSql("SELECT word, cnt FROM word_count_copy"));
    assertThat(rows).containsExactly(Row.of("a", 1L));

    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    CatalogBaseTable copy = catalog.getTable(new ObjectPath("default", "word_count_copy"));
    assertThat(copy.getUnresolvedSchema().getPrimaryKey().orElseThrow().getColumnNames())
        .containsExactly("word");
  }

  @Test
  void testCreateTableLikeRequiresExcludingOptions() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");

    tableEnv.executeSql(
        "CREATE TABLE word_count (word STRING PRIMARY KEY NOT ENFORCED, cnt BIGINT)");

    // Paimon parity: bare LIKE forwards the catalog-synthesized 'connector' and 'path'
    // options, which the catalog refuses on user-authored DDL.
    assertThatThrownBy(() -> tableEnv.executeSql("CREATE TABLE word_count_like LIKE word_count"))
        .hasStackTraceContaining(
            "Table option 'path' is not supported when creating tables in Lance namespace catalog");

    // EXCLUDING OPTIONS strips the forwarded options and the table is created.
    tableEnv.executeSql("CREATE TABLE word_count_like LIKE word_count (EXCLUDING OPTIONS)");

    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    CatalogBaseTable like = catalog.getTable(new ObjectPath("default", "word_count_like"));
    assertThat(like.getUnresolvedSchema().getPrimaryKey().orElseThrow().getColumnNames())
        .containsExactly("word");

    ResolvedSchema schema = tableEnv.from("word_count_like").getResolvedSchema();
    assertThat(nullabilityOf(schema, "word")).isFalse();
    assertThat(nullabilityOf(schema, "cnt")).isTrue();
  }

  @Test
  void testCreateTableRejectsUnknownOptionsButAllowsPrimaryKey() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");

    tableEnv.executeSql(
        "CREATE TABLE pk_only (word STRING, cnt BIGINT) WITH ('primary-key' = 'word')");

    assertThatThrownBy(
            () ->
                tableEnv.executeSql(
                    "CREATE TABLE bad_opts (word STRING, cnt BIGINT) "
                        + "WITH ('primary-key' = 'word', 'write.batch-size' = '128')"))
        .hasStackTraceContaining("Unsupported table options in Lance namespace catalog");

    assertThatThrownBy(
            () ->
                tableEnv.executeSql(
                    "CREATE TABLE wrong_connector (word STRING) WITH ('connector' = 'kafka')"))
        .hasStackTraceContaining("Unsupported table options in Lance namespace catalog");
  }

  private TableEnvironment newCatalog(String name) {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    tableEnv.executeSql(
        "CREATE CATALOG "
            + name
            + " WITH ('type' = 'lance', 'warehouse' = "
            + sqlString(tempDir.toUri().toString())
            + ")");
    tableEnv.executeSql("USE CATALOG " + name);
    return tableEnv;
  }

  @Test
  void testForSystemTimeAsOfReadsHistoricalVersion() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    // Pin the session timezone so the TIMESTAMP literal we build below round-trips
    // deterministically
    // through Flink's session-tz-aware conversion.
    tableEnv.getConfig().setLocalTimeZone(ZoneOffset.UTC);

    tableEnv.executeSql(
        "CREATE CATALOG my_catalog WITH ("
            + "'type' = 'lance', "
            + "'warehouse' = "
            + sqlString(tempDir.toUri().toString())
            + ")");
    tableEnv.executeSql("USE CATALOG my_catalog");
    tableEnv.executeSql("CREATE TABLE word_count (word STRING, cnt BIGINT)");

    tableEnv.executeSql("INSERT INTO word_count VALUES ('a', CAST(10 AS BIGINT))").await();
    long afterFirstMillis = latestVersionDataTime(tableEnv, "word_count");

    Thread.sleep(200);

    tableEnv.executeSql("INSERT INTO word_count VALUES ('b', CAST(20 AS BIGINT))").await();

    // No time travel: both rows visible.
    assertThat(collectRows(tableEnv.executeSql("SELECT word FROM word_count"))).hasSize(2);

    // Pick a timestamp strictly between the two inserts (sleep above guarantees the gap).
    long queryMillis = afterFirstMillis + 100;
    String literal =
        Instant.ofEpochMilli(queryMillis)
            .atZone(ZoneOffset.UTC)
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    List<Row> rows =
        collectRows(
            tableEnv.executeSql(
                "SELECT word, cnt FROM word_count FOR SYSTEM_TIME AS OF TIMESTAMP '"
                    + literal
                    + "'"));

    assertThat(rows).containsExactly(Row.of("a", 10L));
  }

  @Test
  void testCatalogTimeTravelPinsOptionsToScanVersion() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE pinned (id BIGINT)");
    tableEnv.executeSql("INSERT INTO pinned VALUES (CAST(1 AS BIGINT))").await();
    long afterFirstMillis = latestVersionDataTime(tableEnv, "pinned");

    Thread.sleep(200);

    tableEnv.executeSql("INSERT INTO pinned VALUES (CAST(2 AS BIGINT))").await();
    long latestVersion = latestVersionOf(tableEnv, "pinned");

    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    CatalogBaseTable table =
        catalog.getTable(new ObjectPath("default", "pinned"), afterFirstMillis + 100);

    Map<String, String> options = table.getOptions();
    assertThat(options).containsKey(LanceScanOptions.SCAN_VERSION.key());
    assertThat(options).doesNotContainKey(LanceScanOptions.SCAN_TIMESTAMP_MILLIS.key());
    long pinnedVersion = Long.parseLong(options.get(LanceScanOptions.SCAN_VERSION.key()));
    assertThat(pinnedVersion).isGreaterThanOrEqualTo(1L).isLessThan(latestVersion);
  }

  @Test
  void testCatalogTimeTravelUsesHistoricalSchema() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE evolving (id BIGINT)");
    tableEnv.executeSql("INSERT INTO evolving VALUES (CAST(1 AS BIGINT))").await();
    long afterFirstMillis = latestVersionDataTime(tableEnv, "evolving");

    Thread.sleep(200);

    // Evolve the schema at the Lance level (bypassing Flink) so a column appears in newer versions
    // but not in earlier ones.
    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    String datasetPath =
        catalog.getTable(new ObjectPath("default", "evolving")).getOptions().get("path");
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetPath).build()) {
      ds.addColumns(
          List.of(
              new org.apache.arrow.vector.types.pojo.Field(
                  "name",
                  org.apache.arrow.vector.types.pojo.FieldType.nullable(
                      new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()),
                  null)));
    }

    // Latest: 2 columns.
    CatalogBaseTable current = catalog.getTable(new ObjectPath("default", "evolving"));
    assertThat(current.getUnresolvedSchema().getColumns()).hasSize(2);

    // Historical (before addColumns): 1 column.
    CatalogBaseTable historical =
        catalog.getTable(new ObjectPath("default", "evolving"), afterFirstMillis + 100);
    assertThat(historical.getUnresolvedSchema().getColumns()).hasSize(1);
    assertThat(historical.getUnresolvedSchema().getColumns().get(0).getName()).isEqualTo("id");
  }

  @Test
  void testCatalogTimeTravelRejectsTooEarlyTimestamp() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE too_early (id BIGINT)");
    tableEnv.executeSql("INSERT INTO too_early VALUES (CAST(1 AS BIGINT))").await();

    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    assertThatThrownBy(() -> catalog.getTable(new ObjectPath("default", "too_early"), 0L))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Cannot time-travel to timestamp 0")
        .hasMessageContaining("older than the dataset's earliest available version");
  }

  @Test
  void testNormalGetTableHasNoScanOptions() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE plain (id BIGINT)");
    tableEnv.executeSql("INSERT INTO plain VALUES (CAST(1 AS BIGINT))").await();

    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    Map<String, String> options = catalog.getTable(new ObjectPath("default", "plain")).getOptions();

    assertThat(options).containsOnlyKeys("connector", "path");
    for (org.apache.flink.configuration.ConfigOption<?> scanOption : LanceScanOptions.ALL_OPTIONS) {
      assertThat(options).doesNotContainKey(scanOption.key());
    }
  }

  @Test
  void testAlterTableAddColumnWithNullBackfill() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE customers (id INT, name STRING)");
    tableEnv.executeSql("INSERT INTO customers VALUES (1, 'alice'), (2, 'bob')").await();

    tableEnv.executeSql("ALTER TABLE customers ADD email STRING");

    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    CatalogBaseTable described = catalog.getTable(new ObjectPath("default", "customers"));
    assertThat(described.getUnresolvedSchema().getColumns())
        .extracting(col -> col.getName())
        .containsExactly("id", "name", "email");

    List<Row> rowsAfterAdd = collectRows(tableEnv.executeSql("SELECT id, email FROM customers"));
    assertThat(rowsAfterAdd).containsExactlyInAnyOrder(Row.of(1, null), Row.of(2, null));

    tableEnv
        .executeSql(
            "INSERT INTO customers VALUES (3, 'carol', 'carol@example.com'),"
                + " (4, 'dave', 'dave@example.com')")
        .await();

    List<Row> mixedRows =
        collectRows(tableEnv.executeSql("SELECT id, email FROM customers ORDER BY id"));
    assertThat(mixedRows)
        .containsExactly(
            Row.of(1, null),
            Row.of(2, null),
            Row.of(3, "carol@example.com"),
            Row.of(4, "dave@example.com"));
  }

  @Test
  void testAlterTableAddMultipleColumnsInOneStatement() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE multi_add (id INT)");
    tableEnv.executeSql("INSERT INTO multi_add VALUES (1), (2)").await();

    tableEnv.executeSql("ALTER TABLE multi_add ADD (email STRING, phone STRING)");

    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    CatalogBaseTable described = catalog.getTable(new ObjectPath("default", "multi_add"));
    assertThat(described.getUnresolvedSchema().getColumns())
        .extracting(col -> col.getName())
        .containsExactly("id", "email", "phone");

    List<Row> rows =
        collectRows(tableEnv.executeSql("SELECT id, email, phone FROM multi_add ORDER BY id"));
    assertThat(rows).containsExactly(Row.of(1, null, null), Row.of(2, null, null));
  }

  @Test
  void testAlterTableRenameColumn() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE renamed (id INT, label STRING)");
    tableEnv.executeSql("INSERT INTO renamed VALUES (1, 'a'), (2, 'b')").await();

    tableEnv.executeSql("ALTER TABLE renamed RENAME label TO display_name");

    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    CatalogBaseTable described = catalog.getTable(new ObjectPath("default", "renamed"));
    assertThat(described.getUnresolvedSchema().getColumns())
        .extracting(col -> col.getName())
        .containsExactly("id", "display_name");

    List<Row> rows =
        collectRows(tableEnv.executeSql("SELECT id, display_name FROM renamed ORDER BY id"));
    assertThat(rows).containsExactly(Row.of(1, "a"), Row.of(2, "b"));
  }

  @Test
  void testAlterTableDropColumn() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE dropped (id INT, label STRING, extra BIGINT)");
    tableEnv.executeSql("INSERT INTO dropped VALUES (1, 'a', CAST(10 AS BIGINT))").await();

    tableEnv.executeSql("ALTER TABLE dropped DROP extra");

    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    CatalogBaseTable described = catalog.getTable(new ObjectPath("default", "dropped"));
    assertThat(described.getUnresolvedSchema().getColumns())
        .extracting(col -> col.getName())
        .containsExactly("id", "label");

    List<Row> rows = collectRows(tableEnv.executeSql("SELECT * FROM dropped"));
    assertThat(rows).containsExactly(Row.of(1, "a"));
  }

  @Test
  void testAlterTableModifyTypeRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE modify_type (id INT, label STRING)");

    assertThatThrownBy(() -> tableEnv.executeSql("ALTER TABLE modify_type MODIFY id BIGINT"))
        .hasStackTraceContaining("ALTER MODIFY column 'id'")
        .hasStackTraceContaining("lance-core does not yet apply");
  }

  @Test
  void testAlterTableAddNotNullRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE reject_notnull (id INT)");

    assertThatThrownBy(
            () -> tableEnv.executeSql("ALTER TABLE reject_notnull ADD email STRING NOT NULL"))
        .hasStackTraceContaining("ALTER ADD column 'email'")
        .hasStackTraceContaining("nullable");
  }

  @Test
  void testAlterTableAddWithFirstPositionRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE reject_pos (id INT, name STRING)");

    assertThatThrownBy(
            () -> tableEnv.executeSql("ALTER TABLE reject_pos ADD created_at TIMESTAMP(3) FIRST"))
        .hasStackTraceContaining("Column positions (FIRST / AFTER) are not supported");
  }

  @Test
  void testAlterTableDropPrimaryKeyRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql(
        "CREATE TABLE pk_drop (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)");

    // Flink itself rejects the DDL before our catalog sees it; either Flink's
    // "used as the primary key" message or our planner's wording is acceptable.
    assertThatThrownBy(() -> tableEnv.executeSql("ALTER TABLE pk_drop DROP id"))
        .hasStackTraceContaining("primary key");
  }

  @Test
  void testAlterTableRenamePrimaryKeyRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql(
        "CREATE TABLE pk_rename (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)");

    assertThatThrownBy(() -> tableEnv.executeSql("ALTER TABLE pk_rename RENAME id TO new_id"))
        .hasStackTraceContaining("Primary key columns cannot be renamed");
  }

  @Test
  void testAlterTableModifyPrimaryKeyTypeRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql(
        "CREATE TABLE pk_modify (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)");

    // MODIFY is rejected before PK-specific type validation in this PR.
    assertThatThrownBy(() -> tableEnv.executeSql("ALTER TABLE pk_modify MODIFY id BIGINT"))
        .hasStackTraceContaining("ALTER MODIFY column 'id'");
  }

  @Test
  void testAlterTableSetRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE set_options (id INT)");

    assertThatThrownBy(
            () -> tableEnv.executeSql("ALTER TABLE set_options SET ('owner.team' = 'data')"))
        .hasStackTraceContaining("ALTER TABLE SET")
        .hasStackTraceContaining("not supported yet");
  }

  @Test
  void testAlterTableResetRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE reset_options (id INT)");

    assertThatThrownBy(() -> tableEnv.executeSql("ALTER TABLE reset_options RESET ('owner.team')"))
        .hasStackTraceContaining("ALTER TABLE RESET")
        .hasStackTraceContaining("not supported yet");
  }

  @Test
  void testAlterTableMetadataTableRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE meta_alter (id INT)");
    tableEnv.executeSql("INSERT INTO meta_alter VALUES (1)").await();

    assertThatThrownBy(
            () -> tableEnv.executeSql("ALTER TABLE `meta_alter$snapshots` ADD foo STRING"))
        .hasStackTraceContaining("Cannot alter Lance metadata table");
  }

  @Test
  void testAlterTableRenameToWithDirectoryNamespaceRejected() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE to_rename (id INT)");

    assertThatThrownBy(() -> tableEnv.executeSql("ALTER TABLE to_rename RENAME TO renamed_target"))
        .hasStackTraceContaining(
            "Table rename is not supported by this Lance namespace implementation");
  }

  @Test
  void testAlterTableSchemaPersistsAcrossReopen() throws Exception {
    TableEnvironment tableEnv = newCatalog("my_catalog");
    tableEnv.executeSql("CREATE TABLE persisted (id INT, name STRING)");
    tableEnv.executeSql("INSERT INTO persisted VALUES (1, 'a')").await();
    tableEnv.executeSql("ALTER TABLE persisted ADD email STRING");

    // Re-create the catalog against the same warehouse and verify the new column survives.
    TableEnvironment reopened = newCatalog("reopened_catalog");
    Catalog catalog = reopened.getCatalog("reopened_catalog").orElseThrow();
    CatalogBaseTable described = catalog.getTable(new ObjectPath("default", "persisted"));
    assertThat(described.getUnresolvedSchema().getColumns())
        .extracting(col -> col.getName())
        .containsExactly("id", "name", "email");
  }

  private static long latestVersionOf(TableEnvironment tableEnv, String tableName)
      throws Exception {
    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    CatalogBaseTable table = catalog.getTable(new ObjectPath("default", tableName));
    String datasetPath = table.getOptions().get("path");
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetPath).build()) {
      return ds.latestVersion();
    }
  }

  private static long latestVersionDataTime(TableEnvironment tableEnv, String tableName)
      throws Exception {
    Catalog catalog = tableEnv.getCatalog("my_catalog").orElseThrow();
    CatalogBaseTable table = catalog.getTable(new ObjectPath("default", tableName));
    String datasetPath = table.getOptions().get("path");
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetPath).build()) {
      long latest = ds.latestVersion();
      for (Version v : ds.listVersions()) {
        if (v.getId() == latest) {
          return v.getDataTime().toInstant().toEpochMilli();
        }
      }
      throw new IllegalStateException("Latest version not found for " + tableName);
    }
  }

  private static boolean nullabilityOf(ResolvedSchema schema, String column) {
    LogicalType type = schema.getColumn(column).orElseThrow().getDataType().getLogicalType();
    return type.isNullable();
  }

  private static Map.Entry<String, Long> entry(String key, Long value) {
    return new AbstractMap.SimpleEntry<>(key, value);
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

  private static List<String> collectFirstColumnAsStrings(TableResult result) throws Exception {
    List<String> values = new ArrayList<>();
    try (CloseableIterator<Row> it = result.collect()) {
      while (it.hasNext()) {
        values.add(String.valueOf(it.next().getField(0)));
      }
    }
    return values;
  }

  private static String sqlString(String value) {
    return "'" + value.replace("'", "''") + "'";
  }
}
