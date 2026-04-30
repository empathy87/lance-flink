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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

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
