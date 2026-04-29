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
import java.util.ArrayList;
import java.util.List;

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
