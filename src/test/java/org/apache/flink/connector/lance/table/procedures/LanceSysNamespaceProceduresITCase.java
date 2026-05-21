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
package org.apache.flink.connector.lance.table.procedures;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Observational ITCases pinning Flink 1.19.1 behavior of the virtual {@code sys} namespace and
 * generic error paths shared across procedures (unknown procedure, unknown table). The procedure
 * surface contract is only {@code CALL sys.<procedure>(...)} from the catalog's normal context;
 * {@code SHOW TABLES IN sys}, {@code USE sys}, and {@code CREATE DATABASE sys} are observational —
 * the tests accept either Flink's clean-failure or success path.
 */
class LanceSysNamespaceProceduresITCase extends AbstractLanceProcedureITCase {

  @Test
  void unknownProcedureRejects() {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "anything");

    assertThat(
            collectSqlErrorMessage(
                () -> tEnv.executeSql("CALL sys.no_such_procedure(`table` => 'default.anything')")))
        .isNotNull()
        .satisfiesAnyOf(
            msg -> assertThat(msg).contains("no_such_procedure"),
            msg -> assertThat(msg).contains("does not exist"),
            msg -> assertThat(msg).contains("not exist"));
  }

  @Test
  void compactRejectsUnknownTable() {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "compact_unknown");

    assertThat(
            collectSqlErrorMessage(
                () ->
                    tEnv.executeSql("CALL sys.compact(`table` => 'default.does_not_exist')")
                        .await()))
        .isNotNull()
        .satisfiesAnyOf(
            msg -> assertThat(msg).contains("does_not_exist"),
            msg -> assertThat(msg).contains("not found"),
            msg -> assertThat(msg).contains("Table not found"));
  }

  @Test
  void listProceduresReturnsRegistry() {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "anything");

    List<Row> rows = collectRows(tEnv.executeSql("SHOW PROCEDURES IN sys"));
    List<String> names = rows.stream().map(r -> (String) r.getField(0)).sorted().toList();
    assertThat(names).contains("compact", "expire_snapshots", "create_tag", "delete_tag");
  }

  @Test
  void sysIsNotAVisibleDatabase() {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "anything");

    // sys is not added to SHOW DATABASES.
    List<Row> rows = collectRows(tEnv.executeSql("SHOW DATABASES"));
    List<String> databases = rows.stream().map(r -> (String) r.getField(0)).toList();
    assertThat(databases).doesNotContain("sys");
  }

  @Test
  void showTablesInSysReportsExpectedBehavior() {
    // sys is not a real namespace and listProcedures isn't listTables; pin whatever Flink does in
    // a single executeSql roundtrip — either it succeeds and returns no tables, or it fails with a
    // message naming sys.
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "anything");

    List<Row> rows = new ArrayList<>();
    String error =
        collectSqlErrorMessage(
            () -> rows.addAll(collectRows(tEnv.executeSql("SHOW TABLES IN sys"))));
    if (error == null) {
      assertThat(rows).isEmpty();
    } else {
      assertThat(error)
          .satisfiesAnyOf(
              msg -> assertThat(msg).contains("sys"),
              msg -> assertThat(msg).contains("does not exist"),
              msg -> assertThat(msg).contains("not exist"));
    }
  }

  @Test
  void useSysReportsExpectedBehavior() {
    // sys is not in listDatabases(); USE sys most likely fails. Pin either a clean failure or a
    // successful no-op switch (whichever Flink elects).
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "anything");

    String error = collectSqlErrorMessage(() -> tEnv.executeSql("USE sys"));
    if (error != null) {
      assertThat(error)
          .satisfiesAnyOf(
              msg -> assertThat(msg).contains("sys"),
              msg -> assertThat(msg).contains("does not exist"),
              msg -> assertThat(msg).contains("not exist"));
    }
    // If no error, USE sys silently succeeded — both behaviors are acceptable; the contract is
    // only that CALL sys.<procedure>(...) works from the catalog's normal context.
  }

  @Test
  void createDatabaseSysReportsExpectedBehavior() {
    // Whether the namespace impl accepts creating a real `sys` database is impl-specific;
    // DirectoryNamespace currently allows it. Pin observed behavior either way: success creates a
    // real database (so SHOW DATABASES contains "sys") or rejection raises an error.
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "anything");

    String error = collectSqlErrorMessage(() -> tEnv.executeSql("CREATE DATABASE sys"));
    if (error == null) {
      List<Row> dbs = collectRows(tEnv.executeSql("SHOW DATABASES"));
      List<String> names = dbs.stream().map(r -> (String) r.getField(0)).toList();
      assertThat(names).contains("sys");
    } else {
      assertThat(error)
          .satisfiesAnyOf(
              msg -> assertThat(msg).contains("sys"),
              msg -> assertThat(msg).contains("already exists"),
              msg -> assertThat(msg).contains("reserved"),
              msg -> assertThat(msg).contains("not supported"));
    }
  }
}
