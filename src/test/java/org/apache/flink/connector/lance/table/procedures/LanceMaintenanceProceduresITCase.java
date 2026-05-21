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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end SQL tests for the dataset maintenance procedures: {@code sys.compact}, {@code
 * sys.expire_snapshots}, {@code sys.optimize_indices}.
 */
class LanceMaintenanceProceduresITCase extends AbstractLanceProcedureITCase {

  @Test
  void compactProducesPopulatedMetrics() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "customers");

    String datasetPath = resolveTablePath(tEnv, "customers");

    // Seed a compactable layout: 5 fragments of 100 rows each via writeMaxRowsPerFile=100.
    seedSmallFragments(datasetPath, 5, 100);

    int fragmentCountBefore = countFragments(datasetPath);
    assertThat(fragmentCountBefore).isGreaterThanOrEqualTo(5);

    List<Row> rows =
        collectRows(
            tEnv.executeSql(
                "CALL sys.compact(`table` => 'default.customers', target_rows_per_fragment => CAST(1000 AS BIGINT))"));

    assertThat(rows).hasSize(1);
    Row metrics = rows.get(0);
    long fragmentsAdded = (long) metrics.getField(0);
    long fragmentsRemoved = (long) metrics.getField(1);
    long filesAdded = (long) metrics.getField(2);
    long filesRemoved = (long) metrics.getField(3);

    // §6 step 9 acceptance: all four metric fields are populated with fixture-consistent values.
    assertThat(fragmentsRemoved).as("fragmentsRemoved").isGreaterThanOrEqualTo(5);
    assertThat(fragmentsAdded).as("fragmentsAdded").isGreaterThanOrEqualTo(1);
    assertThat(fragmentsRemoved)
        .as("fragmentsRemoved > fragmentsAdded")
        .isGreaterThan(fragmentsAdded);
    assertThat(filesRemoved).as("filesRemoved").isGreaterThan(0L);
    assertThat(filesAdded).as("filesAdded").isGreaterThan(0L);

    int fragmentCountAfter = countFragments(datasetPath);
    assertThat(fragmentCountAfter).isLessThan(fragmentCountBefore);
  }

  @Test
  void expireSnapshotsRetainLastLargerThanHistoryIsNoOp() throws Exception {
    // retain_last >= listVersions().size() must short-circuit to all-zero stats, skipping
    // cleanupWithPolicy entirely. Safe happy-path that exercises the retain_last branch end-to-end
    // without depending on physical-file-removal mechanics.
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "expire_noop");
    seedSmallFragments(resolveTablePath(tEnv, "expire_noop"), 1, 50);

    List<Row> rows =
        collectRows(
            tEnv.executeSql(
                "CALL sys.expire_snapshots(`table` => 'default.expire_noop', retain_last => 1000)"));

    assertThat(rows).hasSize(1);
    for (int i = 0; i < 6; i++) {
      assertThat((long) rows.get(0).getField(i))
          .as("expire_snapshots no-op stats field " + i)
          .isEqualTo(0L);
    }
  }

  @Test
  void expireSnapshotsRejectsMultipleSelectors() {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "expire_t");

    assertThat(
            collectSqlErrorMessage(
                () ->
                    tEnv.executeSql(
                            "CALL sys.expire_snapshots(`table` => 'default.expire_t', "
                                + "before_version => CAST(5 AS BIGINT), retain_last => 3)")
                        .await()))
        .isNotNull()
        .satisfiesAnyOf(
            msg -> assertThat(msg).contains("Exactly one of"),
            msg -> assertThat(msg).contains("mutually exclusive"));
  }

  @Test
  void expireSnapshotsRejectsNoSelectors() {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "expire_none");

    assertThat(
            collectSqlErrorMessage(
                () ->
                    tEnv.executeSql("CALL sys.expire_snapshots(`table` => 'default.expire_none')")
                        .await()))
        .isNotNull()
        .satisfies(
            msg -> {
              assertThat(msg).contains("Exactly one of");
              assertThat(msg).contains("none was provided");
            });
  }

  @Test
  void optimizeIndicesAcceptsNoIndexNamesAsNoOp() throws Exception {
    // §10.1: ARRAY<STRING> failed inside Flink 1.19.1's SqlProcedureCallConverter; locked-in shape
    // is `index_names_csv STRING?`. CSV parsing rules are unit-tested in
    // OptimizeIndicesProcedureTest
    // — this ITCase only pins the end-to-end no-op call shape.
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "opt_indexed");
    seedSmallFragments(resolveTablePath(tEnv, "opt_indexed"), 1, 50);

    tEnv.executeSql("CALL sys.optimize_indices(`table` => 'default.opt_indexed')").await();
  }
}
