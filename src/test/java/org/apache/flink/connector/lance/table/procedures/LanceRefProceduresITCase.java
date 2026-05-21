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

import org.apache.flink.connector.lance.LanceDatasetOpener;

import org.lance.Dataset;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end SQL tests for the tag / branch / version ref procedures: {@code sys.create_tag},
 * {@code sys.delete_tag}, {@code sys.rollback_to_version}, {@code sys.rollback_to_tag}, {@code
 * sys.create_branch}, {@code sys.delete_branch}.
 */
class LanceRefProceduresITCase extends AbstractLanceProcedureITCase {

  @Test
  void createTagRecordsHeadVersion() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "tagged");
    String datasetPath = resolveTablePath(tEnv, "tagged");
    seedSmallFragments(datasetPath, 1, 50);
    long versionBefore;
    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      versionBefore = dataset.latestVersion();
    }

    List<Row> rows =
        collectRows(
            tEnv.executeSql("CALL sys.create_tag(`table` => 'default.tagged', tag => 'v1')"));

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getField(0)).isEqualTo("v1");
    assertThat(rows.get(0).getField(1)).isEqualTo(versionBefore);

    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      assertThat(dataset.tags().getVersion("v1")).isEqualTo(versionBefore);
    }
  }

  @Test
  void deleteTagRemovesTag() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "tagged");
    seedSmallFragments(resolveTablePath(tEnv, "tagged"), 1, 50);

    tEnv.executeSql("CALL sys.create_tag(`table` => 'default.tagged', tag => 'tmp')").await();
    tEnv.executeSql("CALL sys.delete_tag(`table` => 'default.tagged', tag => 'tmp')").await();

    try (Dataset dataset = LanceDatasetOpener.open(allocator, resolveTablePath(tEnv, "tagged"))) {
      assertThat(dataset.tags().list()).noneMatch(t -> "tmp".equals(t.getName()));
    }
  }

  @Test
  void rollbackToVersionMovesHead() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "rolled");
    String datasetPath = resolveTablePath(tEnv, "rolled");
    seedSmallFragments(datasetPath, 1, 50); // version A
    long versionA;
    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      versionA = dataset.latestVersion();
    }
    seedSmallFragments(datasetPath, 1, 50); // version B
    long versionB;
    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      versionB = dataset.latestVersion();
    }
    assertThat(versionB).isGreaterThan(versionA);

    List<Row> rows =
        collectRows(
            tEnv.executeSql(
                "CALL sys.rollback_to_version(`table` => 'default.rolled', version => CAST("
                    + versionA
                    + " AS BIGINT))"));

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getField(0)).isEqualTo(versionA);
    long newHead = (long) rows.get(0).getField(1);
    // Restore commits a new version pointing back to versionA; HEAD must move forward (not back to
    // versionA itself) and the visible row count must match versionA's row count.
    assertThat(newHead).isGreaterThan(versionB);
    try (Dataset reopened = LanceDatasetOpener.open(allocator, datasetPath)) {
      assertThat(reopened.countRows()).isEqualTo(50L);
    }
  }

  @Test
  void rollbackToTagRestoresHead() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "rolled_tag");
    String datasetPath = resolveTablePath(tEnv, "rolled_tag");
    seedSmallFragments(datasetPath, 1, 50); // version A
    long versionA;
    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      versionA = dataset.latestVersion();
    }
    tEnv.executeSql(
            "CALL sys.create_tag(`table` => 'default.rolled_tag', tag => 'v1', version => CAST("
                + versionA
                + " AS BIGINT))")
        .await();
    seedSmallFragments(datasetPath, 1, 50); // version B
    long versionB;
    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      versionB = dataset.latestVersion();
    }
    assertThat(versionB).isGreaterThan(versionA);

    List<Row> rows =
        collectRows(
            tEnv.executeSql(
                "CALL sys.rollback_to_tag(`table` => 'default.rolled_tag', tag => 'v1')"));
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getField(0)).isEqualTo("v1");
    assertThat(rows.get(0).getField(1)).isEqualTo(versionA);
    long newHead = (long) rows.get(0).getField(2);
    assertThat(newHead).isGreaterThan(versionB);
    try (Dataset reopened = LanceDatasetOpener.open(allocator, datasetPath)) {
      assertThat(reopened.countRows()).isEqualTo(50L);
    }
  }

  @Test
  void createBranchCapturesHeadAsParentVersion() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "branched");
    String datasetPath = resolveTablePath(tEnv, "branched");
    seedSmallFragments(datasetPath, 1, 50);
    long headVersion;
    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      headVersion = dataset.latestVersion();
    }

    List<Row> rows =
        collectRows(
            tEnv.executeSql(
                "CALL sys.create_branch(`table` => 'default.branched', branch => 'feature')"));

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getField(0)).isEqualTo("feature");
    assertThat(rows.get(0).getField(1)).isEqualTo(headVersion);

    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      assertThat(dataset.branches().list()).anyMatch(b -> "feature".equals(b.getName()));
    }
  }

  @Test
  void deleteBranchRemovesBranch() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "branched_del");
    String datasetPath = resolveTablePath(tEnv, "branched_del");
    seedSmallFragments(datasetPath, 1, 50);

    tEnv.executeSql("CALL sys.create_branch(`table` => 'default.branched_del', branch => 'tmp')")
        .await();
    tEnv.executeSql("CALL sys.delete_branch(`table` => 'default.branched_del', branch => 'tmp')")
        .await();

    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      assertThat(dataset.branches().list()).noneMatch(b -> "tmp".equals(b.getName()));
    }
  }
}
