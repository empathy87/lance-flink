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
 * End-to-end SQL tests for the index procedures: {@code sys.create_index}, {@code
 * sys.list_indices}, {@code sys.drop_index}. Shares the BTREE-on-{@code id} fixture pattern across
 * cases.
 */
class LanceIndexProceduresITCase extends AbstractLanceProcedureITCase {

  @Test
  void listIndicesReturnsEmptyOnFreshTable() {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "indexless");

    List<Row> rows =
        collectRows(tEnv.executeSql("CALL sys.list_indices(`table` => 'default.indexless')"));

    assertThat(rows).isEmpty();
  }

  @Test
  void listIndicesReturnsCreatedIndexMetadata() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "indexed");
    seedSmallFragments(resolveTablePath(tEnv, "indexed"), 1, 50);

    List<Row> created =
        collectRows(
            tEnv.executeSql(
                "CALL sys.create_index("
                    + "`table` => 'default.indexed', "
                    + "`column` => 'id',"
                    + "index_type => 'BTREE')"));
    assertThat(created).hasSize(1);
    String createdName = (String) created.get(0).getField(0);
    assertThat(createdName).as("created index_name").isNotBlank();

    List<Row> listed =
        collectRows(tEnv.executeSql("CALL sys.list_indices(`table` => 'default.indexed')"));

    assertThat(listed).hasSize(1);
    Row entry = listed.get(0);
    assertThat((String) entry.getField(0)).as("list_indices name").isEqualTo(createdName);
    Integer[] fieldIds = (Integer[]) entry.getField(1);
    assertThat(fieldIds).as("list_indices field_ids").isNotNull().hasSize(1);
    assertThat((String) entry.getField(2)).as("list_indices index_type").isNotBlank();
    assertThat((long) entry.getField(3)).as("list_indices rows_indexed").isEqualTo(50L);
    assertThat((String) entry.getField(4)).as("list_indices details_json").isNotNull();
  }

  @Test
  void createIndexAcceptsExplicitIndexName() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "named_idx");
    seedSmallFragments(resolveTablePath(tEnv, "named_idx"), 1, 50);

    List<Row> created =
        collectRows(
            tEnv.executeSql(
                "CALL sys.create_index("
                    + "`table` => 'default.named_idx', "
                    + "`column` => 'id',"
                    + "index_type => 'BTREE', "
                    + "index_name => 'idx_explicit')"));
    assertThat(created).hasSize(1);
    assertThat(created.get(0).getField(0)).isEqualTo("idx_explicit");

    List<Row> listed =
        collectRows(tEnv.executeSql("CALL sys.list_indices(`table` => 'default.named_idx')"));
    assertThat(listed).hasSize(1);
    assertThat(listed.get(0).getField(0)).isEqualTo("idx_explicit");
  }

  @Test
  void dropIndexRemovesScalarIndex() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "drop_idx");
    seedSmallFragments(resolveTablePath(tEnv, "drop_idx"), 1, 50);

    List<Row> created =
        collectRows(
            tEnv.executeSql(
                "CALL sys.create_index("
                    + "`table` => 'default.drop_idx', "
                    + "`column` => 'id',"
                    + "index_type => 'BTREE')"));
    assertThat(created).hasSize(1);
    String createdName = (String) created.get(0).getField(0);

    List<Row> after =
        collectRows(tEnv.executeSql("CALL sys.list_indices(`table` => 'default.drop_idx')"));
    assertThat(after).hasSize(1);
    assertThat(after.get(0).getField(0)).isEqualTo(createdName);

    List<Row> dropped =
        collectRows(
            tEnv.executeSql(
                "CALL sys.drop_index(`table` => 'default.drop_idx', index_name => '"
                    + createdName
                    + "')"));
    assertThat(dropped).hasSize(1);
    assertThat(dropped.get(0).getField(0)).isEqualTo(createdName);

    List<Row> remaining =
        collectRows(tEnv.executeSql("CALL sys.list_indices(`table` => 'default.drop_idx')"));
    assertThat(remaining).isEmpty();
  }

  @Test
  void createIndexRejectsUnsupportedType() throws Exception {
    TableEnvironment tEnv = newTableEnv();
    createCatalogAndTable(tEnv, "indexed_bad");
    seedSmallFragments(resolveTablePath(tEnv, "indexed_bad"), 1, 50);

    assertThat(
            collectSqlErrorMessage(
                () ->
                    tEnv.executeSql(
                            "CALL sys.create_index("
                                + "`table` => 'default.indexed_bad', "
                                + "`column` => 'id',"
                                + "index_type => 'IVF_PQ')")
                        .await()))
        .isNotNull()
        .satisfiesAnyOf(
            msg -> assertThat(msg).contains("Unsupported index_type"),
            msg -> assertThat(msg).contains("IVF_PQ"));
  }
}
