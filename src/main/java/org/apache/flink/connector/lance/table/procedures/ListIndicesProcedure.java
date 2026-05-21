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

import org.apache.flink.connector.lance.table.LanceNamespaceCatalog;

import org.lance.Dataset;
import org.lance.index.IndexDescription;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Objects;

/** Lists Lance indexes for a table. */
public class ListIndicesProcedure extends AbstractLanceProcedure {

  public ListIndicesProcedure(LanceNamespaceCatalog catalog) {
    super(catalog);
  }

  @ProcedureHint(
      argument = {@ArgumentHint(name = "table", type = @DataTypeHint("STRING"))},
      output =
          @DataTypeHint(
              "ROW<name STRING, field_ids ARRAY<INT>, index_type STRING, rows_indexed BIGINT, details_json STRING>"))
  public Row[] call(ProcedureContext context, String table) {
    LanceProcedureUtils.validateNonEmpty(table, "`table`");
    try (Dataset dataset = catalog.openTable(table)) {
      List<IndexDescription> descriptions = dataset.describeIndices();
      Row[] result = new Row[descriptions.size()];
      for (int i = 0; i < descriptions.size(); i++) {
        result[i] = toRow(descriptions.get(i));
      }
      return result;
    }
  }

  private static Row toRow(IndexDescription desc) {
    List<Integer> fieldIds = Objects.requireNonNullElse(desc.getFieldIds(), List.of());
    return Row.of(
        desc.getName(),
        fieldIds.toArray(Integer[]::new),
        desc.getIndexType(),
        desc.getRowsIndexed(),
        desc.getDetailsJson());
  }
}
