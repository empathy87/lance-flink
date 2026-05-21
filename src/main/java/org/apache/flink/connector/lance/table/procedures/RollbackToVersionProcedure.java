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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

/** Rolls table HEAD back to a specific version. Destructive. */
public class RollbackToVersionProcedure extends AbstractLanceProcedure {

  public RollbackToVersionProcedure(LanceNamespaceCatalog catalog) {
    super(catalog);
  }

  @ProcedureHint(
      argument = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "version", type = @DataTypeHint("BIGINT"))
      },
      output = @DataTypeHint("ROW<rolled_back_to_version BIGINT, new_head_version BIGINT>"))
  public Row[] call(ProcedureContext context, String table, long version) {
    LanceProcedureUtils.validateNonEmpty(table, "`table`");
    LanceProcedureUtils.validatePositive(version, "version");

    try (Dataset dataset = catalog.openTable(table);
        Dataset checkedOut = dataset.checkoutVersion(version)) {
      checkedOut.restore();
    }
    try (Dataset dataset = catalog.openTable(table)) {
      return LanceProcedureUtils.singleRowArray(version, dataset.latestVersion());
    }
  }
}
