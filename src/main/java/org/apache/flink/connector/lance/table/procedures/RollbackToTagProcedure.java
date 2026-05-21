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

/** Rolls table HEAD back to the version referenced by a tag. Destructive. */
public class RollbackToTagProcedure extends AbstractLanceProcedure {

  public RollbackToTagProcedure(LanceNamespaceCatalog catalog) {
    super(catalog);
  }

  @ProcedureHint(
      argument = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "tag", type = @DataTypeHint("STRING"))
      },
      output =
          @DataTypeHint("ROW<tag STRING, rolled_back_to_version BIGINT, new_head_version BIGINT>"))
  public Row[] call(ProcedureContext context, String table, String tag) {
    LanceProcedureUtils.validateNonEmpty(table, "`table`");
    LanceProcedureUtils.validateNonEmpty(tag, "tag");

    long resolvedVersion;
    try (Dataset dataset = catalog.openTable(table)) {
      resolvedVersion = dataset.tags().getVersion(tag);
      try (Dataset checkedOut = dataset.checkoutVersion(resolvedVersion)) {
        checkedOut.restore();
      }
    }
    try (Dataset dataset = catalog.openTable(table)) {
      return LanceProcedureUtils.singleRowArray(tag, resolvedVersion, dataset.latestVersion());
    }
  }
}
