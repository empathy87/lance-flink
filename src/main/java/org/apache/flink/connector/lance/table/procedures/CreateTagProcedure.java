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

import javax.annotation.Nullable;

/** Creates a Lance tag for a specific version or current HEAD. */
public class CreateTagProcedure extends AbstractLanceProcedure {

  public CreateTagProcedure(LanceNamespaceCatalog catalog) {
    super(catalog);
  }

  @ProcedureHint(
      argument = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "tag", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "version", type = @DataTypeHint("BIGINT"), isOptional = true)
      },
      output = @DataTypeHint("ROW<tag STRING, version BIGINT>"))
  public Row[] call(ProcedureContext context, String table, String tag, @Nullable Long version) {
    LanceProcedureUtils.validateNonEmpty(table, "`table`");
    LanceProcedureUtils.validateNonEmpty(tag, "tag");
    if (version != null) {
      LanceProcedureUtils.validatePositive(version, "version");
    }
    try (Dataset dataset = catalog.openTable(table)) {
      long effectiveVersion = version != null ? version : dataset.latestVersion();
      dataset.tags().create(tag, effectiveVersion);
      return LanceProcedureUtils.singleRowArray(tag, effectiveVersion);
    }
  }
}
