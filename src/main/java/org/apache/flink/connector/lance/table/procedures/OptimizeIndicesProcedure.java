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
import org.lance.index.OptimizeOptions;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Optimizes existing Lance indexes. */
public class OptimizeIndicesProcedure extends AbstractLanceProcedure {

  public OptimizeIndicesProcedure(LanceNamespaceCatalog catalog) {
    super(catalog);
  }

  @ProcedureHint(
      argument = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "index_names_csv", type = @DataTypeHint("STRING"), isOptional = true),
        @ArgumentHint(
            name = "num_indices_to_merge",
            type = @DataTypeHint("INT"),
            isOptional = true),
        @ArgumentHint(name = "retrain", type = @DataTypeHint("BOOLEAN"), isOptional = true)
      },
      output = @DataTypeHint("ROW<table STRING, message STRING>"))
  public Row[] call(
      ProcedureContext context,
      String table,
      @Nullable String indexNamesCsv,
      @Nullable Integer numIndicesToMerge,
      @Nullable Boolean retrain) {
    LanceProcedureUtils.validateNonEmpty(table, "`table`");

    OptimizeOptions.Builder builder = OptimizeOptions.builder();
    if (indexNamesCsv != null) {
      builder.indexNames(parseCsv(indexNamesCsv));
    }
    if (numIndicesToMerge != null) {
      LanceProcedureUtils.validatePositive(numIndicesToMerge, "num_indices_to_merge");
      builder.numIndicesToMerge(numIndicesToMerge);
    }
    if (retrain != null) {
      builder.retrain(retrain);
    }

    try (Dataset dataset = catalog.openTable(table)) {
      // TODO: Add distributed Flink index optimization.
      dataset.optimizeIndices(builder.build());
      return LanceProcedureUtils.singleRowArray(table, "Indices optimized");
    }
  }

  static List<String> parseCsv(String csv) {
    // Keep trailing empty entries so malformed CSV is rejected.
    String[] parts = csv.split(",", -1);
    List<String> out = new ArrayList<>(parts.length);
    for (String part : parts) {
      String trimmed = part.trim();
      LanceProcedureUtils.validateNonEmpty(trimmed, "index_names_csv entry");
      out.add(trimmed);
    }
    return out;
  }
}
