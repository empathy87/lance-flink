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
import org.lance.index.Index;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.BTreeIndexParams;
import org.lance.index.scalar.BitmapIndexParams;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.index.scalar.ZoneMapIndexParams;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Locale;

/** Creates a single-column BTREE, BITMAP, or ZONEMAP index synchronously in the caller JVM. */
public class CreateIndexProcedure extends AbstractLanceProcedure {

  // TODO: Add vector and text index types once their parameters and Flink SQL surface are designed.
  private static final List<String> SUPPORTED_INDEX_TYPES = List.of("BTREE", "BITMAP", "ZONEMAP");

  public CreateIndexProcedure(LanceNamespaceCatalog catalog) {
    super(catalog);
  }

  @ProcedureHint(
      argument = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "column", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "index_type", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "index_name", type = @DataTypeHint("STRING"), isOptional = true),
        @ArgumentHint(name = "replace", type = @DataTypeHint("BOOLEAN"), isOptional = true)
      },
      output = @DataTypeHint("ROW<index_name STRING>"))
  public Row[] call(
      ProcedureContext context,
      String table,
      String column,
      String indexType,
      @Nullable String indexName,
      @Nullable Boolean replace) {
    LanceProcedureUtils.validateNonEmpty(table, "`table`");
    LanceProcedureUtils.validateNonEmpty(column, "column");
    LanceProcedureUtils.validateNonEmpty(indexType, "index_type");
    if (indexName != null) {
      LanceProcedureUtils.validateNonEmpty(indexName, "index_name");
    }

    // TODO: Expose scalar index tuning options.
    ScalarIndexSpec spec = resolveIndexSpec(indexType);
    IndexParams params = IndexParams.builder().setScalarIndexParams(spec.params()).build();
    // TODO: Add multi-column index creation.
    IndexOptions.Builder builder = IndexOptions.builder(List.of(column), spec.type(), params);
    if (indexName != null) {
      builder.withIndexName(indexName);
    }
    if (replace != null) {
      builder.replace(replace);
    }

    try (Dataset dataset = catalog.openTable(table)) {
      // TODO: Add distributed Flink index building.
      Index created = dataset.createIndex(builder.build());
      return LanceProcedureUtils.singleRowArray(created.name());
    }
  }

  private record ScalarIndexSpec(IndexType type, ScalarIndexParams params) {}

  private static ScalarIndexSpec resolveIndexSpec(String indexType) {
    String normalized = indexType.toUpperCase(Locale.ROOT);
    return switch (normalized) {
      case "BTREE" -> new ScalarIndexSpec(IndexType.BTREE, BTreeIndexParams.builder().build());
      case "BITMAP" -> new ScalarIndexSpec(IndexType.BITMAP, BitmapIndexParams.builder().build());
      case "ZONEMAP" ->
          new ScalarIndexSpec(IndexType.ZONEMAP, ZoneMapIndexParams.builder().build());
      default ->
          throw new ValidationException(
              "Unsupported index_type '"
                  + indexType
                  + "'. Supported in this release: "
                  + String.join(", ", SUPPORTED_INDEX_TYPES)
                  + ". For other index types, use the existing programmatic Java index-building"
                  + " API.");
    };
  }
}
