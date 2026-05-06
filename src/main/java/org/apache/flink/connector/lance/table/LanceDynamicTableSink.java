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
package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.sink.LanceSinkV2;
import org.apache.flink.connector.lance.sink.LanceUpsertSinkV2;
import org.apache.flink.connector.lance.sink.RowLevelOperation;

import org.lance.Dataset;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.sink.abilities.SupportsTruncate;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.RootAllocator;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Dynamic table sink for Lance datasets. */
public class LanceDynamicTableSink
    implements DynamicTableSink,
        SupportsOverwrite,
        SupportsTruncate,
        SupportsRowLevelUpdate,
        SupportsRowLevelDelete {

  private final LanceOptions options;
  private final DataType physicalDataType;
  private final List<String> primaryKeys;
  private boolean overwrite;
  private RowLevelOperation rowLevelOperation;

  public LanceDynamicTableSink(LanceOptions options, DataType physicalDataType) {
    this(options, physicalDataType, List.of());
  }

  public LanceDynamicTableSink(
      LanceOptions options, DataType physicalDataType, List<String> primaryKeys) {
    this(options, physicalDataType, primaryKeys, false, RowLevelOperation.NONE);
  }

  public LanceDynamicTableSink(
      LanceOptions options,
      DataType physicalDataType,
      List<String> primaryKeys,
      boolean overwrite) {
    this(options, physicalDataType, primaryKeys, overwrite, RowLevelOperation.NONE);
  }

  private LanceDynamicTableSink(
      LanceOptions options,
      DataType physicalDataType,
      List<String> primaryKeys,
      boolean overwrite,
      RowLevelOperation rowLevelOperation) {
    this.options = options;
    this.physicalDataType = physicalDataType;
    this.primaryKeys = List.copyOf(primaryKeys);
    this.overwrite = overwrite;
    this.rowLevelOperation = rowLevelOperation;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    if (rowLevelOperation == RowLevelOperation.DELETE) {
      return ChangelogMode.newBuilder().addContainedKind(RowKind.DELETE).build();
    }
    if (rowLevelOperation == RowLevelOperation.UPDATE) {
      return ChangelogMode.newBuilder().addContainedKind(RowKind.UPDATE_AFTER).build();
    }
    if (overwrite || primaryKeys.isEmpty() || requestedMode.containsOnly(RowKind.INSERT)) {
      return ChangelogMode.insertOnly();
    }
    return ChangelogMode.upsert();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    if (overwrite && !context.isBounded()) {
      throw new UnsupportedOperationException("Lance doesn't support streaming INSERT OVERWRITE.");
    }
    RowType rowType = (RowType) physicalDataType.getLogicalType();
    if (primaryKeys.isEmpty()) {
      return SinkV2Provider.of(new LanceSinkV2(options, rowType, overwrite));
    }
    return SinkV2Provider.of(
        new LanceUpsertSinkV2(options, rowType, primaryKeys, overwrite, rowLevelOperation));
  }

  @Override
  public void applyOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  @Override
  public void executeTruncation() {
    String datasetPath = options.getPath();

    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset dataset = Dataset.open().allocator(allocator).uri(datasetPath).build()) {
      dataset.truncateTable();
    } catch (Exception e) {
      throw new RuntimeException("Failed to truncate Lance dataset at " + datasetPath, e);
    }
  }

  @Override
  public RowLevelUpdateInfo applyRowLevelUpdate(
      List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context) {
    requirePrimaryKeyForRowLevel("UPDATE");
    requireNoOverwriteForRowLevel("UPDATE");
    rejectUpdateOnPrimaryKey(updatedColumns);
    this.rowLevelOperation = RowLevelOperation.UPDATE;
    return new RowLevelUpdateInfo() {};
  }

  @Override
  public RowLevelDeleteInfo applyRowLevelDelete(@Nullable RowLevelModificationScanContext context) {
    requirePrimaryKeyForRowLevel("DELETE");
    requireNoOverwriteForRowLevel("DELETE");
    this.rowLevelOperation = RowLevelOperation.DELETE;
    // TODO: Request only PK columns once Lance supports PK-only row-level deletes.
    return new RowLevelDeleteInfo() {};
  }

  private void requirePrimaryKeyForRowLevel(String operation) {
    if (primaryKeys.isEmpty()) {
      throw new UnsupportedOperationException(
          "Lance row-level "
              + operation
              + " requires a PRIMARY KEY on the table; declare one in the DDL or use INSERT"
              + " OVERWRITE / TRUNCATE for table-level rewrites.");
    }
  }

  private void rejectUpdateOnPrimaryKey(List<Column> updatedColumns) {
    Set<String> pkSet = new HashSet<>(primaryKeys);
    List<String> conflicting =
        updatedColumns.stream().map(Column::getName).filter(pkSet::contains).toList();
    if (!conflicting.isEmpty()) {
      throw new UnsupportedOperationException(
          "Updating primary key columns is not supported by the Lance sink (primary keys "
              + primaryKeys
              + ", updated columns "
              + conflicting
              + ").");
    }
  }

  private void requireNoOverwriteForRowLevel(String operation) {
    if (overwrite) {
      throw new UnsupportedOperationException(
          "Lance row-level " + operation + " cannot be combined with INSERT OVERWRITE.");
    }
  }

  @Override
  public DynamicTableSink copy() {
    return new LanceDynamicTableSink(
        options, physicalDataType, primaryKeys, overwrite, rowLevelOperation);
  }

  @Override
  public String asSummaryString() {
    return "Lance Table Sink";
  }

  public LanceOptions getOptions() {
    return options;
  }

  public DataType getPhysicalDataType() {
    return physicalDataType;
  }

  public boolean isOverwrite() {
    return overwrite;
  }
}
