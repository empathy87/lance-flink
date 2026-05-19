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
package org.apache.flink.connector.lance.lookup;

import org.apache.flink.connector.lance.LanceDatasetOpener;
import org.apache.flink.connector.lance.converter.RowDataConverter;

import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Performs synchronous Lance lookups against the dataset version opened by this function. */
public class LanceLookupFunction extends LookupFunction {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceLookupFunction.class);

  private final String path;
  private final List<String> keyColumns;
  private final List<LogicalType> keyTypes;
  private final List<String> projectedColumns;
  private final RowType producedRowType;
  private final int readBatchSize;
  private final boolean allowFullScan;
  @Nullable private final String pushedFilter;

  private transient BufferAllocator allocator;
  private transient Dataset dataset;
  private transient RowDataConverter converter;
  private transient LanceLookupKeyFilterBuilder filterBuilder;

  public LanceLookupFunction(
      String path,
      List<String> keyColumns,
      List<LogicalType> keyTypes,
      List<String> projectedColumns,
      RowType producedRowType,
      int readBatchSize,
      boolean allowFullScan,
      @Nullable String pushedFilter) {
    this.path = Objects.requireNonNull(path, "path");
    this.keyColumns = List.copyOf(Objects.requireNonNull(keyColumns, "keyColumns"));
    this.keyTypes = List.copyOf(Objects.requireNonNull(keyTypes, "keyTypes"));
    this.projectedColumns =
        List.copyOf(Objects.requireNonNull(projectedColumns, "projectedColumns"));
    this.producedRowType = Objects.requireNonNull(producedRowType, "producedRowType");
    this.readBatchSize = readBatchSize;
    this.allowFullScan = allowFullScan;
    this.pushedFilter = pushedFilter == null || pushedFilter.isBlank() ? null : pushedFilter;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    boolean opened = false;
    try {
      // TODO: Add an opt-in refresh mode if lookup joins must observe newer Lance HEAD versions.
      this.dataset = LanceDatasetOpener.open(allocator, path);
      if (!allowFullScan) {
        validateScalarIndexes();
      }
      this.filterBuilder = new LanceLookupKeyFilterBuilder(keyColumns, keyTypes);
      this.converter = new RowDataConverter(producedRowType);
      opened = true;
    } finally {
      if (!opened) {
        closeQuietly();
      }
    }
  }

  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    String keyFilter = filterBuilder.build(keyRow);
    if (keyFilter == null) {
      // Any null key value short-circuits — SQL = on NULL never matches.
      return Collections.emptyList();
    }
    String filter = combineFilters(keyFilter, pushedFilter);

    ScanOptions.Builder builder = new ScanOptions.Builder().batchSize(readBatchSize).filter(filter);
    if (!projectedColumns.isEmpty()) {
      builder.columns(projectedColumns);
    }
    ScanOptions options = builder.build();

    List<RowData> results = new ArrayList<>();
    try (LanceScanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        results.addAll(converter.toRowDataList(root));
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Lance lookup failed for filter: " + filter, e);
    }
    return results;
  }

  @Override
  public void close() throws Exception {
    closeQuietly();
  }

  List<String> keyColumns() {
    return keyColumns;
  }

  List<String> projectedColumns() {
    return projectedColumns;
  }

  RowType producedRowType() {
    return producedRowType;
  }

  @Nullable
  String pushedFilter() {
    return pushedFilter;
  }

  boolean allowFullScan() {
    return allowFullScan;
  }

  /** Parenthesizes both sides before ANDing to preserve SQL precedence. */
  static String combineFilters(String keyFilter, @Nullable String pushedFilter) {
    if (pushedFilter == null) {
      return keyFilter;
    }
    return "(" + keyFilter + ") AND (" + pushedFilter + ")";
  }

  private void validateScalarIndexes() {
    // TODO: Validate composite-key index coverage if Lance has multi-column exact-match indexes.
    // TODO: Surface Lance index recommendations once index type capabilities are stable in the API.
    List<String> missing = new ArrayList<>();
    for (String column : keyColumns) {
      IndexCriteria criteria =
          new IndexCriteria.Builder().forColumn(column).mustSupportExactEquality(true).build();
      List<IndexDescription> indices = dataset.describeIndices(criteria);
      if (indices.isEmpty()) {
        missing.add(column);
      }
    }
    if (!missing.isEmpty()) {
      throw new IllegalStateException(
          "Lance lookup join requires a scalar index that supports exact equality on every"
              + " lookup key column. Missing exact-equality scalar index on: "
              + missing
              + ". Create a Lance scalar index that supports exact equality on those columns via"
              + " the Lance API, or pass the per-query hint"
              + " /*+ OPTIONS('lookup.allow-full-scan' = 'true') */ on the dimension-table side"
              + " to permit a full Lance scan per probe (slow; intended for small dimension"
              + " tables, not for production streaming joins).");
    }
  }

  private void closeQuietly() {
    try {
      if (dataset != null) {
        dataset.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close Lance dataset", e);
    } finally {
      dataset = null;
    }
    try {
      if (allocator != null) {
        allocator.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close lookup allocator", e);
    } finally {
      allocator = null;
    }
  }
}
