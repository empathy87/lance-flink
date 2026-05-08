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
package org.apache.flink.connector.lance.source;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ReadOptions;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/** Split reader that scans Lance fragments and resumes from recordsToSkip on restore. */
class LanceSourceSplitReader implements SplitReader<RowData, LanceSourceSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceSourceSplitReader.class);

  private final LanceOptions options;
  private final RowType configuredRowType;
  private final String[] selectedColumns;
  private final String filter;
  private final Long limit;

  private final Deque<LanceSourceSplit> pendingSplits = new ArrayDeque<>();
  private final BufferAllocator allocator;
  private Dataset dataset;
  private RowDataConverter converter;

  private LanceSourceSplit currentSplit;
  private LanceScanner currentScanner;
  private ArrowReader currentReader;
  private long consumedFromSplit;
  private long openedDatasetVersion = -1L;
  // Rows emitted by this reader; used to shrink per-fragment scan limits after limit pushdown.
  // TODO: Add tests for LIMIT pushdown with parallelism > 1 and restore.
  private long emitted;

  LanceSourceSplitReader(
      LanceOptions options,
      RowType rowType,
      @Nullable String[] selectedColumns,
      @Nullable String filter,
      @Nullable Long limit) {
    this.options = Objects.requireNonNull(options, "options");
    this.configuredRowType = Objects.requireNonNull(rowType, "rowType");
    this.selectedColumns =
        selectedColumns == null ? null : Arrays.copyOf(selectedColumns, selectedColumns.length);
    this.filter = filter == null || filter.isBlank() ? null : filter;
    this.limit = limit;
    this.allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Override
  public RecordsWithSplitIds<RowData> fetch() throws IOException {
    while (true) {
      if (currentSplit == null) {
        currentSplit = pendingSplits.pollFirst();
        if (currentSplit == null) {
          return LanceRecordsWithSplitIds.empty();
        }
        if (limit != null && emitted >= limit) {
          // Budget already satisfied — drain remaining splits without opening any scanner.
          return finishCurrentSplit();
        }
        ensureDatasetOpen(currentSplit.datasetVersion());
        openScanner(currentSplit);
      }

      boolean hasBatch;
      try {
        hasBatch = currentReader.loadNextBatch();
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException("Failed to load next batch from Lance scanner", e);
      }
      if (!hasBatch) {
        return finishCurrentSplit();
      }

      VectorSchemaRoot root = currentReader.getVectorSchemaRoot();
      List<RowData> rows = converter.toRowDataList(root);

      long initialSkip = currentSplit.recordsToSkip();
      if (consumedFromSplit < initialSkip) {
        long skipNeeded = initialSkip - consumedFromSplit;
        long skipFromBatch = Math.min(rows.size(), skipNeeded);
        consumedFromSplit += skipFromBatch;
        rows = rows.subList((int) skipFromBatch, rows.size());
      }

      if (rows.isEmpty()) {
        continue;
      }

      if (limit != null) {
        long remaining = limit - emitted;
        if (remaining <= 0) {
          return finishCurrentSplit();
        }
        if (rows.size() > remaining) {
          rows = rows.subList(0, (int) remaining);
        }
      }

      if (rows.isEmpty()) {
        continue;
      }

      consumedFromSplit += rows.size();
      emitted += rows.size();
      return LanceRecordsWithSplitIds.forRecords(currentSplit.splitId(), rows);
    }
  }

  @Override
  public void handleSplitsChanges(SplitsChange<LanceSourceSplit> splitsChange) {
    if (!(splitsChange instanceof SplitsAddition<LanceSourceSplit> addition)) {
      throw new UnsupportedOperationException(
          "Unsupported splits change: " + splitsChange.getClass().getSimpleName());
    }
    pendingSplits.addAll(addition.splits());
  }

  @Override
  public void wakeUp() {
    // Synchronous fetch — nothing to interrupt.
  }

  @Override
  public void close() throws Exception {
    closeCurrentSplit();
    pendingSplits.clear();
    try {
      if (dataset != null) {
        dataset.close();
        dataset = null;
        openedDatasetVersion = -1L;
      }
    } catch (Exception e) {
      LOG.warn("Failed to close Lance dataset", e);
    }
    try {
      if (allocator != null) {
        allocator.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close memory allocator", e);
    }
  }

  private void ensureDatasetOpen(long datasetVersion) throws IOException {
    if (dataset != null) {
      if (openedDatasetVersion != datasetVersion) {
        throw new IOException(
            "Lance reader cannot mix dataset versions: opened "
                + openedDatasetVersion
                + ", requested "
                + datasetVersion);
      }
      return;
    }
    String path = options.getPath();
    if (path == null || path.isBlank()) {
      throw new IOException("Lance dataset path cannot be empty");
    }
    try {
      ReadOptions readOptions = new ReadOptions.Builder().setVersion(datasetVersion).build();
      dataset = Dataset.open().readOptions(readOptions).allocator(allocator).uri(path).build();
      openedDatasetVersion = datasetVersion;
    } catch (Exception e) {
      throw new IOException("Cannot open Lance dataset: " + path, e);
    }
    RowType rowType = configuredRowType;
    if (rowType == null) {
      Schema arrowSchema = dataset.getSchema();
      rowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
    }
    converter = new RowDataConverter(rowType);
  }

  private void openScanner(LanceSourceSplit split) throws IOException {
    // TODO: Support zero-column projection for queries like SELECT COUNT(*).
    ScanOptions.Builder builder = new ScanOptions.Builder();
    builder.batchSize(options.getReadBatchSize());

    if (selectedColumns != null) {
      builder.columns(Arrays.asList(selectedColumns));
    }
    if (filter != null) {
      builder.filter(filter);
    }
    if (limit != null) {
      // Read only this reader's remaining limit budget plus rows skipped during restore.
      long remaining = limit - emitted;
      builder.limit(addWithoutOverflow(remaining, split.recordsToSkip()));
    }

    Fragment fragment = findFragment(split.fragmentId());
    // TODO: Ensure stable fragment scan order for recordsToSkip restore.
    try {
      currentScanner = fragment.newScan(builder.build());
      currentReader = currentScanner.scanBatches();
    } catch (Exception e) {
      throw new IOException("Cannot open Lance fragment scanner: " + split.fragmentId(), e);
    }
    consumedFromSplit = 0L;
  }

  private static long addWithoutOverflow(long left, long right) {
    if (Long.MAX_VALUE - left < right) {
      return Long.MAX_VALUE;
    }
    return left + right;
  }

  private Fragment findFragment(int fragmentId) throws IOException {
    for (Fragment frag : dataset.getFragments()) {
      if (frag.getId() == fragmentId) {
        return frag;
      }
    }
    throw new IOException("Fragment id not found in Lance dataset: " + fragmentId);
  }

  private RecordsWithSplitIds<RowData> finishCurrentSplit() {
    String finishedId = currentSplit.splitId();
    closeCurrentSplit();
    return LanceRecordsWithSplitIds.finishedSplit(finishedId);
  }

  private void closeCurrentSplit() {
    try {
      if (currentReader != null) {
        currentReader.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close Arrow reader", e);
    } finally {
      currentReader = null;
    }
    try {
      if (currentScanner != null) {
        currentScanner.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close Lance scanner", e);
    } finally {
      currentScanner = null;
    }
    currentSplit = null;
    consumedFromSplit = 0L;
  }
}
