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
package org.apache.flink.connector.lance.sink;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;

import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;

import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Writes buffered RowData into uncommitted Lance fragments. */
public class LanceAppendWriter
    implements CommittingSinkWriter<RowData, LanceAppendCommittable>,
        StatefulSinkWriter<RowData, LanceWriterState> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceAppendWriter.class);

  private final LanceOptions options;
  private final RowType rowType;
  private final int subtaskId;

  private final BufferAllocator allocator;
  private final RowDataConverter converter;
  private final Schema arrowSchema;
  private final List<RowData> buffer;
  private final List<FragmentMetadata> pendingFragments;

  private long committableCounter;
  private long totalRows;

  public LanceAppendWriter(
      LanceOptions options,
      RowType rowType,
      int subtaskId,
      Collection<LanceWriterState> recoveredStates) {
    if (options.getPath() == null || options.getPath().isEmpty()) {
      throw new IllegalArgumentException("Lance dataset path cannot be empty");
    }
    this.options = options;
    this.rowType = rowType;
    this.subtaskId = subtaskId;
    // TODO: Use bounded task-scoped Arrow allocator.
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.converter = new RowDataConverter(rowType);
    this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
    this.buffer = new ArrayList<>(options.getWriteBatchSize());
    this.pendingFragments = new ArrayList<>();
    for (LanceWriterState state : recoveredStates) {
      // TODO: Validate recovered fragments against the current schema.
      pendingFragments.addAll(state.pendingFragments());
    }
    if (!pendingFragments.isEmpty()) {
      LOG.info(
          "LanceAppendWriter[{}] restored with {} pending fragments",
          subtaskId,
          pendingFragments.size());
    }
  }

  @Override
  public void write(RowData element, Context context) throws IOException {
    // TODO: Add byte-size based flush threshold.
    buffer.add(RowDataMaterializer.materialize(element, rowType));
    if (buffer.size() >= options.getWriteBatchSize()) {
      flushBufferToFragment();
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    flushBufferToFragment();
  }

  @Override
  public Collection<LanceAppendCommittable> prepareCommit() throws IOException {
    flushBufferToFragment();
    if (pendingFragments.isEmpty()) {
      return List.of();
    }
    // TODO: Use real Flink checkpoint id for committable idempotency.
    LanceAppendCommittable committable =
        new LanceAppendCommittable(committableCounter++, subtaskId, List.copyOf(pendingFragments));
    LOG.debug(
        "LanceAppendWriter[{}] prepareCommit emitting {} fragments",
        subtaskId,
        pendingFragments.size());
    pendingFragments.clear();
    return List.of(committable);
  }

  @Override
  public List<LanceWriterState> snapshotState(long checkpointId) {
    return List.of(new LanceWriterState(List.copyOf(pendingFragments)));
  }

  @Override
  public void close() throws Exception {
    LOG.info(
        "LanceAppendWriter[{}] closing, totalRows={}, pendingFragments={}",
        subtaskId,
        totalRows,
        pendingFragments.size());
    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.warn("Failed to close allocator on writer[{}]", subtaskId, e);
      }
    }
  }

  private void flushBufferToFragment() throws IOException {
    if (buffer.isEmpty()) {
      return;
    }
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      converter.toVectorSchemaRoot(buffer, root);
      WriteParams params =
          new WriteParams.Builder().withMaxRowsPerFile(options.getWriteMaxRowsPerFile()).build();
      // TODO: Add bounded retries for transient storage errors.
      List<FragmentMetadata> fragments =
          Fragment.write()
              .datasetUri(options.getPath())
              .allocator(allocator)
              .data(root)
              .writeParams(params)
              .execute();
      pendingFragments.addAll(fragments);
      totalRows += buffer.size();
      LOG.debug(
          "LanceAppendWriter[{}] wrote {} rows into {} fragment(s)",
          subtaskId,
          buffer.size(),
          fragments.size());
      buffer.clear();
    } catch (Exception e) {
      throw new IOException("Failed to write Lance fragment", e);
    }
  }
}
