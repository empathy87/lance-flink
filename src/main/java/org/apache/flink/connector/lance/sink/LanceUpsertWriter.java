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

import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/** Deduplicating writer for Lance upsert committables. */
// TODO: Make pending upsert/delete state checkpointed.
public class LanceUpsertWriter implements CommittingSinkWriter<RowData, LanceUpsertCommittable> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceUpsertWriter.class);

  private final LanceOptions options;
  private final RowType rowType;
  private final int subtaskId;
  private final RowData.FieldGetter[] primaryKeyGetters;

  private final BufferAllocator allocator;
  private final RowDataConverter converter;
  private final Schema arrowSchema;
  private final LinkedHashMap<List<Object>, PendingAction> pending;

  private long committableCounter;

  public LanceUpsertWriter(
      LanceOptions options, RowType rowType, int subtaskId, int[] primaryKeyIndexes) {
    if (options.getPath() == null || options.getPath().isEmpty()) {
      throw new IllegalArgumentException("Lance dataset path cannot be empty");
    }
    if (primaryKeyIndexes == null || primaryKeyIndexes.length == 0) {
      throw new IllegalArgumentException("LanceUpsertWriter requires at least one primary key");
    }
    this.options = options;
    this.rowType = rowType;
    this.subtaskId = subtaskId;
    this.primaryKeyGetters = new RowData.FieldGetter[primaryKeyIndexes.length];
    for (int i = 0; i < primaryKeyIndexes.length; i++) {
      int pos = primaryKeyIndexes[i];
      LogicalType type = rowType.getTypeAt(pos);
      this.primaryKeyGetters[i] = RowData.createFieldGetter(type, pos);
    }
    // TODO: Use bounded task-scoped Arrow allocator.
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.converter = new RowDataConverter(rowType);
    this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
    this.pending = new LinkedHashMap<>(options.getWriteBatchSize());
  }

  @Override
  public void write(RowData element, Context context) {
    // TODO: Bound or spill pending actions for high-cardinality keys.
    RowKind kind = element.getRowKind();
    switch (kind) {
      case INSERT:
      case UPDATE_AFTER:
        RowData upsertRow = materialize(element);
        pending.put(extractKey(upsertRow), PendingAction.upsert(upsertRow));
        break;
      case DELETE:
        // TODO: Materialize key-only rows for deletes.
        RowData deleteRow = materialize(element);
        pending.put(extractKey(deleteRow), PendingAction.delete(deleteRow));
        break;
      case UPDATE_BEFORE:
        return;
      default:
        throw new IllegalStateException("Unexpected RowKind: " + kind);
    }
  }

  @Override
  public void flush(boolean endOfInput) {}

  @Override
  public Collection<LanceUpsertCommittable> prepareCommit() throws IOException {
    if (pending.isEmpty()) {
      return List.of();
    }
    List<RowData> upserts = new ArrayList<>();
    List<RowData> deletes = new ArrayList<>();
    for (PendingAction action : pending.values()) {
      if (action.kind == PendingAction.Kind.UPSERT) {
        upserts.add(action.row());
      } else {
        deletes.add(action.row());
      }
    }
    pending.clear();

    // TODO: Use real Flink checkpoint id for committable idempotency.
    long committableId = committableCounter++;
    List<LanceUpsertCommittable> out = new ArrayList<>(2);
    if (!upserts.isEmpty()) {
      out.add(buildCommittable(committableId, LanceUpsertCommittable.Mode.UPSERT, upserts));
    }
    if (!deletes.isEmpty()) {
      out.add(buildCommittable(committableId, LanceUpsertCommittable.Mode.DELETE, deletes));
    }
    LOG.debug(
        "LanceUpsertWriter[{}] prepareCommit emitting {} committable(s)", subtaskId, out.size());
    return out;
  }

  @Override
  public void close() throws Exception {
    LOG.info("LanceUpsertWriter[{}] closing", subtaskId);
    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.warn("Failed to close allocator on upsert writer[{}]", subtaskId, e);
      }
    }
  }

  private List<Object> extractKey(RowData row) {
    // TODO: Use compact typed PK keys instead of generic object lists.
    Object[] key = new Object[primaryKeyGetters.length];
    for (int i = 0; i < primaryKeyGetters.length; i++) {
      Object value = primaryKeyGetters[i].getFieldOrNull(row);
      if (value == null) {
        throw new IllegalArgumentException("Primary key field at position " + i + " is null");
      }
      key[i] = value;
    }
    return List.of(key);
  }

  private RowData materialize(RowData element) {
    return RowDataMaterializer.materialize(element, rowType);
  }

  private LanceUpsertCommittable buildCommittable(
      long committableId, LanceUpsertCommittable.Mode mode, List<RowData> rows) throws IOException {
    // TODO: Chunk large Arrow IPC payloads.
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      converter.toVectorSchemaRoot(rows, root);
      try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
          WritableByteChannel channel = Channels.newChannel(bytes);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
        return new LanceUpsertCommittable(
            committableId, subtaskId, mode, bytes.toByteArray(), rows.size());
      }
    }
  }

  /** Pending action recorded against a primary key in the dedup map. */
  private record PendingAction(Kind kind, RowData row) {
    enum Kind {
      UPSERT,
      DELETE
    }

    static PendingAction upsert(RowData row) {
      return new PendingAction(Kind.UPSERT, row);
    }

    static PendingAction delete(RowData row) {
      return new PendingAction(Kind.DELETE, row);
    }
  }
}
