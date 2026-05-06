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

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Transaction;
import org.lance.merge.MergeInsertParams;
import org.lance.merge.MergeInsertResult;
import org.lance.operation.Overwrite;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Applies Lance merge-insert committables. */
public class LanceUpsertCommitter implements Committer<LanceUpsertCommittable> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceUpsertCommitter.class);
  // TODO: Make conflict retries configurable.
  private static final int CONFLICT_RETRIES = 10;

  private final LanceOptions options;
  private final List<String> primaryKeys;
  private final BufferAllocator allocator;
  private final Schema arrowSchema;
  private final boolean overwrite;
  private boolean truncated;
  private final RowLevelOperation rowLevelOperation;

  public LanceUpsertCommitter(LanceOptions options, List<String> primaryKeys) {
    this(options, null, primaryKeys, false, RowLevelOperation.NONE);
  }

  public LanceUpsertCommitter(
      LanceOptions options,
      RowType rowType,
      List<String> primaryKeys,
      boolean overwrite,
      RowLevelOperation rowLevelOperation) {
    RowLevelOperation operation = Objects.requireNonNull(rowLevelOperation, "rowLevelOperation");

    if (primaryKeys == null || primaryKeys.isEmpty()) {
      throw new IllegalArgumentException("LanceUpsertCommitter requires at least one primary key");
    }
    if (overwrite && rowType == null) {
      throw new IllegalArgumentException(
          "Overwrite mode requires a non-null row type for truncate");
    }
    if (overwrite && operation != RowLevelOperation.NONE) {
      throw new IllegalArgumentException(
          "Cannot combine INSERT OVERWRITE with row-level "
              + operation
              + " on the same committer instance");
    }

    this.options = options;
    this.primaryKeys = List.copyOf(primaryKeys);
    this.overwrite = overwrite;
    this.arrowSchema = rowType == null ? null : LanceTypeConverter.toArrowSchema(rowType);
    // TODO: Use bounded task-scoped Arrow allocator.
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.rowLevelOperation = operation;
  }

  @Override
  public void commit(Collection<CommitRequest<LanceUpsertCommittable>> requests)
      throws IOException, InterruptedException {
    if (requests.isEmpty() && !overwrite) {
      return;
    }

    if (overwrite && !truncated) {
      try {
        // TODO: Commit INSERT OVERWRITE atomically with writer-produced fragments.
        truncateDataset();
      } catch (Exception e) {
        throw new IOException("Failed to truncate Lance dataset for INSERT OVERWRITE", e);
      }
      truncated = true;
    }

    // TODO: Make merge-insert commits idempotent across Flink retries.
    for (CommitRequest<LanceUpsertCommittable> request : requests) {
      try (Dataset dataset = Dataset.open().allocator(allocator).uri(options.getPath()).build()) {
        applyOne(dataset, request.getCommittable());
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException("Failed to apply Lance merge-insert", e);
      }
    }
  }

  private void truncateDataset() {
    Overwrite operation = Overwrite.builder().fragments(List.of()).schema(arrowSchema).build();
    String datasetPath = options.getPath();
    try (Transaction txn = new Transaction.Builder().operation(operation).build();
        Dataset dataset =
            new CommitBuilder(datasetPath, allocator).writeParams(Map.of()).execute(txn)) {}
    LOG.info("Truncated Lance dataset at {} for INSERT OVERWRITE", datasetPath);
  }

  private void applyOne(Dataset dataset, LanceUpsertCommittable committable) throws Exception {
    // TODO: Validate IPC schema against dataset schema.
    MergeInsertParams params = mergeInsertParams(committable.mode());

    MergeInsertResult result = null;
    try (ByteArrayInputStream bytes = new ByteArrayInputStream(committable.arrowIpcBytes());
        ReadableByteChannel channel = Channels.newChannel(bytes);
        ArrowStreamReader reader = new ArrowStreamReader(channel, allocator);
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, stream);
      result = dataset.mergeInsert(params, stream);
      LOG.debug(
          "Committed {} {} row(s) from subtask {}",
          committable.rowCount(),
          committable.mode(),
          committable.subtaskId());
    } finally {
      closeResultDatasetQuietly(result);
    }
  }

  private MergeInsertParams mergeInsertParams(LanceUpsertCommittable.Mode mode) {
    MergeInsertParams params =
        new MergeInsertParams(primaryKeys).withConflictRetries(CONFLICT_RETRIES);

    if (rowLevelOperation == RowLevelOperation.UPDATE) {
      if (mode != LanceUpsertCommittable.Mode.UPSERT) {
        throw new IllegalArgumentException(
            "Unexpected " + mode + " committable for row-level UPDATE");
      }
      return params
          .withMatchedUpdateAll()
          .withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing);
    }

    if (rowLevelOperation == RowLevelOperation.DELETE) {
      if (mode != LanceUpsertCommittable.Mode.DELETE) {
        throw new IllegalArgumentException(
            "Unexpected " + mode + " committable for row-level DELETE");
      }
      return params.withMatchedDelete().withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing);
    }

    return switch (mode) {
      case UPSERT ->
          params.withMatchedUpdateAll().withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);
      case DELETE ->
          params.withMatchedDelete().withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing);
    };
  }

  private static void closeResultDatasetQuietly(MergeInsertResult result) {
    // TODO: MergeInsertResult may carry more than just `dataset()`
    if (result == null || result.dataset() == null) {
      return;
    }
    try {
      result.dataset().close();
    } catch (Exception e) {
      LOG.warn("Failed to close merge-insert result dataset", e);
    }
  }

  @Override
  public void close() throws Exception {
    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.warn("Failed to close allocator on upsert committer", e);
      }
    }
  }
}
