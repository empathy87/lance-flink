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

import org.lance.Dataset;
import org.lance.merge.MergeInsertParams;
import org.lance.merge.MergeInsertResult;

import org.apache.flink.api.connector.sink2.Committer;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.List;

/** Applies Lance merge-insert committables. */
public class LanceUpsertCommitter implements Committer<LanceUpsertCommittable> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceUpsertCommitter.class);
  // TODO: Make conflict retries configurable.
  private static final int CONFLICT_RETRIES = 10;

  private final LanceOptions options;
  private final List<String> primaryKeys;
  private final BufferAllocator allocator;

  public LanceUpsertCommitter(LanceOptions options, List<String> primaryKeys) {
    if (primaryKeys == null || primaryKeys.isEmpty()) {
      throw new IllegalArgumentException("LanceUpsertCommitter requires at least one primary key");
    }
    this.options = options;
    this.primaryKeys = List.copyOf(primaryKeys);
    // TODO: Use bounded task-scoped Arrow allocator.
    this.allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Override
  public void commit(Collection<CommitRequest<LanceUpsertCommittable>> requests)
      throws IOException, InterruptedException {
    if (requests.isEmpty()) {
      return;
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

  private void applyOne(Dataset dataset, LanceUpsertCommittable committable) throws Exception {
    // TODO: Validate IPC schema against dataset schema.
    MergeInsertParams params = mergeInsertParams(committable.mode());

    try (ByteArrayInputStream bytes = new ByteArrayInputStream(committable.arrowIpcBytes());
        ReadableByteChannel channel = Channels.newChannel(bytes);
        ArrowStreamReader reader = new ArrowStreamReader(channel, allocator);
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, stream);
      MergeInsertResult result = dataset.mergeInsert(params, stream);
      closeResultDataset(result);
      LOG.debug(
          "Committed {} {} row(s) from subtask {}",
          committable.rowCount(),
          committable.mode(),
          committable.subtaskId());
    }
  }

  private MergeInsertParams mergeInsertParams(LanceUpsertCommittable.Mode mode) {
    MergeInsertParams params =
        new MergeInsertParams(primaryKeys).withConflictRetries(CONFLICT_RETRIES);

    return switch (mode) {
      case UPSERT ->
          params.withMatchedUpdateAll().withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);
      case DELETE ->
          params.withMatchedDelete().withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing);
    };
  }

  private static void closeResultDataset(MergeInsertResult result) throws Exception {
    // TODO: MergeInsertResult may carry more than just `dataset()`
    if (result != null && result.dataset() != null) {
      result.dataset().close();
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
