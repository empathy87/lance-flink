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
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.operation.Append;
import org.lance.operation.Overwrite;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/** Commits append fragments to a Lance dataset. */
public class LanceAppendCommitter implements Committer<LanceAppendCommittable> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceAppendCommitter.class);

  private final LanceOptions options;
  private final Schema arrowSchema;
  private final BufferAllocator allocator;
  private final boolean overwrite;

  public LanceAppendCommitter(LanceOptions options, RowType rowType) {
    this(options, rowType, false);
  }

  public LanceAppendCommitter(LanceOptions options, RowType rowType, boolean overwrite) {
    this.options = options;
    this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
    // TODO: Use bounded task-scoped Arrow allocator.
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.overwrite = overwrite;
  }

  @Override
  public void commit(Collection<CommitRequest<LanceAppendCommittable>> requests)
      throws IOException, InterruptedException {
    if (requests.isEmpty() && !overwrite) {
      return;
    }

    // TODO: Make append commits idempotent across Flink commit retries.
    List<FragmentMetadata> fragments = new ArrayList<>();
    for (CommitRequest<LanceAppendCommittable> request : requests) {
      fragments.addAll(request.getCommittable().fragments());
    }
    if (fragments.isEmpty() && !overwrite) {
      return;
    }

    String datasetPath = options.getPath();
    // TODO: Avoid probe-then-commit race for first dataset commit.
    boolean datasetExists = datasetHasManifest(datasetPath);
    boolean useOverwrite = overwrite || !datasetExists;

    try {
      if (useOverwrite) {
        // TODO: Fail closed instead of overwriting existing data on uncertain existence.
        commitOverwrite(datasetPath, fragments);
      } else {
        // TODO: Validate committed dataset schema before append.
        commitAppend(datasetPath, fragments);
      }
      LOG.info(
          "Committed {} fragment(s) from {} request(s) via {} to {}",
          fragments.size(),
          requests.size(),
          useOverwrite ? "Overwrite" : "Append",
          datasetPath);
    } catch (Exception e) {
      throw new IOException("Failed to commit Lance transaction", e);
    }
  }

  private void commitAppend(String datasetPath, List<FragmentMetadata> fragments) throws Exception {
    // TODO: Add configurable Lance commit conflict retries.
    Append operation = Append.builder().fragments(fragments).build();
    try (Transaction txn = new Transaction.Builder().operation(operation).build();
        Dataset dataset =
            new CommitBuilder(datasetPath, allocator).writeParams(Map.of()).execute(txn)) {}
  }

  private void commitOverwrite(String datasetPath, List<FragmentMetadata> fragments)
      throws Exception {
    Overwrite operation = Overwrite.builder().fragments(fragments).schema(arrowSchema).build();
    try (Transaction txn = new Transaction.Builder().operation(operation).build();
        Dataset dataset =
            new CommitBuilder(datasetPath, allocator).writeParams(Map.of()).execute(txn)) {}
  }

  @Override
  public void close() throws Exception {
    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.warn("Failed to close allocator on append committer", e);
      }
    }
  }

  private boolean datasetHasManifest(String datasetPath) {
    // TODO: Distinguish missing dataset from storage/corruption errors.
    try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetPath).build()) {
      dataset.version();
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
