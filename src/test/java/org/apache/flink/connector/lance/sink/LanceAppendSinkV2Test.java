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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end checks for the Tier 1 append sink: drives {@link LanceAppendWriter} and {@link
 * LanceAppendCommitter} directly against a real on-disk Lance dataset, then re-opens the dataset to
 * verify the committed row count.
 */
class LanceAppendSinkV2Test {

  private static final RowType ROW_TYPE =
      new RowType(
          List.of(
              new RowType.RowField("id", new BigIntType()),
              new RowType.RowField("name", new VarCharType())));

  @TempDir Path tempDir;

  private String datasetPath;

  @BeforeEach
  void setUp() {
    datasetPath = tempDir.resolve("append_dataset").toString();
  }

  @Test
  void testWriterEmitsSingleCommittablePerPrepareCommit() throws Exception {
    LanceOptions options = options().writeBatchSize(1024).build();
    LanceAppendWriter writer = new LanceAppendWriter(options, ROW_TYPE, 0, Collections.emptyList());

    try {
      for (long i = 0; i < 50; i++) {
        writer.write(SinkTestRows.simple(i, "row-" + i), null);
      }
      Collection<LanceAppendCommittable> emitted = writer.prepareCommit();
      assertThat(emitted).hasSize(1);
      LanceAppendCommittable committable = emitted.iterator().next();
      assertThat(committable.subtaskId()).isEqualTo(0);
      assertThat(committable.fragments()).isNotEmpty();
      assertThat(writer.prepareCommit()).isEmpty(); // pending drained
    } finally {
      writer.close();
    }
  }

  @Test
  void testWriterFlushesEagerlyWhenBufferFills() throws Exception {
    // batch size 4 → writing 12 rows triggers 3 mid-buffer flushes plus a final drain.
    LanceOptions options = options().writeBatchSize(4).build();
    LanceAppendWriter writer = new LanceAppendWriter(options, ROW_TYPE, 0, Collections.emptyList());
    try {
      for (long i = 0; i < 12; i++) {
        writer.write(SinkTestRows.simple(i, "x"), null);
      }
      // Snapshot before prepareCommit: pending fragments must already be present.
      List<LanceWriterState> snapshot = writer.snapshotState(1L);
      assertThat(snapshot).hasSize(1);
      assertThat(snapshot.get(0).pendingFragments()).isNotEmpty();

      Collection<LanceAppendCommittable> emitted = writer.prepareCommit();
      long totalRowsCommitted =
          emitted.iterator().next().fragments().stream().mapToLong(f -> f.getNumRows()).sum();
      assertThat(totalRowsCommitted).isEqualTo(12L);
    } finally {
      writer.close();
    }
  }

  @Test
  void testCommitterCreatesAndAppendsAcrossCheckpoints() throws Exception {
    LanceOptions options = options().writeBatchSize(1024).build();

    // First checkpoint: 30 rows. Dataset doesn't exist yet → committer must Overwrite-create.
    runCheckpoint(options, 0, 30);
    assertThat(rowCount(datasetPath)).isEqualTo(30L);

    // Second checkpoint: 20 more rows. Committer is reconstructed for the next cycle and must
    // Append because the dataset now exists.
    runCheckpoint(options, 30, 20);
    assertThat(rowCount(datasetPath)).isEqualTo(50L);
  }

  @Test
  void testRestoredWriterPicksUpPendingFragments() throws Exception {
    LanceOptions options = options().writeBatchSize(4).build();

    // Stage some fragments via writer A — capture state before draining via prepareCommit.
    LanceAppendWriter writerA =
        new LanceAppendWriter(options, ROW_TYPE, 0, Collections.emptyList());
    List<LanceWriterState> stateForRestore;
    try {
      for (long i = 0; i < 8; i++) {
        writerA.write(SinkTestRows.simple(i, "a"), null);
      }
      writerA.flush(false); // forces remaining buffer into pendingFragments
      stateForRestore = writerA.snapshotState(1L);
      assertThat(stateForRestore.get(0).pendingFragments()).isNotEmpty();
    } finally {
      writerA.close();
    }

    // Writer B starts with the recovered state and immediately emits a committable carrying it.
    LanceAppendWriter writerB = new LanceAppendWriter(options, ROW_TYPE, 0, stateForRestore);
    try {
      Collection<LanceAppendCommittable> emitted = writerB.prepareCommit();
      assertThat(emitted).hasSize(1);
      assertThat(emitted.iterator().next().fragments())
          .hasSize(stateForRestore.get(0).pendingFragments().size());
    } finally {
      writerB.close();
    }
  }

  private void runCheckpoint(LanceOptions options, long startId, int rowCount) throws Exception {
    LanceAppendWriter writer = new LanceAppendWriter(options, ROW_TYPE, 0, Collections.emptyList());
    Collection<LanceAppendCommittable> committables;
    try {
      for (int i = 0; i < rowCount; i++) {
        writer.write(SinkTestRows.simple(startId + i, "r"), null);
      }
      writer.flush(false);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }

    LanceAppendCommitter committer = new LanceAppendCommitter(options, ROW_TYPE);
    try {
      List<Committer.CommitRequest<LanceAppendCommittable>> requests = new ArrayList<>();
      for (LanceAppendCommittable c : committables) {
        requests.add(new StubCommitRequest<>(c));
      }
      committer.commit(requests);
    } finally {
      committer.close();
    }
  }

  private LanceOptions.Builder options() {
    return LanceOptions.builder()
        .path(datasetPath)
        .writeBatchSize(1024)
        .writeMaxRowsPerFile(10_000);
  }

  private static long rowCount(String path) {
    try (Dataset dataset = Dataset.open(path)) {
      return dataset.countRows();
    }
  }
}
