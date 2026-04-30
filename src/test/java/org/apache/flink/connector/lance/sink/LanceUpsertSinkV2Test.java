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
import org.lance.ipc.LanceScanner;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end checks for the Tier 2 upsert sink: drives {@link LanceUpsertWriter} and {@link
 * LanceUpsertCommitter} directly against a real on-disk Lance dataset and verifies the row count
 * after upserts and deletes, plus per-PK dedup semantics inside the writer.
 */
class LanceUpsertSinkV2Test {

  private static final RowType ROW_TYPE =
      new RowType(
          List.of(
              new RowType.RowField("id", new BigIntType()),
              new RowType.RowField("name", new VarCharType())));
  private static final List<String> PRIMARY_KEYS = List.of("id");
  private static final int[] PRIMARY_KEY_INDEXES = {0};

  @TempDir Path tempDir;

  private String datasetPath;

  @BeforeEach
  void setUp() throws Exception {
    datasetPath = tempDir.resolve("upsert_dataset").toString();
    // Seed the dataset with 5 rows: ids 0..4, name="seed-N". The upsert tests then write changes
    // on top of this baseline.
    seedDataset(5);
  }

  @Test
  void testWriterRoutesByRowKind() throws Exception {
    LanceOptions options = options().build();
    LanceUpsertWriter writer = newWriter(options);
    try {
      writer.write(SinkTestRows.tagged(RowKind.INSERT, 100L, "ins"), null);
      writer.write(SinkTestRows.tagged(RowKind.UPDATE_AFTER, 101L, "upd"), null);
      writer.write(SinkTestRows.tagged(RowKind.UPDATE_BEFORE, 999L, "ignored"), null);
      writer.write(SinkTestRows.tagged(RowKind.DELETE, 1L, "del"), null);

      Collection<LanceUpsertCommittable> emitted = writer.prepareCommit();
      assertThat(emitted).hasSize(2);

      LanceUpsertCommittable upsert = pick(emitted, LanceUpsertCommittable.Mode.UPSERT);
      LanceUpsertCommittable delete = pick(emitted, LanceUpsertCommittable.Mode.DELETE);
      assertThat(upsert.rowCount()).isEqualTo(2L); // INSERT + UPDATE_AFTER (distinct PKs)
      assertThat(delete.rowCount()).isEqualTo(1L); // DELETE
      assertThat(upsert.arrowIpcBytes()).isNotEmpty();
      assertThat(delete.arrowIpcBytes()).isNotEmpty();

      assertThat(writer.prepareCommit()).isEmpty();
    } finally {
      writer.close();
    }
  }

  @Test
  void testWriterEmitsNothingWhenIdle() throws Exception {
    LanceOptions options = options().build();
    LanceUpsertWriter writer = newWriter(options);
    try {
      assertThat(writer.prepareCommit()).isEmpty();
    } finally {
      writer.close();
    }
  }

  @Test
  void testCommitterUpsertsExistingRowsAndInsertsNewOnes() throws Exception {
    LanceOptions options = options().build();
    Collection<LanceUpsertCommittable> committables;
    LanceUpsertWriter writer = newWriter(options);
    try {
      writer.write(SinkTestRows.tagged(RowKind.UPDATE_AFTER, 2L, "updated"), null);
      writer.write(SinkTestRows.tagged(RowKind.INSERT, 99L, "fresh"), null);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }

    runCommitter(options, committables);

    // Seed had 5 rows; one upsert in-place + one new → 6 rows total.
    assertThat(rowCount(datasetPath)).isEqualTo(6L);
  }

  @Test
  void testCommitterDeletesByPrimaryKey() throws Exception {
    LanceOptions options = options().build();
    Collection<LanceUpsertCommittable> committables;
    LanceUpsertWriter writer = newWriter(options);
    try {
      writer.write(SinkTestRows.tagged(RowKind.DELETE, 0L, "x"), null);
      writer.write(SinkTestRows.tagged(RowKind.DELETE, 4L, "x"), null);
      // Delete for a non-existent key — committer must not throw.
      writer.write(SinkTestRows.tagged(RowKind.DELETE, 7777L, "x"), null);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }

    runCommitter(options, committables);
    assertThat(rowCount(datasetPath)).isEqualTo(3L);
  }

  @Test
  void testCommitterAppliesUpsertAndDeleteInSameCheckpoint() throws Exception {
    LanceOptions options = options().build();
    Collection<LanceUpsertCommittable> committables;
    LanceUpsertWriter writer = newWriter(options);
    try {
      writer.write(SinkTestRows.tagged(RowKind.UPDATE_AFTER, 1L, "u"), null);
      writer.write(SinkTestRows.tagged(RowKind.INSERT, 200L, "n"), null);
      writer.write(SinkTestRows.tagged(RowKind.DELETE, 3L, "x"), null);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }

    assertThat(committables).hasSize(2);
    runCommitter(options, committables);
    // Started at 5; one update (no count change) + one new + one delete → 5.
    assertThat(rowCount(datasetPath)).isEqualTo(5L);
  }

  @Test
  void testWriterDedupsKeepsLatestUpsertPerPrimaryKey() throws Exception {
    LanceOptions options = options().build();
    Collection<LanceUpsertCommittable> committables;
    LanceUpsertWriter writer = newWriter(options);
    try {
      writer.write(SinkTestRows.tagged(RowKind.UPDATE_AFTER, 2L, "first"), null);
      writer.write(SinkTestRows.tagged(RowKind.UPDATE_AFTER, 2L, "second"), null);
      writer.write(SinkTestRows.tagged(RowKind.INSERT, 2L, "third"), null);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }

    LanceUpsertCommittable upsert = pick(committables, LanceUpsertCommittable.Mode.UPSERT);
    assertThat(upsert.rowCount()).isEqualTo(1L);

    runCommitter(options, committables);
    assertThat(readNameById(datasetPath)).containsEntry(2L, "third");
  }

  @Test
  void testWriterDeleteOverridesPriorUpsertSameCheckpoint() throws Exception {
    LanceOptions options = options().build();
    Collection<LanceUpsertCommittable> committables;
    LanceUpsertWriter writer = newWriter(options);
    try {
      writer.write(SinkTestRows.tagged(RowKind.UPDATE_AFTER, 2L, "won't survive"), null);
      writer.write(SinkTestRows.tagged(RowKind.DELETE, 2L, "delete"), null);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }

    assertThat(committables).hasSize(1);
    LanceUpsertCommittable delete = pick(committables, LanceUpsertCommittable.Mode.DELETE);
    assertThat(delete.rowCount()).isEqualTo(1L);

    runCommitter(options, committables);
    // Seed had 5 (ids 0..4); -D id=2 → 4 rows.
    assertThat(rowCount(datasetPath)).isEqualTo(4L);
    assertThat(readNameById(datasetPath)).doesNotContainKey(2L);
  }

  @Test
  void testWriterUpsertOverridesPriorDeleteSameCheckpoint() throws Exception {
    LanceOptions options = options().build();
    Collection<LanceUpsertCommittable> committables;
    LanceUpsertWriter writer = newWriter(options);
    try {
      writer.write(SinkTestRows.tagged(RowKind.DELETE, 2L, "delete"), null);
      writer.write(SinkTestRows.tagged(RowKind.UPDATE_AFTER, 2L, "reborn"), null);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }

    assertThat(committables).hasSize(1);
    LanceUpsertCommittable upsert = pick(committables, LanceUpsertCommittable.Mode.UPSERT);
    assertThat(upsert.rowCount()).isEqualTo(1L);

    runCommitter(options, committables);
    assertThat(rowCount(datasetPath)).isEqualTo(5L);
    assertThat(readNameById(datasetPath)).containsEntry(2L, "reborn");
  }

  @Test
  void testWriterEmitsDeleteEvenWhenBuffersAreEmptyAfterPriorCommit() throws Exception {
    LanceOptions options = options().build();
    LanceUpsertWriter writer = newWriter(options);
    Collection<LanceUpsertCommittable> firstCommit;
    Collection<LanceUpsertCommittable> secondCommit;
    try {
      // Checkpoint A: upsert id=2.
      writer.write(SinkTestRows.tagged(RowKind.UPDATE_AFTER, 2L, "first"), null);
      firstCommit = writer.prepareCommit();
      // After prepareCommit the pending map is cleared. The next -D for id=2 lands on an empty
      // map but must still produce a delete committable.
      writer.write(SinkTestRows.tagged(RowKind.DELETE, 2L, "gone"), null);
      secondCommit = writer.prepareCommit();
    } finally {
      writer.close();
    }

    runCommitter(options, firstCommit);
    assertThat(readNameById(datasetPath)).containsEntry(2L, "first");

    assertThat(secondCommit).hasSize(1);
    LanceUpsertCommittable delete = pick(secondCommit, LanceUpsertCommittable.Mode.DELETE);
    assertThat(delete.rowCount()).isEqualTo(1L);

    runCommitter(options, secondCommit);
    assertThat(readNameById(datasetPath)).doesNotContainKey(2L);
  }

  @Test
  void testCommitterRejectsEmptyPrimaryKeys() {
    LanceOptions options = options().build();
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> new LanceUpsertCommitter(options, Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("primary key");
  }

  @Test
  void testWriterRejectsEmptyPrimaryKeys() {
    LanceOptions options = options().build();
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> new LanceUpsertWriter(options, ROW_TYPE, 0, new int[0]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("primary key");
  }

  private LanceUpsertWriter newWriter(LanceOptions options) {
    return new LanceUpsertWriter(options, ROW_TYPE, 0, PRIMARY_KEY_INDEXES);
  }

  private void runCommitter(LanceOptions options, Collection<LanceUpsertCommittable> committables)
      throws Exception {
    LanceUpsertCommitter committer = new LanceUpsertCommitter(options, PRIMARY_KEYS);
    try {
      List<Committer.CommitRequest<LanceUpsertCommittable>> requests = new ArrayList<>();
      for (LanceUpsertCommittable c : committables) {
        requests.add(new StubCommitRequest<>(c));
      }
      committer.commit(requests);
    } finally {
      committer.close();
    }
  }

  private void seedDataset(int rowCount) throws Exception {
    LanceOptions seedOptions = options().build();
    LanceAppendWriter writer =
        new LanceAppendWriter(seedOptions, ROW_TYPE, 0, Collections.emptyList());
    Collection<LanceAppendCommittable> committables;
    try {
      for (long i = 0; i < rowCount; i++) {
        writer.write(SinkTestRows.simple(i, "seed-" + i), null);
      }
      writer.flush(false);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }

    LanceAppendCommitter committer = new LanceAppendCommitter(seedOptions, ROW_TYPE);
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

  private static LanceUpsertCommittable pick(
      Collection<LanceUpsertCommittable> committables, LanceUpsertCommittable.Mode mode) {
    return committables.stream()
        .filter(c -> c.mode() == mode)
        .findFirst()
        .orElseThrow(() -> new AssertionError("no committable of mode " + mode));
  }

  private static long rowCount(String path) {
    try (Dataset dataset = Dataset.open(path)) {
      return dataset.countRows();
    }
  }

  private static Map<Long, String> readNameById(String path) throws Exception {
    Map<Long, String> out = new HashMap<>();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset dataset = Dataset.open(path, allocator);
        LanceScanner scanner = dataset.newScan();
        ArrowReader reader = scanner.scanBatches()) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        BigIntVector idVec = (BigIntVector) root.getVector("id");
        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        for (int i = 0; i < root.getRowCount(); i++) {
          out.put(idVec.get(i), new String(nameVec.get(i)));
        }
      }
    }
    return out;
  }
}
