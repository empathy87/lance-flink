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

import org.lance.FragmentMetadata;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Round-trip tests for the SimpleVersionedSerializers used to ferry committables and state. */
class LanceCommittableSerializersTest {

  @TempDir static Path sharedTempDir;

  private static List<FragmentMetadata> sampleFragments;

  @BeforeAll
  static void generateRealFragments() throws Exception {
    // Spin up a real LanceAppendWriter so we have authentic FragmentMetadata to round-trip.
    Path datasetPath = sharedTempDir.resolve("frag-source-dataset");
    LanceOptions options =
        LanceOptions.builder().path(datasetPath.toString()).writeBatchSize(8).build();
    RowType rowType =
        new RowType(
            List.of(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType())));
    LanceAppendWriter writer = new LanceAppendWriter(options, rowType, 0, Collections.emptyList());
    try {
      for (long i = 0; i < 4; i++) {
        writer.write(SinkTestRows.simple(i, "n" + i), null);
      }
      writer.flush(false);
      // prepareCommit drains pending fragments — capture the committable's fragments before clear.
      List<LanceAppendCommittable> emitted = new ArrayList<>(writer.prepareCommit());
      assertThat(emitted).hasSize(1);
      sampleFragments = emitted.get(0).fragments();
      assertThat(sampleFragments).isNotEmpty();
    } finally {
      writer.close();
    }
  }

  @Test
  void testAppendCommittableRoundTripEmptyFragments() throws IOException {
    LanceAppendCommittableSerializer serializer = new LanceAppendCommittableSerializer();
    LanceAppendCommittable original = new LanceAppendCommittable(7L, 3, Collections.emptyList());
    byte[] bytes = serializer.serialize(original);
    LanceAppendCommittable restored = serializer.deserialize(serializer.getVersion(), bytes);
    assertThat(restored.committableId()).isEqualTo(7L);
    assertThat(restored.subtaskId()).isEqualTo(3);
    assertThat(restored.fragments()).isEmpty();
  }

  @Test
  void testAppendCommittableRoundTripWithFragments() throws IOException {
    LanceAppendCommittableSerializer serializer = new LanceAppendCommittableSerializer();
    LanceAppendCommittable original = new LanceAppendCommittable(42L, 1, sampleFragments);
    byte[] bytes = serializer.serialize(original);
    LanceAppendCommittable restored = serializer.deserialize(serializer.getVersion(), bytes);
    assertThat(restored.committableId()).isEqualTo(42L);
    assertThat(restored.subtaskId()).isEqualTo(1);
    assertThat(restored.fragments()).hasSize(sampleFragments.size());
    for (int i = 0; i < sampleFragments.size(); i++) {
      assertThat(restored.fragments().get(i).getId()).isEqualTo(sampleFragments.get(i).getId());
      assertThat(restored.fragments().get(i).getNumRows())
          .isEqualTo(sampleFragments.get(i).getNumRows());
    }
  }

  @Test
  void testAppendCommittableSerializerRejectsUnknownVersion() throws IOException {
    LanceAppendCommittableSerializer serializer = new LanceAppendCommittableSerializer();
    byte[] bytes = serializer.serialize(new LanceAppendCommittable(0L, 0, Collections.emptyList()));
    int badVersion = serializer.getVersion() + 99;
    assertThatThrownBy(() -> serializer.deserialize(badVersion, bytes))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("version");
  }

  @Test
  void testUpsertCommittableRoundTripUpsertMode() throws IOException {
    LanceUpsertCommittableSerializer serializer = new LanceUpsertCommittableSerializer();
    byte[] payload = new byte[] {0x10, 0x20, 0x30, 0x40, 0x55, 0x77};
    LanceUpsertCommittable original =
        new LanceUpsertCommittable(11L, 2, LanceUpsertCommittable.Mode.UPSERT, payload, 6L);
    byte[] bytes = serializer.serialize(original);
    LanceUpsertCommittable restored = serializer.deserialize(serializer.getVersion(), bytes);
    assertThat(restored.committableId()).isEqualTo(11L);
    assertThat(restored.subtaskId()).isEqualTo(2);
    assertThat(restored.mode()).isEqualTo(LanceUpsertCommittable.Mode.UPSERT);
    assertThat(restored.rowCount()).isEqualTo(6L);
    assertThat(restored.arrowIpcBytes()).containsExactly(payload);
  }

  @Test
  void testUpsertCommittableRoundTripDeleteModeEmptyPayload() throws IOException {
    LanceUpsertCommittableSerializer serializer = new LanceUpsertCommittableSerializer();
    LanceUpsertCommittable original =
        new LanceUpsertCommittable(0L, 0, LanceUpsertCommittable.Mode.DELETE, new byte[0], 0L);
    byte[] bytes = serializer.serialize(original);
    LanceUpsertCommittable restored = serializer.deserialize(serializer.getVersion(), bytes);
    assertThat(restored.mode()).isEqualTo(LanceUpsertCommittable.Mode.DELETE);
    assertThat(restored.arrowIpcBytes()).isEmpty();
    assertThat(restored.rowCount()).isZero();
  }

  @Test
  void testUpsertCommittableSerializerRejectsUnknownVersion() throws IOException {
    LanceUpsertCommittableSerializer serializer = new LanceUpsertCommittableSerializer();
    byte[] bytes =
        serializer.serialize(
            new LanceUpsertCommittable(
                0L, 0, LanceUpsertCommittable.Mode.UPSERT, new byte[] {1}, 1L));
    int badVersion = serializer.getVersion() + 99;
    assertThatThrownBy(() -> serializer.deserialize(badVersion, bytes))
        .isInstanceOf(IOException.class);
  }

  @Test
  void testWriterStateRoundTripEmpty() throws IOException {
    LanceWriterStateSerializer serializer = new LanceWriterStateSerializer();
    LanceWriterState original = new LanceWriterState(Collections.emptyList());
    byte[] bytes = serializer.serialize(original);
    LanceWriterState restored = serializer.deserialize(serializer.getVersion(), bytes);
    assertThat(restored.isEmpty()).isTrue();
  }

  @Test
  void testWriterStateRoundTripWithFragments() throws IOException {
    LanceWriterStateSerializer serializer = new LanceWriterStateSerializer();
    LanceWriterState original = new LanceWriterState(sampleFragments);
    byte[] bytes = serializer.serialize(original);
    LanceWriterState restored = serializer.deserialize(serializer.getVersion(), bytes);
    assertThat(restored.pendingFragments()).hasSize(sampleFragments.size());
    assertThat(restored.pendingFragments().get(0).getId())
        .isEqualTo(sampleFragments.get(0).getId());
  }
}
