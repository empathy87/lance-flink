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
package org.apache.flink.connector.lance.source.continuous;

import org.apache.flink.connector.lance.source.LanceSourceSplit;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceContinuousEnumStateSerializerTest {

  private final LanceContinuousEnumStateSerializer s = LanceContinuousEnumStateSerializer.INSTANCE;

  @Test
  void emptyStateRoundTrips() throws IOException {
    LanceContinuousEnumState state =
        new LanceContinuousEnumState(42L, Set.of(), List.of(), "fp-v1");
    assertThat(s.deserialize(1, s.serialize(state))).isEqualTo(state);
  }

  @Test
  void knownFragmentIdsRoundTrip() throws IOException {
    LanceContinuousEnumState state =
        new LanceContinuousEnumState(7L, Set.of(0, 2, 5), List.of(), "fp-known");
    LanceContinuousEnumState round = s.deserialize(1, s.serialize(state));
    assertThat(round.knownFragmentIds()).containsExactlyInAnyOrder(0, 2, 5);
    assertThat(round.lastEnumeratedVersion()).isEqualTo(7L);
    assertThat(round.schemaFingerprint()).isEqualTo("fp-known");
  }

  @Test
  void splitsRoundTrip() throws IOException {
    LanceSourceSplit a = new LanceSourceSplit(7L, 3, 0L);
    LanceSourceSplit b = new LanceSourceSplit(7L, 4, 17L);
    LanceContinuousEnumState state =
        new LanceContinuousEnumState(7L, Set.of(3, 4), List.of(a, b), "schema-X");
    LanceContinuousEnumState round = s.deserialize(1, s.serialize(state));
    assertThat(round.lastEnumeratedVersion()).isEqualTo(7L);
    assertThat(round.knownFragmentIds()).containsExactlyInAnyOrder(3, 4);
    assertThat(round.schemaFingerprint()).isEqualTo("schema-X");
    assertThat(round.remainingSplits()).containsExactly(a, b);
  }

  @Test
  void nullFingerprintRoundTrips() throws IOException {
    LanceContinuousEnumState state = new LanceContinuousEnumState(0L, Set.of(), List.of(), null);
    assertThat(s.deserialize(1, s.serialize(state)).schemaFingerprint()).isNull();
  }

  @Test
  void unknownVersionFailsDeserialize() {
    assertThatThrownBy(() -> s.deserialize(99, new byte[] {}))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unknown LanceContinuousEnumState version");
  }

  @Test
  void knownFragmentIdsSerializationIsDeterministic() throws IOException {
    // Two LinkedHashSet instances with the same elements in opposite iteration orders must
    // produce identical serialized bytes — the on-disk format is the canonical ordering.
    Set<Integer> ascending = new LinkedHashSet<>(Arrays.asList(0, 2, 5, 9));
    Set<Integer> descending = new LinkedHashSet<>(Arrays.asList(9, 5, 2, 0));
    LanceContinuousEnumState a = new LanceContinuousEnumState(3L, ascending, List.of(), "fp");
    LanceContinuousEnumState b = new LanceContinuousEnumState(3L, descending, List.of(), "fp");
    assertThat(s.serialize(a)).containsExactly(s.serialize(b));
  }
}
