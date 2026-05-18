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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceSourceSplitSerializerTest {

  private final LanceSourceSplitSerializer serializer = LanceSourceSplitSerializer.INSTANCE;

  @Test
  void currentVersionIsV1() {
    assertThat(serializer.getVersion()).isEqualTo(1);
  }

  @Test
  void splitRoundTrip() throws IOException {
    LanceSourceSplit orig = new LanceSourceSplit(7L, 3, 42L);
    LanceSourceSplit round =
        serializer.deserialize(serializer.getVersion(), serializer.serialize(orig));
    assertThat(round).isEqualTo(orig);
    assertThat(round.splitId()).isEqualTo("v7-frag-3");
  }

  @Test
  void unknownVersionFails() {
    assertThatThrownBy(() -> serializer.deserialize(99, new byte[] {}))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unknown LanceSourceSplit version");
  }
}
