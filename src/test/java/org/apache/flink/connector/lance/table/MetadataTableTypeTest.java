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
package org.apache.flink.connector.lance.table;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link MetadataTableType#splitName} and {@link MetadataTableType#fromSuffix}. */
class MetadataTableTypeTest {

  @Test
  void splitNameRecognizesSuffix() {
    Optional<MetadataTableType.TableNameParts> parts = MetadataTableType.splitName("t$snapshots");
    assertThat(parts).isPresent();
    assertThat(parts.get().baseName()).isEqualTo("t");
    assertThat(parts.get().suffix()).isEqualTo("snapshots");
  }

  @Test
  void splitNameWithoutSuffixReturnsEmpty() {
    assertThat(MetadataTableType.splitName("plain_table")).isEmpty();
  }

  @Test
  void splitNameWithLeadingDollarReturnsEmpty() {
    assertThat(MetadataTableType.splitName("$snapshots")).isEmpty();
  }

  @Test
  void splitNameWithTrailingDollarReturnsEmpty() {
    assertThat(MetadataTableType.splitName("t$")).isEmpty();
  }

  @Test
  void splitNameSplitsOnLastDollar() {
    Optional<MetadataTableType.TableNameParts> parts = MetadataTableType.splitName("a$b$c");
    assertThat(parts).isPresent();
    assertThat(parts.get().baseName()).isEqualTo("a$b");
    assertThat(parts.get().suffix()).isEqualTo("c");
  }

  @Test
  void splitNameRejectsNull() {
    assertThat(MetadataTableType.splitName(null)).isEmpty();
  }

  @Test
  void fromSuffixIsCaseInsensitive() {
    assertThat(MetadataTableType.fromSuffix("SNAPSHOTS")).hasValue(MetadataTableType.SNAPSHOTS);
    assertThat(MetadataTableType.fromSuffix("Tags")).hasValue(MetadataTableType.TAGS);
  }

  @Test
  void fromSuffixRejectsUnknown() {
    assertThat(MetadataTableType.fromSuffix("bogus")).isEmpty();
    assertThat(MetadataTableType.fromSuffix("")).isEmpty();
    assertThat(MetadataTableType.fromSuffix(null)).isEmpty();
  }

  @Test
  void allSuffixesRoundTrip() {
    for (MetadataTableType type : MetadataTableType.values()) {
      assertThat(MetadataTableType.fromSuffix(type.suffix())).hasValue(type);
    }
  }
}
