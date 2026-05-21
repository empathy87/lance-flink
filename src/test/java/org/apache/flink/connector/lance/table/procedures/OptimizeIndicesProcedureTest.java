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
package org.apache.flink.connector.lance.table.procedures;

import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pure-unit coverage for the CSV parsing locked in by §10.1 (the {@code ARRAY<STRING>?} probe
 * failed inside Flink 1.19.1's {@code SqlProcedureCallConverter}). The ITCase no longer relies on
 * SDK behavior for unknown index names, so the parsing rules are pinned here.
 */
class OptimizeIndicesProcedureTest {

  @Test
  void splitsAndTrimsEntries() {
    assertThat(OptimizeIndicesProcedure.parseCsv("a , b,c ")).containsExactly("a", "b", "c");
  }

  @Test
  void preservesEntryOrder() {
    assertThat(OptimizeIndicesProcedure.parseCsv("z,a,m")).containsExactly("z", "a", "m");
  }

  @Test
  void rejectsBlankEntries() {
    assertThatThrownBy(() -> OptimizeIndicesProcedure.parseCsv("a,,b"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("index_names_csv entry");
  }

  @Test
  void rejectsTrailingComma() {
    assertThatThrownBy(() -> OptimizeIndicesProcedure.parseCsv("a,b,"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("index_names_csv entry");
  }

  @Test
  void rejectsWhitespaceOnlyEntries() {
    assertThatThrownBy(() -> OptimizeIndicesProcedure.parseCsv("a,   ,b"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("index_names_csv entry");
  }
}
