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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure-unit tests for procedure argument validators and identifier parsing. */
class LanceProcedureUtilsTest {

  @Test
  void validateNonEmptyRejectsNullAndBlank() {
    assertThatThrownBy(() -> LanceProcedureUtils.validateNonEmpty(null, "x"))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> LanceProcedureUtils.validateNonEmpty("  ", "x"))
        .isInstanceOf(ValidationException.class);
    LanceProcedureUtils.validateNonEmpty("ok", "x");
  }

  @Test
  void validatePositiveLongRejectsZeroAndNegative() {
    assertThatThrownBy(() -> LanceProcedureUtils.validatePositive(0L, "x"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must be positive");
    assertThatThrownBy(() -> LanceProcedureUtils.validatePositive(-1L, "x"))
        .isInstanceOf(ValidationException.class);
    LanceProcedureUtils.validatePositive(1L, "x");
  }

  @Test
  void validatePositiveIntRejectsZeroAndNegative() {
    assertThatThrownBy(() -> LanceProcedureUtils.validatePositive(0, "x"))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> LanceProcedureUtils.validatePositive(-1, "x"))
        .isInstanceOf(ValidationException.class);
    LanceProcedureUtils.validatePositive(1, "x");
  }

  @Test
  void validateAtMostOneNonNullAllowsZeroOrOne() {
    LanceProcedureUtils.validateAtMostOneNonNull(
        new LanceProcedureUtils.NamedValue("a", null),
        new LanceProcedureUtils.NamedValue("b", null));
    LanceProcedureUtils.validateAtMostOneNonNull(
        new LanceProcedureUtils.NamedValue("a", 1), new LanceProcedureUtils.NamedValue("b", null));
  }

  @Test
  void validateAtMostOneNonNullRejectsTwo() {
    assertThatThrownBy(
            () ->
                LanceProcedureUtils.validateAtMostOneNonNull(
                    new LanceProcedureUtils.NamedValue("before_version", 5L),
                    new LanceProcedureUtils.NamedValue("retain_last", 3)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("At most one of")
        .hasMessageContaining("before_version")
        .hasMessageContaining("retain_last");
  }

  @Test
  void validateExactlyOneNonNullAcceptsSingleSelector() {
    LanceProcedureUtils.validateExactlyOneNonNull(
        new LanceProcedureUtils.NamedValue("a", 1), new LanceProcedureUtils.NamedValue("b", null));
  }

  @Test
  void validateExactlyOneNonNullRejectsAllNull() {
    assertThatThrownBy(
            () ->
                LanceProcedureUtils.validateExactlyOneNonNull(
                    new LanceProcedureUtils.NamedValue("before_timestamp_millis", null),
                    new LanceProcedureUtils.NamedValue("before_version", null),
                    new LanceProcedureUtils.NamedValue("retain_last", null)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Exactly one of")
        .hasMessageContaining("none was provided");
  }

  @Test
  void validateExactlyOneNonNullRejectsMultipleSet() {
    assertThatThrownBy(
            () ->
                LanceProcedureUtils.validateExactlyOneNonNull(
                    new LanceProcedureUtils.NamedValue("before_version", 5L),
                    new LanceProcedureUtils.NamedValue("retain_last", 3)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Exactly one of")
        .hasMessageContaining("before_version")
        .hasMessageContaining("retain_last");
  }

  @Test
  void validateRatioAcceptsBoundsAndMidpoint() {
    LanceProcedureUtils.validateRatio(0.0f, "x");
    LanceProcedureUtils.validateRatio(0.5f, "x");
    LanceProcedureUtils.validateRatio(1.0f, "x");
  }

  @Test
  void validateRatioRejectsOutOfRangeAndNaN() {
    assertThatThrownBy(() -> LanceProcedureUtils.validateRatio(-0.1f, "x"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must be a ratio in [0.0, 1.0]");
    assertThatThrownBy(() -> LanceProcedureUtils.validateRatio(1.5f, "x"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must be a ratio in [0.0, 1.0]");
    assertThatThrownBy(() -> LanceProcedureUtils.validateRatio(Float.NaN, "x"))
        .isInstanceOf(ValidationException.class);
  }
}
