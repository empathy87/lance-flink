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
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Shared validation and result-row helpers for Lance catalog procedures. */
public final class LanceProcedureUtils {

  private LanceProcedureUtils() {}

  public static void validateNonEmpty(String value, String argName) {
    if (value == null || value.isBlank()) {
      throw new ValidationException(argName + " must not be null or empty");
    }
  }

  public static void validatePositive(long value, String argName) {
    if (value <= 0) {
      throw new ValidationException(argName + " must be positive, got " + value);
    }
  }

  public static void validatePositive(int value, String argName) {
    if (value <= 0) {
      throw new ValidationException(argName + " must be positive, got " + value);
    }
  }

  /** Named argument value used by selector validation helpers. */
  public record NamedValue(String name, Object value) {
    public NamedValue {
      Objects.requireNonNull(name, "name");
    }
  }

  /** Rejects calls that set more than one of the given arguments. */
  public static void validateAtMostOneNonNull(NamedValue... values) {
    List<String> setNames =
        Arrays.stream(values).filter(v -> v.value() != null).map(NamedValue::name).toList();
    if (setNames.size() <= 1) {
      return;
    }
    String allNames = Arrays.stream(values).map(NamedValue::name).collect(Collectors.joining(", "));
    throw new ValidationException(
        "At most one of (" + allNames + ") may be set, but got: " + String.join(", ", setNames));
  }

  /** Rejects calls that set zero or more-than-one of the given arguments. */
  public static void validateExactlyOneNonNull(NamedValue... values) {
    List<String> setNames =
        Arrays.stream(values).filter(v -> v.value() != null).map(NamedValue::name).toList();
    if (setNames.size() == 1) {
      return;
    }
    String allNames = Arrays.stream(values).map(NamedValue::name).collect(Collectors.joining(", "));
    if (setNames.isEmpty()) {
      throw new ValidationException(
          "Exactly one of (" + allNames + ") must be set, but none was provided.");
    }
    throw new ValidationException(
        "Exactly one of (" + allNames + ") must be set, but got: " + String.join(", ", setNames));
  }

  /** Validates a ratio value is in the closed interval {@code [0.0, 1.0]}. */
  public static void validateRatio(float value, String argName) {
    if (Float.isNaN(value) || value < 0.0f || value > 1.0f) {
      throw new ValidationException(argName + " must be a ratio in [0.0, 1.0], got " + value);
    }
  }

  public static Row[] singleRowArray(Object... values) {
    return new Row[] {Row.of(values)};
  }
}
