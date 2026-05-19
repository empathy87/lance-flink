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
package org.apache.flink.connector.lance;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.regex.Pattern;

/** Shared Lance filter identifier, literal, and predicate helpers. */
public final class LanceFilters {

  /** Identifier safety pattern — matches column names that are safe to inline into a filter. */
  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  /** Maximum {@code TIMESTAMP(p)} fractional-second precision Lance filter literals accept. */
  private static final int MAX_TIMESTAMP_PRECISION = 9;

  private static final DateTimeFormatter[] TIMESTAMP_FORMATTERS =
      buildTimestampFormatters(MAX_TIMESTAMP_PRECISION);

  private LanceFilters() {}

  public static boolean isSafeIdentifier(String name) {
    // TODO: Support quoted identifiers if Lance filter syntax exposes safe identifier escaping.
    return name != null && IDENTIFIER_PATTERN.matcher(name).matches();
  }

  /** Throws with a contextual {@code role} prefix when {@code column} is not a safe identifier. */
  public static void validateIdentifier(String column, String role) {
    if (!isSafeIdentifier(column)) {
      throw new IllegalArgumentException(
          role + " is not a safe Lance identifier: " + (column == null ? "<null>" : column));
    }
  }

  /** Single-quote-escapes a string for inlining as a Lance filter literal. */
  public static String quoteString(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  public static String formatBoolean(boolean value) {
    return value ? "TRUE" : "FALSE";
  }

  /** Formats a finite float; rejects NaN / Infinity (Lance does not accept either as a literal). */
  public static String formatFiniteFloat(float value) {
    if (Float.isNaN(value) || Float.isInfinite(value)) {
      throw new IllegalArgumentException(
          "Lance filter literal cannot be NaN or Infinity (float value rejected).");
    }
    return Float.toString(value);
  }

  public static String formatFiniteDouble(double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new IllegalArgumentException(
          "Lance filter literal cannot be NaN or Infinity (double value rejected).");
    }
    return Double.toString(value);
  }

  /** Renders a Flink {@code DATE} (epoch-day-since-1970) as a Lance {@code DATE 'YYYY-MM-DD'}. */
  public static String formatDate(int epochDay) {
    return "DATE '" + LocalDate.ofEpochDay(epochDay) + "'";
  }

  /**
   * Renders {@code TIMESTAMP(p) WITHOUT TIME ZONE} as {@code TIMESTAMP 'YYYY-MM-DD
   * HH:MM:SS[.f...]'} with exactly {@code precision} fractional digits ({@code 0..9}). Higher
   * precisions are rejected up-front rather than silently truncated.
   */
  public static String formatTimestamp(LocalDateTime value, int precision) {
    if (precision < 0 || precision > MAX_TIMESTAMP_PRECISION) {
      throw new IllegalArgumentException(
          "Lance filter literal does not support TIMESTAMP precision "
              + precision
              + " — supported range is 0.."
              + MAX_TIMESTAMP_PRECISION
              + ".");
    }
    return "TIMESTAMP '" + TIMESTAMP_FORMATTERS[precision].format(value) + "'";
  }

  private static DateTimeFormatter[] buildTimestampFormatters(int maxPrecision) {
    DateTimeFormatter[] formatters = new DateTimeFormatter[maxPrecision + 1];
    for (int precision = 0; precision <= maxPrecision; precision++) {
      DateTimeFormatterBuilder builder =
          new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss");
      if (precision > 0) {
        builder.appendFraction(ChronoField.NANO_OF_SECOND, precision, precision, true);
      }
      formatters[precision] = builder.toFormatter();
    }
    return formatters;
  }

  /**
   * Validates a {@link LogicalType} can be rendered as a Lance filter literal. {@code role} is
   * prefixed into the error message so callers (e.g. the lookup key path) can keep their
   * domain-specific wording while sharing the whitelist.
   */
  public static void ensureSupportedFilterLiteralType(LogicalType type, String role) {
    LogicalTypeRoot root = type.getTypeRoot();
    // TODO: Add DECIMAL filter literals once precision handling is verified.
    switch (root) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case VARCHAR:
      case CHAR:
      case DATE:
        return;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        // TODO: Add TIMESTAMP_LTZ once timezone semantics are defined.
        int precision = ((TimestampType) type).getPrecision();
        if (precision < 0 || precision > MAX_TIMESTAMP_PRECISION) {
          throw new IllegalArgumentException(
              "Unsupported Lance "
                  + role
                  + " type: "
                  + type.asSummaryString()
                  + ". TIMESTAMP precision must be in 0.."
                  + MAX_TIMESTAMP_PRECISION
                  + ".");
        }
        return;
      default:
        throw new IllegalArgumentException(
            "Unsupported Lance "
                + role
                + " type: "
                + type.asSummaryString()
                + ". Supported: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, BOOLEAN,"
                + " VARCHAR/CHAR, DATE, TIMESTAMP(p<=9) WITHOUT TIME ZONE.");
    }
  }

  /**
   * Renders a non-null {@link RowData} column at {@code idx} as a Lance filter literal using {@code
   * type} for dispatch. Callers must null-check {@code row} at {@code idx} first; the supported
   * type set matches {@link #ensureSupportedFilterLiteralType(LogicalType, String)} so a validated
   * key list never falls through to the {@link IllegalStateException} branch.
   */
  public static String formatLiteralFromRowData(RowData row, int idx, LogicalType type) {
    LogicalTypeRoot root = type.getTypeRoot();
    return switch (root) {
      case TINYINT -> Byte.toString(row.getByte(idx));
      case SMALLINT -> Short.toString(row.getShort(idx));
      case INTEGER -> Integer.toString(row.getInt(idx));
      case BIGINT -> Long.toString(row.getLong(idx));
      case FLOAT -> formatFiniteFloat(row.getFloat(idx));
      case DOUBLE -> formatFiniteDouble(row.getDouble(idx));
      case BOOLEAN -> formatBoolean(row.getBoolean(idx));
      case VARCHAR, CHAR -> quoteString(row.getString(idx).toString());
      case DATE -> formatDate(row.getInt(idx));
      case TIMESTAMP_WITHOUT_TIME_ZONE -> formatRowDataTimestamp(row, idx, (TimestampType) type);
      default ->
          throw new IllegalStateException(
              "Unsupported Lance filter literal type at runtime: " + type.asSummaryString());
    };
  }

  private static String formatRowDataTimestamp(RowData row, int idx, TimestampType type) {
    int precision = type.getPrecision();
    TimestampData ts = row.getTimestamp(idx, precision);
    return formatTimestamp(ts.toLocalDateTime(), precision);
  }

  /** Formats supported planner literals, or returns null to keep the predicate in Flink. */
  @Nullable
  public static String tryFormatLiteralFromValue(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    try {
      if (value instanceof String strValue) {
        return quoteString(strValue);
      }
      if (value instanceof Boolean booleanValue) {
        return formatBoolean(booleanValue);
      }
      if (value instanceof Byte byteValue) {
        return Byte.toString(byteValue);
      }
      if (value instanceof Short shortValue) {
        return Short.toString(shortValue);
      }
      if (value instanceof Integer intValue) {
        return Integer.toString(intValue);
      }
      if (value instanceof Long longValue) {
        return Long.toString(longValue);
      }
      if (value instanceof Float floatValue) {
        return formatFiniteFloat(floatValue);
      }
      if (value instanceof Double doubleValue) {
        return formatFiniteDouble(doubleValue);
      }
    } catch (IllegalArgumentException e) {
      return null;
    }
    return null;
  }

  /** Composes {@code <column> = <literal>}. Both sides must already be safely formatted. */
  public static String equality(String column, String literal) {
    return column + " = " + literal;
  }

  /**
   * Joins predicates with {@code AND}. The caller is responsible for parenthesizing children that
   * may contain top-level boolean operators — this helper does not wrap them.
   */
  public static String andAll(List<String> predicates) {
    if (predicates.isEmpty()) {
      throw new IllegalArgumentException("andAll requires at least one predicate.");
    }
    return String.join(" AND ", predicates);
  }
}
