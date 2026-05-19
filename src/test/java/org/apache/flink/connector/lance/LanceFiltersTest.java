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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the Lance filter syntax rules that scan filter pushdown and lookup key filters both share.
 * Path-specific behavior (lookup throws on unsupported types, scan returns null) is verified in the
 * builder/converter tests instead.
 */
class LanceFiltersTest {

  // --- identifier safety -------------------------------------------------------------------

  @Test
  void safeIdentifiers() {
    assertThat(LanceFilters.isSafeIdentifier("name")).isTrue();
    assertThat(LanceFilters.isSafeIdentifier("snake_case_99")).isTrue();
    assertThat(LanceFilters.isSafeIdentifier("_underscore_first")).isTrue();
  }

  @Test
  void unsafeIdentifiers() {
    assertThat(LanceFilters.isSafeIdentifier(null)).isFalse();
    assertThat(LanceFilters.isSafeIdentifier("")).isFalse();
    assertThat(LanceFilters.isSafeIdentifier("0starts_with_digit")).isFalse();
    assertThat(LanceFilters.isSafeIdentifier("has space")).isFalse();
    assertThat(LanceFilters.isSafeIdentifier("has-dash")).isFalse();
    assertThat(LanceFilters.isSafeIdentifier("drop;table")).isFalse();
  }

  @Test
  void validateIdentifierIncludesRoleAndOffendingValue() {
    assertThatThrownBy(() -> LanceFilters.validateIdentifier("0bad", "Lookup key column name"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Lookup key column name")
        .hasMessageContaining("0bad");
  }

  // --- string escaping ---------------------------------------------------------------------

  @Test
  void stringEscapingDoublesSingleQuotes() {
    assertThat(LanceFilters.quoteString("plain")).isEqualTo("'plain'");
    assertThat(LanceFilters.quoteString("O'Brien")).isEqualTo("'O''Brien'");
    assertThat(LanceFilters.quoteString("'leading")).isEqualTo("'''leading'");
    assertThat(LanceFilters.quoteString("trailing'")).isEqualTo("'trailing'''");
  }

  // --- boolean -----------------------------------------------------------------------------

  @Test
  void booleanUppercased() {
    assertThat(LanceFilters.formatBoolean(true)).isEqualTo("TRUE");
    assertThat(LanceFilters.formatBoolean(false)).isEqualTo("FALSE");
  }

  // --- finite float / double ---------------------------------------------------------------

  @Test
  void finiteFloatAndDouble() {
    assertThat(LanceFilters.formatFiniteFloat(1.5f)).isEqualTo("1.5");
    assertThat(LanceFilters.formatFiniteDouble(2.25d)).isEqualTo("2.25");
  }

  @Test
  void nanAndInfinityRejected() {
    assertThatThrownBy(() -> LanceFilters.formatFiniteFloat(Float.NaN))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("NaN");
    assertThatThrownBy(() -> LanceFilters.formatFiniteFloat(Float.POSITIVE_INFINITY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Infinity");
    assertThatThrownBy(() -> LanceFilters.formatFiniteFloat(Float.NEGATIVE_INFINITY))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LanceFilters.formatFiniteDouble(Double.NaN))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("NaN");
    assertThatThrownBy(() -> LanceFilters.formatFiniteDouble(Double.POSITIVE_INFINITY))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LanceFilters.formatFiniteDouble(Double.NEGATIVE_INFINITY))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- DATE --------------------------------------------------------------------------------

  @Test
  void dateFormatting() {
    int epochDay = (int) LocalDate.of(2024, 1, 15).toEpochDay();
    assertThat(LanceFilters.formatDate(epochDay)).isEqualTo("DATE '2024-01-15'");
  }

  // --- TIMESTAMP precision -----------------------------------------------------------------

  @Test
  void timestampPrecisionZeroEmitsNoFraction() {
    LocalDateTime t = LocalDateTime.of(2024, 1, 15, 12, 34, 56);
    assertThat(LanceFilters.formatTimestamp(t, 0)).isEqualTo("TIMESTAMP '2024-01-15 12:34:56'");
  }

  @Test
  void timestampPrecisionThreeEmitsMillis() {
    LocalDateTime t = LocalDateTime.of(2024, 1, 15, 12, 34, 56).withNano(123_000_000);
    assertThat(LanceFilters.formatTimestamp(t, 3)).isEqualTo("TIMESTAMP '2024-01-15 12:34:56.123'");
  }

  @Test
  void timestampPrecisionSixEmitsMicros() {
    LocalDateTime t = LocalDateTime.of(2024, 1, 15, 12, 34, 56).withNano(123_456_000);
    assertThat(LanceFilters.formatTimestamp(t, 6))
        .isEqualTo("TIMESTAMP '2024-01-15 12:34:56.123456'");
  }

  @Test
  void timestampPrecisionNinePreservesNanoseconds() {
    LocalDateTime t = LocalDateTime.of(2024, 1, 15, 12, 34, 56).withNano(123_456_789);
    assertThat(LanceFilters.formatTimestamp(t, 9))
        .isEqualTo("TIMESTAMP '2024-01-15 12:34:56.123456789'");
  }

  @Test
  void timestampPrecisionOutOfRangeRejected() {
    LocalDateTime t = LocalDateTime.of(2024, 1, 15, 12, 34, 56);
    assertThatThrownBy(() -> LanceFilters.formatTimestamp(t, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("precision 10");
    assertThatThrownBy(() -> LanceFilters.formatTimestamp(t, -1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // --- ensureSupportedFilterLiteralType ----------------------------------------------------

  @Test
  void ensureSupportedAcceptsAllWhitelistedTypes() {
    LanceFilters.ensureSupportedFilterLiteralType(new TinyIntType(), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new SmallIntType(), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new IntType(), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new BigIntType(), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new FloatType(), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new DoubleType(), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new BooleanType(), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new VarCharType(64), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new DateType(), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new TimestampType(0), "lookup key");
    LanceFilters.ensureSupportedFilterLiteralType(new TimestampType(9), "lookup key");
  }

  @Test
  void ensureSupportedRejectsBinary() {
    assertThatThrownBy(
            () -> LanceFilters.ensureSupportedFilterLiteralType(new BinaryType(8), "lookup key"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported Lance lookup key type");
  }

  @Test
  void ensureSupportedRejectsDecimal() {
    assertThatThrownBy(
            () ->
                LanceFilters.ensureSupportedFilterLiteralType(new DecimalType(10, 2), "lookup key"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported Lance lookup key type");
  }

  @Test
  void ensureSupportedRejectsTimestampWithLocalZone() {
    assertThatThrownBy(
            () ->
                LanceFilters.ensureSupportedFilterLiteralType(
                    new LocalZonedTimestampType(3), "lookup key"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported Lance lookup key type");
  }

  @Test
  void ensureSupportedRolePrefixesErrorMessage() {
    assertThatThrownBy(
            () ->
                LanceFilters.ensureSupportedFilterLiteralType(new BinaryType(4), "filter literal"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported Lance filter literal type");
  }

  // --- formatLiteralFromRowData ------------------------------------------------------------

  @Test
  void formatRowDataIntegerKinds() {
    GenericRowData row = new GenericRowData(4);
    row.setField(0, (byte) 7);
    row.setField(1, (short) 8);
    row.setField(2, 9);
    row.setField(3, 10L);
    assertThat(LanceFilters.formatLiteralFromRowData(row, 0, new TinyIntType())).isEqualTo("7");
    assertThat(LanceFilters.formatLiteralFromRowData(row, 1, new SmallIntType())).isEqualTo("8");
    assertThat(LanceFilters.formatLiteralFromRowData(row, 2, new IntType())).isEqualTo("9");
    assertThat(LanceFilters.formatLiteralFromRowData(row, 3, new BigIntType())).isEqualTo("10");
  }

  @Test
  void formatRowDataFloatAndDouble() {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, 1.5f);
    row.setField(1, 2.25d);
    assertThat(LanceFilters.formatLiteralFromRowData(row, 0, new FloatType())).isEqualTo("1.5");
    assertThat(LanceFilters.formatLiteralFromRowData(row, 1, new DoubleType())).isEqualTo("2.25");
  }

  @Test
  void formatRowDataBoolean() {
    GenericRowData row = new GenericRowData(1);
    row.setField(0, true);
    assertThat(LanceFilters.formatLiteralFromRowData(row, 0, new BooleanType())).isEqualTo("TRUE");
  }

  @Test
  void formatRowDataStringEscapes() {
    GenericRowData row = new GenericRowData(1);
    row.setField(0, StringData.fromString("O'Brien"));
    assertThat(LanceFilters.formatLiteralFromRowData(row, 0, new VarCharType(64)))
        .isEqualTo("'O''Brien'");
  }

  @Test
  void formatRowDataDate() {
    GenericRowData row = new GenericRowData(1);
    row.setField(0, (int) LocalDate.of(2024, 1, 15).toEpochDay());
    assertThat(LanceFilters.formatLiteralFromRowData(row, 0, new DateType()))
        .isEqualTo("DATE '2024-01-15'");
  }

  @Test
  void formatRowDataTimestampHonorsPrecision() {
    GenericRowData row = new GenericRowData(1);
    row.setField(
        0,
        TimestampData.fromLocalDateTime(
            LocalDateTime.of(2024, 1, 15, 12, 34, 56).withNano(123_456_789)));
    assertThat(LanceFilters.formatLiteralFromRowData(row, 0, new TimestampType(9)))
        .isEqualTo("TIMESTAMP '2024-01-15 12:34:56.123456789'");
    assertThat(LanceFilters.formatLiteralFromRowData(row, 0, new TimestampType(3)))
        .isEqualTo("TIMESTAMP '2024-01-15 12:34:56.123'");
  }

  // --- tryFormatLiteralFromValue (Object dispatch for scan pushdown) -----------------------

  @Test
  void objectLiteralForSupportedTypes() {
    assertThat(LanceFilters.tryFormatLiteralFromValue("active")).isEqualTo("'active'");
    assertThat(LanceFilters.tryFormatLiteralFromValue("it's")).isEqualTo("'it''s'");
    assertThat(LanceFilters.tryFormatLiteralFromValue(true)).isEqualTo("TRUE");
    assertThat(LanceFilters.tryFormatLiteralFromValue(false)).isEqualTo("FALSE");
    assertThat(LanceFilters.tryFormatLiteralFromValue(1.5f)).isEqualTo("1.5");
    assertThat(LanceFilters.tryFormatLiteralFromValue(2.25d)).isEqualTo("2.25");
    assertThat(LanceFilters.tryFormatLiteralFromValue(10L)).isEqualTo("10");
    assertThat(LanceFilters.tryFormatLiteralFromValue(7)).isEqualTo("7");
    assertThat(LanceFilters.tryFormatLiteralFromValue((short) 8)).isEqualTo("8");
    assertThat(LanceFilters.tryFormatLiteralFromValue((byte) 9)).isEqualTo("9");
  }

  @Test
  void objectLiteralNullReturnsNull() {
    assertThat(LanceFilters.tryFormatLiteralFromValue(null)).isNull();
  }

  @Test
  void objectLiteralNanAndInfinityFallBackToNull() {
    assertThat(LanceFilters.tryFormatLiteralFromValue(Float.NaN)).isNull();
    assertThat(LanceFilters.tryFormatLiteralFromValue(Double.NaN)).isNull();
    assertThat(LanceFilters.tryFormatLiteralFromValue(Float.POSITIVE_INFINITY)).isNull();
    assertThat(LanceFilters.tryFormatLiteralFromValue(Double.POSITIVE_INFINITY)).isNull();
    assertThat(LanceFilters.tryFormatLiteralFromValue(Double.NEGATIVE_INFINITY)).isNull();
  }

  @Test
  void objectLiteralUnsupportedTypeFallsBackToNull() {
    assertThat(LanceFilters.tryFormatLiteralFromValue(new byte[] {1, 2, 3})).isNull();
    assertThat(LanceFilters.tryFormatLiteralFromValue(LocalDate.of(2024, 1, 15))).isNull();
    assertThat(LanceFilters.tryFormatLiteralFromValue(new Object())).isNull();
  }

  @Test
  void objectLiteralNumberSubtypesOutsideAllowListFallBackToNull() {
    // BigDecimal / BigInteger extend Number but lookup-key validation rejects DECIMAL — the scan
    // path must reject them too so the two paths cannot drift on which literals are accepted.
    assertThat(LanceFilters.tryFormatLiteralFromValue(new java.math.BigDecimal("1.23"))).isNull();
    assertThat(LanceFilters.tryFormatLiteralFromValue(java.math.BigInteger.valueOf(42))).isNull();
    assertThat(
            LanceFilters.tryFormatLiteralFromValue(
                new java.util.concurrent.atomic.AtomicInteger(7)))
        .isNull();
    assertThat(
            LanceFilters.tryFormatLiteralFromValue(new java.util.concurrent.atomic.AtomicLong(7L)))
        .isNull();
  }

  // --- equality / andAll -------------------------------------------------------------------

  @Test
  void equalityComposition() {
    assertThat(LanceFilters.equality("id", "42")).isEqualTo("id = 42");
    assertThat(LanceFilters.equality("name", "'O''Brien'")).isEqualTo("name = 'O''Brien'");
  }

  @Test
  void andAllJoinsWithoutOuterParentheses() {
    assertThat(LanceFilters.andAll(List.of("a = 1"))).isEqualTo("a = 1");
    assertThat(LanceFilters.andAll(List.of("a = 1", "b = 2"))).isEqualTo("a = 1 AND b = 2");
    assertThat(LanceFilters.andAll(List.of("a = 1", "b = 2", "c = 3")))
        .isEqualTo("a = 1 AND b = 2 AND c = 3");
  }

  @Test
  void andAllEmptyRejected() {
    assertThatThrownBy(() -> LanceFilters.andAll(List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
