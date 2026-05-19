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
package org.apache.flink.connector.lance.lookup;

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
import org.apache.flink.table.types.logical.LogicalType;
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
 * Path-specific contract for the lookup key filter builder. Shared identifier and literal
 * formatting rules (string escape, NaN/Inf, DATE, TIMESTAMP precisions) are pinned in {@code
 * LanceFiltersTest}; this file only verifies behavior that is specific to the lookup path: the
 * builder fails fast on bad input rather than degrading to a Flink-side filter, and it short-
 * circuits to {@code null} when any key value is null.
 */
class LanceLookupKeyFilterBuilderTest {

  // --- wiring confirmation per major type group -------------------------------------------

  @Test
  void stringKey() {
    LanceLookupKeyFilterBuilder builder = build("name", new VarCharType(64));
    GenericRowData row = new GenericRowData(1);
    row.setField(0, StringData.fromString("O'Brien"));
    assertThat(builder.build(row)).isEqualTo("name = 'O''Brien'");
  }

  @Test
  void longKey() {
    LanceLookupKeyFilterBuilder builder = build("id", new BigIntType());
    GenericRowData row = new GenericRowData(1);
    row.setField(0, 42L);
    assertThat(builder.build(row)).isEqualTo("id = 42");
  }

  @Test
  void integerKinds() {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, (byte) 7);
    row.setField(1, (short) 8);
    row.setField(2, 9);
    LanceLookupKeyFilterBuilder builder =
        new LanceLookupKeyFilterBuilder(
            List.of("a", "b", "c"), List.of(new TinyIntType(), new SmallIntType(), new IntType()));
    assertThat(builder.build(row)).isEqualTo("a = 7 AND b = 8 AND c = 9");
  }

  @Test
  void floatAndDoubleKeys() {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, 1.5f);
    row.setField(1, 2.25d);
    LanceLookupKeyFilterBuilder builder =
        new LanceLookupKeyFilterBuilder(
            List.of("a", "b"), List.of(new FloatType(), new DoubleType()));
    assertThat(builder.build(row)).isEqualTo("a = 1.5 AND b = 2.25");
  }

  @Test
  void booleanKey() {
    LanceLookupKeyFilterBuilder builder = build("active", new BooleanType());
    GenericRowData row = new GenericRowData(1);
    row.setField(0, true);
    assertThat(builder.build(row)).isEqualTo("active = TRUE");
  }

  @Test
  void dateKey() {
    LanceLookupKeyFilterBuilder builder = build("d", new DateType());
    GenericRowData row = new GenericRowData(1);
    row.setField(0, (int) LocalDate.of(2024, 1, 15).toEpochDay());
    assertThat(builder.build(row)).isEqualTo("d = DATE '2024-01-15'");
  }

  @Test
  void timestampKey() {
    LanceLookupKeyFilterBuilder builder = build("ts", new TimestampType(6));
    GenericRowData row = new GenericRowData(1);
    row.setField(0, TimestampData.fromLocalDateTime(LocalDateTime.of(2024, 1, 15, 12, 34, 56)));
    assertThat(builder.build(row)).isEqualTo("ts = TIMESTAMP '2024-01-15 12:34:56.000000'");
  }

  // --- composite AND-ed equalities (path contract) ----------------------------------------

  @Test
  void compositeKeyEmitsAndedEqualities() {
    LanceLookupKeyFilterBuilder builder =
        new LanceLookupKeyFilterBuilder(
            List.of("tenant", "id"), List.of(new VarCharType(64), new BigIntType()));
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("acme"));
    row.setField(1, 7L);
    assertThat(builder.build(row)).isEqualTo("tenant = 'acme' AND id = 7");
  }

  // --- null lookup keys short-circuit to no filter (path contract) ------------------------

  @Test
  void nullKeyShortCircuitsToNull() {
    LanceLookupKeyFilterBuilder builder = build("id", new BigIntType());
    GenericRowData row = new GenericRowData(1);
    row.setField(0, null);
    assertThat(builder.build(row)).isNull();
  }

  @Test
  void anyNullInCompositeShortCircuitsToNull() {
    LanceLookupKeyFilterBuilder builder =
        new LanceLookupKeyFilterBuilder(
            List.of("a", "b"), List.of(new VarCharType(64), new BigIntType()));
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("x"));
    row.setField(1, null);
    assertThat(builder.build(row)).isNull();
  }

  // --- fail-fast on bad construction (path contract) --------------------------------------

  @Test
  void unsafeColumnNameRejectedAtConstruction() {
    assertThatThrownBy(() -> build("col;drop table x", new BigIntType()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not a safe Lance identifier");
    assertThatThrownBy(() -> build("0col", new BigIntType()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> build("", new BigIntType()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void unsupportedKeyTypesRejectedWithLookupKeyWording() {
    assertThatThrownBy(() -> build("k", new BinaryType(8)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported Lance lookup key type");
    assertThatThrownBy(() -> build("k", new DecimalType(10, 2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported Lance lookup key type");
  }

  @Test
  void emptyKeyListRejected() {
    assertThatThrownBy(() -> new LanceLookupKeyFilterBuilder(List.of(), List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be empty");
  }

  @Test
  void mismatchedColumnAndTypeSizesRejected() {
    assertThatThrownBy(
            () -> new LanceLookupKeyFilterBuilder(List.of("a", "b"), List.of(new BigIntType())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sizes differ");
  }

  @Test
  void rowArityShorterThanKeyListRejected() {
    LanceLookupKeyFilterBuilder builder =
        new LanceLookupKeyFilterBuilder(
            List.of("a", "b"), List.of(new BigIntType(), new BigIntType()));
    GenericRowData shortRow = new GenericRowData(1);
    shortRow.setField(0, 1L);
    assertThatThrownBy(() -> builder.build(shortRow))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arity");
  }

  private static LanceLookupKeyFilterBuilder build(String column, LogicalType type) {
    return new LanceLookupKeyFilterBuilder(List.of(column), List.of(type));
  }
}
