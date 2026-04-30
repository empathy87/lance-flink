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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Flink runtime reuses a single {@link RowData} container per record by mutating its fields
 * between emissions. The sink writers must therefore deep-copy each incoming row before buffering
 * it, otherwise older entries silently get the latest values when the buffer is later read.
 */
class RowDataMaterializerTest {

  private static final RowType ROW_TYPE =
      new RowType(
          List.of(
              new RowType.RowField("id", new BigIntType()),
              new RowType.RowField("name", new VarCharType()),
              new RowType.RowField("payload", new VarBinaryType())));

  @Test
  void testMaterializeProducesIndependentSnapshots() {
    GenericRowData reused = new GenericRowData(3);
    reused.setField(0, 1L);
    reused.setField(1, StringData.fromString("alpha"));
    reused.setField(2, new byte[] {0x01, 0x02});

    RowData snapshot1 = RowDataMaterializer.materialize(reused, ROW_TYPE);

    // Mutate the source container exactly as the runtime would.
    reused.setField(0, 2L);
    reused.setField(1, StringData.fromString("beta"));
    reused.setField(2, new byte[] {(byte) 0xff, (byte) 0xee});

    RowData snapshot2 = RowDataMaterializer.materialize(reused, ROW_TYPE);

    assertThat(snapshot1.getLong(0)).isEqualTo(1L);
    assertThat(snapshot1.getString(1).toString()).isEqualTo("alpha");
    assertThat(snapshot1.getBinary(2)).containsExactly((byte) 0x01, (byte) 0x02);

    assertThat(snapshot2.getLong(0)).isEqualTo(2L);
    assertThat(snapshot2.getString(1).toString()).isEqualTo("beta");
    assertThat(snapshot2.getBinary(2)).containsExactly((byte) 0xff, (byte) 0xee);
  }

  @Test
  void testMaterializePreservesRowKind() {
    GenericRowData row = new GenericRowData(RowKind.UPDATE_AFTER, 3);
    row.setField(0, 5L);
    row.setField(1, StringData.fromString("x"));
    row.setField(2, new byte[0]);

    RowData copy = RowDataMaterializer.materialize(row, ROW_TYPE);

    assertThat(copy.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
  }

  @Test
  void testMaterializeHandlesNullFields() {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, 7L);
    row.setField(1, null);
    row.setField(2, null);

    RowData copy = RowDataMaterializer.materialize(row, ROW_TYPE);

    assertThat(copy.isNullAt(0)).isFalse();
    assertThat(copy.getLong(0)).isEqualTo(7L);
    assertThat(copy.isNullAt(1)).isTrue();
    assertThat(copy.isNullAt(2)).isTrue();
  }

  @Test
  void testMaterializeBinaryArrayIsCloned() {
    byte[] original = {1, 2, 3, 4};
    GenericRowData row = new GenericRowData(3);
    row.setField(0, 0L);
    row.setField(1, StringData.fromString("y"));
    row.setField(2, original);

    RowData copy = RowDataMaterializer.materialize(row, ROW_TYPE);

    // Mutate the source array — the buffered copy must not see the change.
    original[0] = 99;

    assertThat(copy.getBinary(2)).containsExactly((byte) 1, (byte) 2, (byte) 3, (byte) 4);
  }

  @Test
  void testMaterializeRejectsUnsupportedType() {
    RowType withDecimal =
        new RowType(
            List.of(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField(
                    "raw", new org.apache.flink.table.types.logical.VarCharType())));
    GenericRowData row = new GenericRowData(2);
    row.setField(0, 1L);
    row.setField(1, StringData.fromString("ok"));

    // Sanity: supported.
    RowDataMaterializer.materialize(row, withDecimal);

    // Now hand it a type the helper doesn't know about.
    RowType unsupportedRow =
        new RowType(
            List.of(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField(
                    "raw",
                    new org.apache.flink.table.types.logical.MapType(
                        new IntType(), new IntType()))));

    GenericRowData mapRow = new GenericRowData(2);
    mapRow.setField(0, 1L);
    mapRow.setField(1, new Object());

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> RowDataMaterializer.materialize(mapRow, unsupportedRow))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unsupported");
  }
}
