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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for resolving Flink lookup key paths into Lance key descriptors. */
class LanceLookupKeyResolverTest {

  private static final RowType PHYSICAL_ROW_TYPE =
      RowType.of(
          new org.apache.flink.table.types.logical.LogicalType[] {
            new BigIntType(), new VarCharType(64), new BigIntType()
          },
          new String[] {"id", "name", "tenant_id"});

  @Test
  void singleTopLevelKeyResolvesAgainstProducedRowType() {
    // Produced row type matches physical (no projection): key index 0 → "id".
    LanceLookupKeys keys =
        LanceLookupKeyResolver.resolve(lookupContext(new int[][] {{0}}), PHYSICAL_ROW_TYPE);
    assertThat(keys.columnNames()).containsExactly("id");
    assertThat(keys.types()).hasSize(1);
    assertThat(keys.types().get(0)).isInstanceOf(BigIntType.class);
  }

  @Test
  void compositeKeyResolves() {
    LanceLookupKeys keys =
        LanceLookupKeyResolver.resolve(lookupContext(new int[][] {{2}, {0}}), PHYSICAL_ROW_TYPE);
    assertThat(keys.columnNames()).containsExactly("tenant_id", "id");
  }

  @Test
  void keyIndexResolvedAgainstProducedRowAfterProjectionReorder() {
    // Produced row type [name, id] — key index 1 must name "id", not whatever sat at index 1
    // in the physical schema.
    RowType producedRowType =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new VarCharType(64), new BigIntType()
            },
            new String[] {"name", "id"});
    LanceLookupKeys keys =
        LanceLookupKeyResolver.resolve(lookupContext(new int[][] {{1}}), producedRowType);
    assertThat(keys.columnNames()).containsExactly("id");
    assertThat(keys.types().get(0)).isInstanceOf(BigIntType.class);
  }

  @Test
  void nonKeyColumnSelectedAlongsideKeyStillResolvesKeyCorrectly() {
    // SELECT c.name + JOIN ON c.id — produced row carries both name and id so the lookup can run.
    RowType producedRowType =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new VarCharType(64), new BigIntType()
            },
            new String[] {"name", "id"});
    LanceLookupKeys keys =
        LanceLookupKeyResolver.resolve(lookupContext(new int[][] {{1}}), producedRowType);
    assertThat(keys.columnNames()).containsExactly("id");
  }

  @Test
  void nestedKeyPathRejected() {
    assertThatThrownBy(
            () ->
                LanceLookupKeyResolver.resolve(
                    lookupContext(new int[][] {{0, 1}}), PHYSICAL_ROW_TYPE))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Nested lookup keys");
  }

  @Test
  void emptyKeyPathRejected() {
    assertThatThrownBy(
            () -> LanceLookupKeyResolver.resolve(lookupContext(new int[0][]), PHYSICAL_ROW_TYPE))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("at least one equality predicate");
  }

  @Test
  void outOfRangeKeyIndexRejected() {
    assertThatThrownBy(
            () ->
                LanceLookupKeyResolver.resolve(
                    lookupContext(new int[][] {{99}}), PHYSICAL_ROW_TYPE))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("out of range");
  }

  @Test
  void unsupportedKeyTypeRejected() {
    RowType withDecimal =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {new DecimalType(10, 2)},
            new String[] {"amount"});
    assertThatThrownBy(
            () -> LanceLookupKeyResolver.resolve(lookupContext(new int[][] {{0}}), withDecimal))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Unsupported Lance lookup key type")
        .hasMessageContaining("DECIMAL");
  }

  private static LookupContext lookupContext(int[][] keys) {
    return new TestLookupContext(keys);
  }

  /** Minimal {@link LookupContext} stub — the resolver only reads {@link #getKeys()}. */
  private static final class TestLookupContext implements LookupContext {
    private final int[][] keys;

    TestLookupContext(int[][] keys) {
      this.keys = keys;
    }

    @Override
    public int[][] getKeys() {
      return keys;
    }

    @Override
    public <T> org.apache.flink.api.common.typeinfo.TypeInformation<T> createTypeInformation(
        org.apache.flink.table.types.DataType producedDataType) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> org.apache.flink.api.common.typeinfo.TypeInformation<T> createTypeInformation(
        org.apache.flink.table.types.logical.LogicalType producedLogicalType) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter
        createDataStructureConverter(org.apache.flink.table.types.DataType producedDataType) {
      throw new UnsupportedOperationException();
    }
  }
}
