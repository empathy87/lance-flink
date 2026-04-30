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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

/** Deep-copies RowData for buffering outside Flink's object-reuse lifecycle. */
final class RowDataMaterializer {

  private RowDataMaterializer() {}

  static void validateSupported(RowType rowType) {
    for (RowType.RowField field : rowType.getFields()) {
      validateSupported(field.getType(), field.getName());
    }
  }

  static RowData materialize(RowData row, RowType rowType) {
    GenericRowData out = new GenericRowData(row.getRowKind(), rowType.getFieldCount());
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      out.setField(i, row.isNullAt(i) ? null : copyField(row, i, rowType.getTypeAt(i)));
    }
    return out;
  }

  private static void validateSupported(LogicalType type, String path) {
    if (isScalarSupported(type)) {
      return;
    }

    if (type instanceof ArrayType arrayType) {
      validateSupported(arrayType.getElementType(), path + "[]");
      return;
    }

    if (type instanceof RowType rowType) {
      for (RowType.RowField field : rowType.getFields()) {
        validateSupported(field.getType(), path + "." + field.getName());
      }
      return;
    }

    throw new IllegalArgumentException(
        "Unsupported logical type for Lance sink materialization at '" + path + "': " + type);
  }

  private static boolean isScalarSupported(LogicalType type) {
    return type instanceof CharType
        || type instanceof VarCharType
        || type instanceof BooleanType
        || type instanceof TinyIntType
        || type instanceof SmallIntType
        || type instanceof IntType
        || type instanceof BigIntType
        || type instanceof FloatType
        || type instanceof DoubleType
        || type instanceof DateType
        || type instanceof TimeType
        || type instanceof TimestampType
        || type instanceof LocalZonedTimestampType
        || type instanceof DecimalType
        || type instanceof BinaryType
        || type instanceof VarBinaryType
        || type instanceof NullType;
  }

  private static Object copyField(RowData row, int index, LogicalType type) {
    if (type instanceof CharType || type instanceof VarCharType) {
      return copyString(row.getString(index));
    } else if (type instanceof BooleanType) {
      return row.getBoolean(index);
    } else if (type instanceof TinyIntType) {
      return row.getByte(index);
    } else if (type instanceof SmallIntType) {
      return row.getShort(index);
    } else if (type instanceof IntType || type instanceof DateType || type instanceof TimeType) {
      return row.getInt(index);
    } else if (type instanceof BigIntType) {
      return row.getLong(index);
    } else if (type instanceof FloatType) {
      return row.getFloat(index);
    } else if (type instanceof DoubleType) {
      return row.getDouble(index);
    } else if (type instanceof DecimalType decimalType) {
      return copyDecimal(row.getDecimal(index, decimalType.getPrecision(), decimalType.getScale()));
    } else if (type instanceof TimestampType timestampType) {
      return copyTimestamp(row.getTimestamp(index, timestampType.getPrecision()));
    } else if (type instanceof LocalZonedTimestampType timestampType) {
      return copyTimestamp(row.getTimestamp(index, timestampType.getPrecision()));
    } else if (type instanceof BinaryType || type instanceof VarBinaryType) {
      return row.getBinary(index).clone();
    } else if (type instanceof ArrayType arrayType) {
      return copyArray(row.getArray(index), arrayType);
    } else if (type instanceof RowType nestedType) {
      return materialize(row.getRow(index, nestedType.getFieldCount()), nestedType);
    } else if (type instanceof NullType) {
      return null;
    }

    throw new IllegalStateException("Unsupported logical type: " + type);
  }

  private static ArrayData copyArray(ArrayData array, ArrayType arrayType) {
    LogicalType elementType = arrayType.getElementType();
    Object[] copy = new Object[array.size()];

    for (int i = 0; i < array.size(); i++) {
      copy[i] = array.isNullAt(i) ? null : copyArrayElement(array, i, elementType);
    }

    return new GenericArrayData(copy);
  }

  private static Object copyArrayElement(ArrayData array, int index, LogicalType type) {
    if (type instanceof CharType || type instanceof VarCharType) {
      return copyString(array.getString(index));
    } else if (type instanceof BooleanType) {
      return array.getBoolean(index);
    } else if (type instanceof TinyIntType) {
      return array.getByte(index);
    } else if (type instanceof SmallIntType) {
      return array.getShort(index);
    } else if (type instanceof IntType || type instanceof DateType || type instanceof TimeType) {
      return array.getInt(index);
    } else if (type instanceof BigIntType) {
      return array.getLong(index);
    } else if (type instanceof FloatType) {
      return array.getFloat(index);
    } else if (type instanceof DoubleType) {
      return array.getDouble(index);
    } else if (type instanceof DecimalType decimalType) {
      return copyDecimal(
          array.getDecimal(index, decimalType.getPrecision(), decimalType.getScale()));
    } else if (type instanceof TimestampType timestampType) {
      return copyTimestamp(array.getTimestamp(index, timestampType.getPrecision()));
    } else if (type instanceof LocalZonedTimestampType timestampType) {
      return copyTimestamp(array.getTimestamp(index, timestampType.getPrecision()));
    } else if (type instanceof BinaryType || type instanceof VarBinaryType) {
      return array.getBinary(index).clone();
    } else if (type instanceof ArrayType nestedArrayType) {
      return copyArray(array.getArray(index), nestedArrayType);
    } else if (type instanceof RowType rowType) {
      return materialize(array.getRow(index, rowType.getFieldCount()), rowType);
    } else if (type instanceof NullType) {
      return null;
    }

    throw new IllegalStateException("Unsupported array element type: " + type);
  }

  private static StringData copyString(StringData value) {
    return StringData.fromString(value.toString());
  }

  private static DecimalData copyDecimal(DecimalData value) {
    return DecimalData.fromBigDecimal(value.toBigDecimal(), value.precision(), value.scale());
  }

  private static TimestampData copyTimestamp(TimestampData value) {
    return TimestampData.fromLocalDateTime(value.toLocalDateTime());
  }
}
