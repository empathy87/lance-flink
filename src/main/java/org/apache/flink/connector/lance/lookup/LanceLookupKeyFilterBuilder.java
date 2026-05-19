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

import org.apache.flink.connector.lance.LanceFilters;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Builds per-probe equality filters from runtime lookup key rows. */
public final class LanceLookupKeyFilterBuilder implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String KEY_ROLE = "lookup key";

  private final String[] keyColumns;
  private final LogicalType[] keyTypes;

  public LanceLookupKeyFilterBuilder(List<String> keyColumns, List<LogicalType> keyTypes) {
    Objects.requireNonNull(keyColumns, "keyColumns");
    Objects.requireNonNull(keyTypes, "keyTypes");
    if (keyColumns.isEmpty()) {
      throw new IllegalArgumentException("Lookup key column list must not be empty.");
    }
    if (keyColumns.size() != keyTypes.size()) {
      throw new IllegalArgumentException(
          "Lookup key column / type list sizes differ: "
              + keyColumns.size()
              + " vs "
              + keyTypes.size());
    }
    for (String column : keyColumns) {
      LanceFilters.validateIdentifier(column, "Lookup key column name");
    }
    validateKeyTypes(keyTypes);
    this.keyColumns = keyColumns.toArray(new String[0]);
    this.keyTypes = keyTypes.toArray(new LogicalType[0]);
  }

  /** Validates every key column has a type the lookup filter can render to a Lance literal. */
  private static void validateKeyTypes(List<LogicalType> keyTypes) {
    for (LogicalType type : keyTypes) {
      LanceFilters.ensureSupportedFilterLiteralType(type, KEY_ROLE);
    }
  }

  @Nullable
  public String build(RowData keyRow) {
    Objects.requireNonNull(keyRow, "keyRow");
    if (keyRow.getArity() < keyColumns.length) {
      throw new IllegalArgumentException(
          "Lookup key row arity " + keyRow.getArity() + " < expected " + keyColumns.length);
    }
    List<String> predicates = new ArrayList<>(keyColumns.length);
    for (int i = 0; i < keyColumns.length; i++) {
      if (keyRow.isNullAt(i)) {
        return null;
      }
      String literal = LanceFilters.formatLiteralFromRowData(keyRow, i, keyTypes[i]);
      predicates.add(LanceFilters.equality(keyColumns[i], literal));
    }
    return LanceFilters.andAll(predicates);
  }
}
