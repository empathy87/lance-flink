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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/** Resolves Flink lookup key paths into Lance column names and key types. */
public final class LanceLookupKeyResolver {

  private static final String KEY_ROLE = "lookup key";

  private LanceLookupKeyResolver() {}

  public static LanceLookupKeys resolve(LookupContext context, RowType producedRowType) {
    List<String> producedNames = producedRowType.getFieldNames();
    List<LogicalType> producedTypes = producedRowType.getChildren();

    int[][] rawKeys = context.getKeys();
    if (rawKeys.length == 0) {
      throw new ValidationException(
          "Lance lookup join requires at least one equality predicate against the dimension"
              + " table; the planner produced an empty key list.");
    }

    List<String> columnNames = new ArrayList<>(rawKeys.length);
    List<LogicalType> types = new ArrayList<>(rawKeys.length);

    for (int[] keyPath : rawKeys) {
      if (keyPath.length != 1) {
        throw new ValidationException(
            "Nested lookup keys are not supported by Lance; only top-level physical columns can"
                + " be used as lookup keys. Got key path length "
                + keyPath.length
                + ".");
      }

      int columnIndex = keyPath[0];
      if (columnIndex < 0 || columnIndex >= producedNames.size()) {
        throw new ValidationException(
            "Lookup key column index "
                + columnIndex
                + " is out of range for the Lance lookup table (produced columns: "
                + producedNames
                + ").");
      }

      columnNames.add(producedNames.get(columnIndex));
      types.add(producedTypes.get(columnIndex));
    }

    validateKeyTypes(types);
    return new LanceLookupKeys(columnNames, types);
  }

  private static void validateKeyTypes(List<LogicalType> types) {
    try {
      for (LogicalType type : types) {
        LanceFilters.ensureSupportedFilterLiteralType(type, KEY_ROLE);
      }
    } catch (IllegalArgumentException e) {
      throw new ValidationException(e.getMessage(), e);
    }
  }
}
