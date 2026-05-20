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
package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.converter.LanceTypeConverter;

import org.lance.schema.ColumnAlteration;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.AddColumn;
import org.apache.flink.table.catalog.TableChange.AddUniqueConstraint;
import org.apache.flink.table.catalog.TableChange.AddWatermark;
import org.apache.flink.table.catalog.TableChange.DropColumn;
import org.apache.flink.table.catalog.TableChange.DropConstraint;
import org.apache.flink.table.catalog.TableChange.DropWatermark;
import org.apache.flink.table.catalog.TableChange.ModifyColumn;
import org.apache.flink.table.catalog.TableChange.ModifyColumnComment;
import org.apache.flink.table.catalog.TableChange.ModifyColumnName;
import org.apache.flink.table.catalog.TableChange.ModifyColumnPosition;
import org.apache.flink.table.catalog.TableChange.ModifyPhysicalColumnType;
import org.apache.flink.table.catalog.TableChange.ModifyWatermark;
import org.apache.flink.table.catalog.TableChange.ResetOption;
import org.apache.flink.table.catalog.TableChange.SetOption;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.*;

/** Plans supported Flink ALTER TABLE changes for Lance schema evolution. */
final class LanceTableAlterPlanner {

  private LanceTableAlterPlanner() {}

  private enum AlterKind {
    ADD,
    DROP,
    RENAME
  }

  static AlterPlan plan(
      RowType currentRowType, List<String> currentPrimaryKeys, List<TableChange> changes) {
    if (changes == null || changes.isEmpty()) {
      return AlterPlan.empty();
    }

    AlterKind kind = detectSingleSupportedKind(changes);

    Set<String> primaryKeys = Set.copyOf(currentPrimaryKeys);
    Set<String> currentColumns = currentColumnNames(currentRowType);

    return switch (kind) {
      case ADD -> planAdds(changes, currentColumns);
      case DROP -> planDrops(changes, currentColumns, primaryKeys);
      case RENAME -> planRenames(changes, currentColumns, primaryKeys);
    };
  }

  private static AlterKind kindOf(TableChange change) {
    if (change instanceof AddColumn) {
      return AlterKind.ADD;
    }
    if (change instanceof DropColumn) {
      return AlterKind.DROP;
    }
    if (change instanceof ModifyColumnName) {
      return AlterKind.RENAME;
    }
    return null;
  }

  private static AlterKind detectSingleSupportedKind(List<TableChange> changes) {
    AlterKind seen = null;
    for (TableChange change : changes) {
      AlterKind kind = kindOf(change);
      if (kind == null) {
        throw rejectUnsupported(change);
      }
      if (seen != null && seen != kind) {
        // TODO: Apply mixed-kind ALTER atomically once lance-core ships multi-op transactions.
        throw reject(
            "ALTER combining " + seen + " and " + kind + " in one statement",
            "Run each change kind in its own ALTER statement.");
      }
      seen = kind;
    }
    return Objects.requireNonNull(seen, "ALTER changes must not be empty");
  }

  private static AlterPlan planAdds(List<TableChange> changes, Set<String> currentColumns) {
    List<Field> columnsToAdd = new ArrayList<>();
    Set<String> addedNames = new HashSet<>();
    for (TableChange change : changes) {
      Field field = planAdd((AddColumn) change, currentColumns);
      if (!addedNames.add(field.getName())) {
        throw reject(
            "ALTER ADD column '" + field.getName() + "'",
            "Column is added more than once in the same statement.");
      }
      columnsToAdd.add(field);
    }
    return new AlterPlan(List.copyOf(columnsToAdd), List.of(), List.of());
  }

  private static AlterPlan planDrops(
      List<TableChange> changes, Set<String> currentColumns, Set<String> primaryKeys) {
    List<String> columnsToDrop = new ArrayList<>();
    Set<String> droppedNames = new HashSet<>();
    for (TableChange change : changes) {
      String name = planDrop((DropColumn) change, currentColumns, primaryKeys);
      if (!droppedNames.add(name)) {
        throw reject(
            "ALTER DROP column '" + name + "'",
            "Column is dropped more than once in the same statement.");
      }
      columnsToDrop.add(name);
    }
    return new AlterPlan(List.of(), List.copyOf(columnsToDrop), List.of());
  }

  private static AlterPlan planRenames(
      List<TableChange> changes, Set<String> currentColumns, Set<String> primaryKeys) {
    List<ColumnAlteration> columnsToRename = new ArrayList<>();
    Set<String> renameSources = new HashSet<>();
    Set<String> renameTargets = new HashSet<>();
    for (TableChange change : changes) {
      ModifyColumnName rename = (ModifyColumnName) change;
      ColumnAlteration alteration = planRename(rename, currentColumns, primaryKeys);
      String oldName = rename.getOldColumnName();
      String newName = rename.getNewColumnName();
      if (!renameSources.add(oldName)) {
        throw reject(
            "ALTER RENAME column '" + oldName + "'",
            "Column is renamed more than once in the same statement.");
      }
      if (!renameTargets.add(newName)) {
        throw reject(
            "ALTER RENAME column to '" + newName + "'",
            "Target name appears more than once in the same statement.");
      }
      columnsToRename.add(alteration);
    }
    return new AlterPlan(List.of(), List.of(), List.copyOf(columnsToRename));
  }

  private static CatalogException rejectUnsupported(TableChange change) {
    if (change instanceof ModifyPhysicalColumnType retype) {
      return rejectModifyType(retype.getOldColumn().getName());
    }
    if (change instanceof ModifyColumnPosition) {
      return reject(
          "ALTER MODIFY column position",
          "Lance does not preserve column ordering on ALTER. Drop and re-add the column instead.");
    }
    if (change instanceof ModifyColumnComment) {
      // TODO: Add MODIFY COMMENT once column comment metadata mapping is designed.
      return reject(
          "ALTER MODIFY column comment", "Lance schema does not yet carry per-column comments.");
    }
    if (change instanceof ModifyColumn generic) {
      return rejectModifyType(generic.getOldColumn().getName());
    }
    if (change instanceof SetOption) {
      return rejectSetReset("ALTER TABLE SET");
    }
    if (change instanceof ResetOption) {
      return rejectSetReset("ALTER TABLE RESET");
    }
    if (change instanceof AddWatermark
        || change instanceof DropWatermark
        || change instanceof ModifyWatermark) {
      return reject("ALTER watermark", "Lance namespace catalog does not support watermarks.");
    }
    if (change instanceof AddUniqueConstraint || change instanceof DropConstraint) {
      // TODO: Allow PK constraint evolution once Lance supports coupled schema + PK metadata.
      return reject(
          "ALTER constraint",
          "Lance namespace catalog does not support primary-key evolution through ALTER. Recreate"
              + " the table with the desired primary key.");
    }
    return reject("ALTER change " + change.getClass().getSimpleName(), "Not supported.");
  }

  private static Field planAdd(AddColumn add, Set<String> currentColumns) {
    Column column = add.getColumn();
    String name = column.getName();
    if (!column.isPhysical()) {
      throw reject(
          "ALTER ADD column '" + name + "'", "Computed and metadata columns are not supported.");
    }
    if (add.getPosition() != null) {
      // TODO: Honor FIRST/AFTER once lance-core has position-preserving addColumns.
      throw reject(
          "ALTER ADD column '" + name + "'",
          "Column positions (FIRST / AFTER) are not supported. New columns are appended at the"
              + " end.");
    }
    LogicalType type = column.getDataType().getLogicalType();
    if (!type.isNullable()) {
      // TODO: Allow NOT NULL ADD once Lance supports safe backfill semantics.
      throw reject(
          "ALTER ADD column '" + name + "'",
          "Schema-only ADD requires the new column to be nullable. Backfilled ADD with NOT NULL"
              + " is not yet supported.");
    }
    if (currentColumns.contains(name)) {
      throw reject("ALTER ADD column '" + name + "'", "A column with that name already exists.");
    }
    return LanceTypeConverter.flinkTypeToArrowField(name, type);
  }

  private static String planDrop(
      DropColumn drop, Set<String> currentColumns, Set<String> primaryKeys) {
    String name = drop.getColumnName();
    if (!currentColumns.contains(name)) {
      throw reject(
          "ALTER DROP column '" + name + "'", "Column does not exist on the current table.");
    }
    if (primaryKeys.contains(name)) {
      // TODO: Allow PK column drop once Lance supports coupled schema + PK metadata commits.
      throw reject("ALTER DROP column '" + name + "'", "Primary key columns cannot be dropped.");
    }
    return name;
  }

  private static ColumnAlteration planRename(
      ModifyColumnName rename, Set<String> currentColumns, Set<String> primaryKeys) {
    String oldName = rename.getOldColumnName();
    String newName = rename.getNewColumnName();
    if (oldName.equals(newName)) {
      throw reject(
          "ALTER RENAME column '" + oldName + "'", "Source and target column names are the same.");
    }
    if (!currentColumns.contains(oldName)) {
      throw reject(
          "ALTER RENAME column '" + oldName + "'", "Column does not exist on the current table.");
    }
    if (primaryKeys.contains(oldName)) {
      // TODO: Allow PK column rename once Lance supports coupled schema + PK metadata commits.
      throw reject(
          "ALTER RENAME column '" + oldName + "'", "Primary key columns cannot be renamed.");
    }
    if (currentColumns.contains(newName)) {
      throw reject(
          "ALTER RENAME column '" + oldName + "' to '" + newName + "'",
          "Target name conflicts with an existing column.");
    }
    return new ColumnAlteration.Builder(oldName).rename(newName).build();
  }

  private static CatalogException rejectModifyType(String columnName) {
    // TODO: Re-enable MODIFY type/nullability after lance-core applies schema casts reliably.
    return new CatalogException(
        "ALTER MODIFY column '"
            + columnName
            + "' is not supported: lance-core does not yet apply schema-level type or nullability"
            + " changes through Dataset.alterColumns. Drop and re-add the column to change its"
            + " type.");
  }

  private static Set<String> currentColumnNames(RowType rowType) {
    Set<String> names = new HashSet<>();
    for (RowType.RowField field : rowType.getFields()) {
      names.add(field.getName());
    }
    return names;
  }

  private static CatalogException reject(String operation, String reason) {
    return new CatalogException(operation + " is not supported: " + reason);
  }

  private static CatalogException rejectSetReset(String operation) {
    // TODO: Design SET/RESET storage separately for user properties and connector-owned options.
    return new CatalogException(
        operation
            + " is not supported yet by the Lance namespace catalog. Table property storage needs"
            + " a separate design.");
  }

  /** Result of planning a list of {@link TableChange}s for a Lance dataset. */
  record AlterPlan(
      List<Field> columnsToAdd,
      List<String> columnsToDrop,
      List<ColumnAlteration> columnsToRename) {

    static AlterPlan empty() {
      return new AlterPlan(List.of(), List.of(), List.of());
    }

    boolean isEmpty() {
      return columnsToAdd.isEmpty() && columnsToDrop.isEmpty() && columnsToRename.isEmpty();
    }
  }
}
