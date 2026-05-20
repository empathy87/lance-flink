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

import org.lance.schema.ColumnAlteration;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceTableAlterPlannerTest {

  private static final RowType BASE_ROW_TYPE =
      (RowType)
          DataTypes.ROW(
                  DataTypes.FIELD("id", DataTypes.INT().notNull()),
                  DataTypes.FIELD("name", DataTypes.STRING()))
              .getLogicalType();

  private static final List<String> NO_PRIMARY_KEYS = List.of();
  private static final List<String> ID_AS_PRIMARY_KEY = List.of("id");

  @Test
  void emptyChangesProducesEmptyPlan() {
    LanceTableAlterPlanner.AlterPlan plan =
        LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of());
    assertThat(plan.isEmpty()).isTrue();
  }

  // ----- ADD COLUMN -----

  @Test
  void addColumnAppendsNullablePhysicalField() {
    TableChange change = TableChange.add(Column.physical("email", DataTypes.STRING()));
    LanceTableAlterPlanner.AlterPlan plan =
        LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change));
    assertThat(plan.columnsToAdd()).hasSize(1);
    assertThat(plan.columnsToAdd().get(0).getName()).isEqualTo("email");
    assertThat(plan.columnsToAdd().get(0).isNullable()).isTrue();
    assertThat(plan.columnsToDrop()).isEmpty();
    assertThat(plan.columnsToRename()).isEmpty();
  }

  @Test
  void addColumnRejectsNotNull() {
    TableChange change = TableChange.add(Column.physical("email", DataTypes.STRING().notNull()));
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER ADD column 'email'")
        .hasMessageContaining("nullable");
  }

  @Test
  void addColumnRejectsFirstPosition() {
    TableChange change =
        TableChange.add(
            Column.physical("email", DataTypes.STRING()), TableChange.ColumnPosition.first());
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Column positions (FIRST / AFTER) are not supported");
  }

  @Test
  void addColumnRejectsAfterPosition() {
    TableChange change =
        TableChange.add(
            Column.physical("email", DataTypes.STRING()), TableChange.ColumnPosition.after("name"));
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Column positions (FIRST / AFTER) are not supported");
  }

  @Test
  void addColumnRejectsComputedColumn() {
    ResolvedExpression expression = Mockito.mock(ResolvedExpression.class);
    Mockito.when(expression.getOutputDataType()).thenReturn(DataTypes.STRING());
    TableChange change = TableChange.add(Column.computed("upper_name", expression));
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Computed and metadata columns are not supported");
  }

  @Test
  void addColumnRejectsMetadataColumn() {
    TableChange change =
        TableChange.add(Column.metadata("ts", DataTypes.TIMESTAMP(3), null, false));
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Computed and metadata columns are not supported");
  }

  @Test
  void addColumnRejectsDuplicateName() {
    TableChange change = TableChange.add(Column.physical("name", DataTypes.STRING()));
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("A column with that name already exists");
  }

  @Test
  void addMultipleColumnsInOneStatement() {
    TableChange first = TableChange.add(Column.physical("email", DataTypes.STRING()));
    TableChange second = TableChange.add(Column.physical("phone", DataTypes.STRING()));
    LanceTableAlterPlanner.AlterPlan plan =
        LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(first, second));
    assertThat(plan.columnsToAdd()).hasSize(2);
    assertThat(plan.columnsToAdd().get(0).getName()).isEqualTo("email");
    assertThat(plan.columnsToAdd().get(1).getName()).isEqualTo("phone");
  }

  @Test
  void addMultipleColumnsRejectsDuplicateName() {
    TableChange first = TableChange.add(Column.physical("email", DataTypes.STRING()));
    TableChange second = TableChange.add(Column.physical("email", DataTypes.STRING()));
    assertThatThrownBy(
            () ->
                LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(first, second)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER ADD column 'email'")
        .hasMessageContaining("added more than once");
  }

  // ----- DROP COLUMN -----

  @Test
  void dropColumnAppendedToPlan() {
    TableChange change = TableChange.dropColumn("name");
    LanceTableAlterPlanner.AlterPlan plan =
        LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change));
    assertThat(plan.columnsToDrop()).containsExactly("name");
    assertThat(plan.columnsToAdd()).isEmpty();
    assertThat(plan.columnsToRename()).isEmpty();
  }

  @Test
  void dropColumnRejectsPrimaryKey() {
    TableChange change = TableChange.dropColumn("id");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, ID_AS_PRIMARY_KEY, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Primary key columns cannot be dropped");
  }

  @Test
  void dropColumnRejectsUnknownColumn() {
    TableChange change = TableChange.dropColumn("does_not_exist");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Column does not exist on the current table");
  }

  @Test
  void dropMultipleColumnsRejectsDuplicateName() {
    RowType wider =
        (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT().notNull()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("email", DataTypes.STRING()))
                .getLogicalType();
    TableChange first = TableChange.dropColumn("name");
    TableChange second = TableChange.dropColumn("name");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(wider, NO_PRIMARY_KEYS, List.of(first, second)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER DROP column 'name'")
        .hasMessageContaining("dropped more than once");
  }

  // ----- RENAME COLUMN -----

  @Test
  void renameColumnEmitsAlteration() {
    TableChange change =
        TableChange.modifyColumnName(Column.physical("name", DataTypes.STRING()), "full_name");
    LanceTableAlterPlanner.AlterPlan plan =
        LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change));
    assertThat(plan.columnsToRename()).hasSize(1);
    ColumnAlteration alteration = plan.columnsToRename().get(0);
    assertThat(alteration.getPath()).isEqualTo("name");
    assertThat(alteration.getRename()).hasValue("full_name");
    assertThat(alteration.getDataType()).isEmpty();
    assertThat(alteration.getNullable()).isEmpty();
  }

  @Test
  void renameColumnRejectsPrimaryKey() {
    TableChange change =
        TableChange.modifyColumnName(Column.physical("id", DataTypes.INT().notNull()), "new_id");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, ID_AS_PRIMARY_KEY, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Primary key columns cannot be renamed");
  }

  @Test
  void renameColumnRejectsSameName() {
    TableChange change =
        TableChange.modifyColumnName(Column.physical("name", DataTypes.STRING()), "name");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Source and target column names are the same");
  }

  @Test
  void acceptsMultipleRenamesInOneStatement() {
    RowType wider =
        (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT().notNull()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("alias", DataTypes.STRING()))
                .getLogicalType();
    TableChange first =
        TableChange.modifyColumnName(Column.physical("name", DataTypes.STRING()), "full_name");
    TableChange second =
        TableChange.modifyColumnName(Column.physical("alias", DataTypes.STRING()), "display_alias");
    LanceTableAlterPlanner.AlterPlan plan =
        LanceTableAlterPlanner.plan(wider, NO_PRIMARY_KEYS, List.of(first, second));
    assertThat(plan.columnsToRename()).hasSize(2);
    assertThat(plan.columnsToRename().get(0).getPath()).isEqualTo("name");
    assertThat(plan.columnsToRename().get(0).getRename()).hasValue("full_name");
    assertThat(plan.columnsToRename().get(1).getPath()).isEqualTo("alias");
    assertThat(plan.columnsToRename().get(1).getRename()).hasValue("display_alias");
    assertThat(plan.columnsToAdd()).isEmpty();
    assertThat(plan.columnsToDrop()).isEmpty();
  }

  @Test
  void renameColumnRejectsExistingTargetName() {
    TableChange change =
        TableChange.modifyColumnName(Column.physical("name", DataTypes.STRING()), "id");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("Target name conflicts with an existing column");
  }

  @Test
  void renameMultipleColumnsRejectsDuplicateSource() {
    RowType wider =
        (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT().notNull()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("alias", DataTypes.STRING()))
                .getLogicalType();
    TableChange first =
        TableChange.modifyColumnName(Column.physical("name", DataTypes.STRING()), "full_name");
    TableChange second =
        TableChange.modifyColumnName(Column.physical("name", DataTypes.STRING()), "display_name");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(wider, NO_PRIMARY_KEYS, List.of(first, second)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER RENAME column 'name'")
        .hasMessageContaining("renamed more than once");
  }

  @Test
  void renameMultipleColumnsRejectsDuplicateTarget() {
    RowType wider =
        (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT().notNull()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("alias", DataTypes.STRING()))
                .getLogicalType();
    TableChange first =
        TableChange.modifyColumnName(Column.physical("name", DataTypes.STRING()), "display_name");
    TableChange second =
        TableChange.modifyColumnName(Column.physical("alias", DataTypes.STRING()), "display_name");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(wider, NO_PRIMARY_KEYS, List.of(first, second)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER RENAME column to 'display_name'")
        .hasMessageContaining("more than once");
  }

  // ----- MODIFY TYPE / NULLABILITY (all rejected in this PR — see planner TODO) -----

  @Test
  void modifyTypeRejected() {
    Column oldColumn = Column.physical("id", DataTypes.INT());
    TableChange change = TableChange.modifyPhysicalColumnType(oldColumn, DataTypes.BIGINT());
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER MODIFY column 'id'")
        .hasMessageContaining("lance-core does not yet apply");
  }

  @Test
  void modifyNullabilityRejected() {
    Column oldColumn = Column.physical("name", DataTypes.STRING());
    Column newColumn = Column.physical("name", DataTypes.STRING().notNull());
    TableChange change = TableChange.modify(oldColumn, newColumn, null);
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER MODIFY column 'name'")
        .hasMessageContaining("lance-core does not yet apply");
  }

  // ----- REJECTED CHANGE KINDS -----

  @Test
  void modifyColumnCommentRejected() {
    Column oldColumn = Column.physical("name", DataTypes.STRING());
    TableChange change = TableChange.modifyColumnComment(oldColumn, "human readable name");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER MODIFY column comment");
  }

  @Test
  void modifyColumnPositionRejected() {
    Column column = Column.physical("name", DataTypes.STRING());
    TableChange change =
        TableChange.modifyColumnPosition(column, TableChange.ColumnPosition.first());
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER MODIFY column position");
  }

  @Test
  void addWatermarkRejected() {
    WatermarkSpec watermarkSpec = Mockito.mock(WatermarkSpec.class);
    TableChange change = TableChange.add(watermarkSpec);
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER watermark");
  }

  @Test
  void dropWatermarkRejected() {
    TableChange change = TableChange.dropWatermark();
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER watermark");
  }

  @Test
  void addUniqueConstraintRejected() {
    UniqueConstraint constraint = UniqueConstraint.primaryKey("pk", List.of("id"));
    TableChange change = TableChange.add(constraint);
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER constraint");
  }

  @Test
  void dropConstraintRejected() {
    TableChange change = TableChange.dropConstraint("pk");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER constraint");
  }

  // ----- SET / RESET -----

  @Test
  void setOptionRejectedWithTodo() {
    TableChange change = TableChange.set("owner.team", "data-platform");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER TABLE SET")
        .hasMessageContaining("not supported yet");
  }

  @Test
  void resetOptionRejectedWithTodo() {
    TableChange change = TableChange.reset("owner.team");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(change)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("ALTER TABLE RESET")
        .hasMessageContaining("not supported yet");
  }

  // ----- SINGLE-KIND GROUPING -----

  @Test
  void rejectsAddPlusDropInOneStatement() {
    TableChange add = TableChange.add(Column.physical("email", DataTypes.STRING()));
    TableChange drop = TableChange.dropColumn("name");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(add, drop)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("combining ADD and DROP");
  }

  @Test
  void rejectsAddPlusRenameInOneStatement() {
    TableChange add = TableChange.add(Column.physical("email", DataTypes.STRING()));
    TableChange rename =
        TableChange.modifyColumnName(Column.physical("name", DataTypes.STRING()), "full_name");
    assertThatThrownBy(
            () -> LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(add, rename)))
        .isInstanceOf(CatalogException.class)
        .hasMessageContaining("combining ADD and RENAME");
  }

  @Test
  void acceptsMultipleAddsInOneStatement() {
    TableChange first = TableChange.add(Column.physical("email", DataTypes.STRING()));
    TableChange second = TableChange.add(Column.physical("phone", DataTypes.STRING()));
    LanceTableAlterPlanner.AlterPlan plan =
        LanceTableAlterPlanner.plan(BASE_ROW_TYPE, NO_PRIMARY_KEYS, List.of(first, second));
    assertThat(plan.columnsToAdd()).hasSize(2);
  }

  @Test
  void acceptsMultipleDropsInOneStatement() {
    RowType wider =
        (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT().notNull()),
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("email", DataTypes.STRING()))
                .getLogicalType();
    TableChange drop1 = TableChange.dropColumn("name");
    TableChange drop2 = TableChange.dropColumn("email");
    LanceTableAlterPlanner.AlterPlan plan =
        LanceTableAlterPlanner.plan(wider, NO_PRIMARY_KEYS, List.of(drop1, drop2));
    assertThat(plan.columnsToDrop()).containsExactly("name", "email");
  }
}
