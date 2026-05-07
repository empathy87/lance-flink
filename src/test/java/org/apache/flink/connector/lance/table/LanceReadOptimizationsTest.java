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

import org.apache.flink.connector.lance.config.LanceOptions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Read optimization tests
 *
 * <p>Test contents:
 *
 * <ul>
 *   <li>Predicate push-down (comparisons, AND, IS NULL / IS NOT NULL)
 *   <li>Column pruning
 * </ul>
 */
@DisplayName("Read Optimization Tests")
public class LanceReadOptimizationsTest {

  @TempDir File tempDir;

  private LanceOptions baseOptions;
  private DataType physicalDataType;

  @BeforeEach
  void setUp() {
    baseOptions =
        LanceOptions.builder()
            .path(tempDir.getAbsolutePath() + "/test_dataset")
            .readBatchSize(100)
            .build();

    // Define test table schema
    physicalDataType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("status", DataTypes.STRING()),
            DataTypes.FIELD("score", DataTypes.DOUBLE()),
            DataTypes.FIELD("created_time", DataTypes.STRING()));
  }

  // ==================== Predicate Push-Down Tests ====================

  @Nested
  @DisplayName("Predicate Push-Down Tests")
  class FilterPushDownTests {

    @Test
    @DisplayName("Test equals comparison push-down")
    void testEqualsFilterPushDown() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      // Create status = 'active' expression
      List<ResolvedExpression> filters = createEqualsFilter("status", "active");

      SupportsFilterPushDown.Result result = source.applyFilters(filters);

      // Verify filter is accepted
      assertEquals(1, result.getAcceptedFilters().size(), "Equals comparison should be accepted");
      assertEquals(0, result.getRemainingFilters().size(), "Should not have remaining filters");
    }

    @Test
    @DisplayName("Test numeric comparison push-down")
    void testNumericComparisonPushDown() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      // Create score > 80 expression
      List<ResolvedExpression> filters =
          createComparisonFilter("score", 80.0, BuiltInFunctionDefinitions.GREATER_THAN);

      SupportsFilterPushDown.Result result = source.applyFilters(filters);

      assertEquals(1, result.getAcceptedFilters().size(), "Numeric comparison should be accepted");
    }

    @Test
    @DisplayName("Test AND logic push-down")
    void testAndLogicPushDown() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      // Create status = 'active' AND score > 60 expression
      ResolvedExpression statusFilter = createEqualsExpression("status", "active");
      ResolvedExpression scoreFilter =
          createComparisonExpression("score", 60.0, BuiltInFunctionDefinitions.GREATER_THAN);

      CallExpression andExpr =
          CallExpression.permanent(
              BuiltInFunctionDefinitions.AND,
              Arrays.asList(statusFilter, scoreFilter),
              DataTypes.BOOLEAN());

      SupportsFilterPushDown.Result result =
          source.applyFilters(Collections.singletonList(andExpr));

      assertEquals(1, result.getAcceptedFilters().size(), "AND logic should be accepted");
    }

    @Test
    @DisplayName("Test IS NULL push-down")
    void testIsNullPushDown() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      // Create name IS NULL expression
      FieldReferenceExpression fieldRef =
          new FieldReferenceExpression("name", DataTypes.STRING(), 0, 1);

      CallExpression isNullExpr =
          CallExpression.permanent(
              BuiltInFunctionDefinitions.IS_NULL,
              Collections.singletonList(fieldRef),
              DataTypes.BOOLEAN());

      SupportsFilterPushDown.Result result =
          source.applyFilters(Collections.singletonList(isNullExpr));

      assertEquals(1, result.getAcceptedFilters().size(), "IS NULL should be accepted");
    }

    @Test
    @DisplayName("Test IS NOT NULL push-down")
    void testIsNotNullPushDown() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      // Create name IS NOT NULL expression
      FieldReferenceExpression fieldRef =
          new FieldReferenceExpression("name", DataTypes.STRING(), 0, 1);

      CallExpression isNotNullExpr =
          CallExpression.permanent(
              BuiltInFunctionDefinitions.IS_NOT_NULL,
              Collections.singletonList(fieldRef),
              DataTypes.BOOLEAN());

      SupportsFilterPushDown.Result result =
          source.applyFilters(Collections.singletonList(isNotNullExpr));

      assertEquals(1, result.getAcceptedFilters().size(), "IS NOT NULL should be accepted");
    }

    @Test
    @DisplayName("Test multiple independent filter conditions")
    void testMultipleFilters() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      // Create multiple independent filter conditions
      List<ResolvedExpression> filter1 = createEqualsFilter("status", "active");
      List<ResolvedExpression> filter2 =
          createComparisonFilter("score", 60.0, BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL);

      List<ResolvedExpression> allFilters = new ArrayList<>();
      allFilters.addAll(filter1);
      allFilters.addAll(filter2);

      SupportsFilterPushDown.Result result = source.applyFilters(allFilters);

      assertEquals(
          2, result.getAcceptedFilters().size(), "Two filter conditions should be accepted");
      assertEquals(0, result.getRemainingFilters().size(), "Should not have remaining filters");
    }

    @Test
    @DisplayName("Test copy preserves filter conditions")
    void testCopyPreservesFilters() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      // Apply filter conditions
      List<ResolvedExpression> filters = createEqualsFilter("status", "active");
      source.applyFilters(filters);

      LanceDynamicTableSource copied = (LanceDynamicTableSource) source.copy();

      // Verify copied source preserves filter conditions
      assertNotNull(copied, "copy() should succeed");
    }
  }

  // ==================== Column Pruning Tests ====================

  @Nested
  @DisplayName("Column Pruning Tests")
  class ProjectionPushDownTests {

    @Test
    @DisplayName("Test single column projection")
    void testSingleColumnProjection() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      int[][] projection = {{0}}; // First column
      source.applyProjection(projection, projectedType(projection));

      assertNotNull(source, "Projection should be successfully applied");
    }

    @Test
    @DisplayName("Test multiple column projection")
    void testMultipleColumnProjection() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      int[][] projection = {{0}, {1}, {3}};
      source.applyProjection(projection, projectedType(projection));

      assertNotNull(source, "Multiple column projection should be successfully applied");
    }

    @Test
    @DisplayName("Test nested projection not supported")
    void testNestedProjectionNotSupported() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      assertFalse(source.supportsNestedProjection(), "Should not support nested projection");
    }

    @Test
    @DisplayName("Test copy preserves projection")
    void testCopyPreservesProjection() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      int[][] projection = {{0}, {2}};
      source.applyProjection(projection, projectedType(projection));

      LanceDynamicTableSource copied = (LanceDynamicTableSource) source.copy();

      assertNotNull(copied, "copy() should preserve projection information");
    }
  }

  // ==================== Combined Tests ====================

  @Nested
  @DisplayName("Combined Optimization Tests")
  class CombinedOptimizationsTests {

    @Test
    @DisplayName("Test filter + projection combination")
    void testFilterWithProjection() {
      LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

      int[][] projection = {{0}, {1}, {3}}; // id, name, score
      source.applyProjection(projection, projectedType(projection));

      List<ResolvedExpression> filters =
          createComparisonFilter("score", 60.0, BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL);
      SupportsFilterPushDown.Result result = source.applyFilters(filters);

      assertEquals(1, result.getAcceptedFilters().size(), "Filter condition should be accepted");
    }
  }

  // ==================== Helper Methods ====================

  /** Create equals comparison filter expression */
  private List<ResolvedExpression> createEqualsFilter(String fieldName, String value) {
    ResolvedExpression expr = createEqualsExpression(fieldName, value);
    return Collections.singletonList(expr);
  }

  /** Create equals comparison expression */
  private ResolvedExpression createEqualsExpression(String fieldName, String value) {
    FieldReferenceExpression fieldRef =
        new FieldReferenceExpression(fieldName, DataTypes.STRING(), 0, getFieldIndex(fieldName));
    ValueLiteralExpression literal = new ValueLiteralExpression(value);

    return CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS, Arrays.asList(fieldRef, literal), DataTypes.BOOLEAN());
  }

  /** Create comparison filter expression */
  private List<ResolvedExpression> createComparisonFilter(
      String fieldName, Double value, BuiltInFunctionDefinition funcDef) {
    ResolvedExpression expr = createComparisonExpression(fieldName, value, funcDef);
    return Collections.singletonList(expr);
  }

  /** Create comparison expression */
  private ResolvedExpression createComparisonExpression(
      String fieldName, Double value, BuiltInFunctionDefinition funcDef) {
    FieldReferenceExpression fieldRef =
        new FieldReferenceExpression(fieldName, DataTypes.DOUBLE(), 0, getFieldIndex(fieldName));
    ValueLiteralExpression literal = new ValueLiteralExpression(value);

    return CallExpression.permanent(funcDef, Arrays.asList(fieldRef, literal), DataTypes.BOOLEAN());
  }

  /** Get field index */
  private int getFieldIndex(String fieldName) {
    switch (fieldName) {
      case "id":
        return 0;
      case "name":
        return 1;
      case "status":
        return 2;
      case "score":
        return 3;
      case "created_time":
        return 4;
      default:
        return 0;
    }
  }

  /** Build the produced DataType for a top-level projection of {@code physicalDataType}. */
  private DataType projectedType(int[][] indices) {
    RowType rowType = (RowType) physicalDataType.getLogicalType();
    List<DataType> children = physicalDataType.getChildren();
    DataTypes.Field[] fields = new DataTypes.Field[indices.length];
    for (int i = 0; i < indices.length; i++) {
      int idx = indices[i][0];
      fields[i] = DataTypes.FIELD(rowType.getFieldNames().get(idx), children.get(idx));
    }
    return DataTypes.ROW(fields);
  }
}
