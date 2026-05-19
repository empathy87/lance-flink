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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link LanceFilterExpressionConverter}. */
class LanceFilterExpressionConverterTest {

  @Test
  void equalsString() {
    assertThat(convert(equals("status", DataTypes.STRING(), "active")))
        .isEqualTo("status = 'active'");
  }

  @Test
  void notEqualsLong() {
    assertThat(
            convert(
                comparison("id", DataTypes.BIGINT(), 10L, BuiltInFunctionDefinitions.NOT_EQUALS)))
        .isEqualTo("id != 10");
  }

  @Test
  void greaterThanLong() {
    assertThat(
            convert(
                comparison("id", DataTypes.BIGINT(), 10L, BuiltInFunctionDefinitions.GREATER_THAN)))
        .isEqualTo("id > 10");
  }

  @Test
  void greaterThanOrEqualDouble() {
    assertThat(
            convert(
                comparison(
                    "score",
                    DataTypes.DOUBLE(),
                    60.0,
                    BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL)))
        .isEqualTo("score >= 60.0");
  }

  @Test
  void lessThanLong() {
    assertThat(
            convert(comparison("id", DataTypes.BIGINT(), 5L, BuiltInFunctionDefinitions.LESS_THAN)))
        .isEqualTo("id < 5");
  }

  @Test
  void lessThanOrEqualLong() {
    assertThat(
            convert(
                comparison(
                    "id", DataTypes.BIGINT(), 5L, BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL)))
        .isEqualTo("id <= 5");
  }

  @Test
  void booleanLiteralUppercased() {
    assertThat(convert(equals("active", DataTypes.BOOLEAN(), Boolean.TRUE)))
        .isEqualTo("active = TRUE");
  }

  @Test
  void reversedLessThanFlipsToGreaterThan() {
    // 10 < id  ⟶  id > 10
    FieldReferenceExpression fieldRef = field("id", DataTypes.BIGINT(), 0);
    ValueLiteralExpression literal = new ValueLiteralExpression(10L);
    CallExpression expr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.LESS_THAN,
            Arrays.asList(literal, fieldRef),
            DataTypes.BOOLEAN());
    assertThat(convert(expr)).isEqualTo("id > 10");
  }

  @Test
  void reversedGreaterThanOrEqualFlipsToLessThanOrEqual() {
    // 10 >= id  ⟶  id <= 10
    FieldReferenceExpression fieldRef = field("id", DataTypes.BIGINT(), 0);
    ValueLiteralExpression literal = new ValueLiteralExpression(10L);
    CallExpression expr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
            Arrays.asList(literal, fieldRef),
            DataTypes.BOOLEAN());
    assertThat(convert(expr)).isEqualTo("id <= 10");
  }

  @Test
  void andOfTwoComparisons() {
    ResolvedExpression statusFilter = equals("status", DataTypes.STRING(), "active");
    ResolvedExpression scoreFilter =
        comparison("score", DataTypes.DOUBLE(), 60.0, BuiltInFunctionDefinitions.GREATER_THAN);
    CallExpression andExpr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.AND,
            Arrays.asList(statusFilter, scoreFilter),
            DataTypes.BOOLEAN());
    assertThat(convert(andExpr)).isEqualTo("(status = 'active') AND (score > 60.0)");
  }

  @Test
  void andRejectsWhenChildIsUnsupported() {
    ResolvedExpression supported = equals("status", DataTypes.STRING(), "active");
    ResolvedExpression unsupported =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.LIKE,
            Arrays.asList(field("status", DataTypes.STRING(), 0), new ValueLiteralExpression("a%")),
            DataTypes.BOOLEAN());
    CallExpression andExpr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.AND,
            Arrays.asList(supported, unsupported),
            DataTypes.BOOLEAN());
    assertThat(convert(andExpr)).isNull();
  }

  @Test
  void isNull() {
    CallExpression expr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.IS_NULL,
            Collections.singletonList(field("name", DataTypes.STRING(), 0)),
            DataTypes.BOOLEAN());
    assertThat(convert(expr)).isEqualTo("name IS NULL");
  }

  @Test
  void isNotNull() {
    CallExpression expr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.IS_NOT_NULL,
            Collections.singletonList(field("name", DataTypes.STRING(), 0)),
            DataTypes.BOOLEAN());
    assertThat(convert(expr)).isEqualTo("name IS NOT NULL");
  }

  @Test
  void stringLiteralEscapesSingleQuote() {
    assertThat(convert(equals("name", DataTypes.STRING(), "it's"))).isEqualTo("name = 'it''s'");
  }

  @Test
  void unsafeFieldIdentifierWithDashRejected() {
    assertThat(convert(equals("bad-name", DataTypes.STRING(), "x"))).isNull();
  }

  @Test
  void unsafeFieldIdentifierStartingWithDigitRejected() {
    assertThat(convert(equals("1bad", DataTypes.STRING(), "x"))).isNull();
  }

  @Test
  void unsafeFieldIdentifierInIsNullRejected() {
    CallExpression expr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.IS_NULL,
            Collections.singletonList(field("bad name", DataTypes.STRING(), 0)),
            DataTypes.BOOLEAN());
    assertThat(convert(expr)).isNull();
  }

  @Test
  void nullLiteralComparisonRejected() {
    // "id = NULL" — the literal carries a null payload, which we refuse to push down.
    FieldReferenceExpression fieldRef = field("id", DataTypes.BIGINT(), 0);
    ValueLiteralExpression nullLiteral = new ValueLiteralExpression(null, DataTypes.BIGINT());
    CallExpression expr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.EQUALS,
            Arrays.asList(fieldRef, nullLiteral),
            DataTypes.BOOLEAN());
    assertThat(convert(expr)).isNull();
  }

  @Test
  void unsupportedOrRejected() {
    ResolvedExpression a = equals("status", DataTypes.STRING(), "active");
    ResolvedExpression b = equals("status", DataTypes.STRING(), "pending");
    CallExpression orExpr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.OR, Arrays.asList(a, b), DataTypes.BOOLEAN());
    assertThat(convert(orExpr)).isNull();
  }

  @Test
  void unsupportedNotRejected() {
    ResolvedExpression inner =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.IS_NULL,
            Collections.singletonList(field("name", DataTypes.STRING(), 0)),
            DataTypes.BOOLEAN());
    CallExpression notExpr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.NOT, Collections.singletonList(inner), DataTypes.BOOLEAN());
    assertThat(convert(notExpr)).isNull();
  }

  @Test
  void unsupportedLikeRejected() {
    CallExpression likeExpr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.LIKE,
            Arrays.asList(field("name", DataTypes.STRING(), 0), new ValueLiteralExpression("a%")),
            DataTypes.BOOLEAN());
    assertThat(convert(likeExpr)).isNull();
  }

  @Test
  void unsupportedLiteralTypeRejected() {
    // byte[] is neither String nor Number nor Boolean — must be refused.
    FieldReferenceExpression fieldRef = field("payload", DataTypes.BYTES(), 0);
    ValueLiteralExpression bytesLiteral =
        new ValueLiteralExpression(new byte[] {1, 2, 3}, DataTypes.BYTES().notNull());
    CallExpression expr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.EQUALS,
            Arrays.asList(fieldRef, bytesLiteral),
            DataTypes.BOOLEAN());
    assertThat(convert(expr)).isNull();
  }

  @Test
  void fieldOnBothSidesRejected() {
    FieldReferenceExpression a = field("id", DataTypes.BIGINT(), 0);
    FieldReferenceExpression b = field("other", DataTypes.BIGINT(), 1);
    CallExpression expr =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.EQUALS, Arrays.asList(a, b), DataTypes.BOOLEAN());
    assertThat(convert(expr)).isNull();
  }

  @Test
  void nonCallExpressionReturnsNull() {
    assertThat(convert(field("id", DataTypes.BIGINT(), 0))).isNull();
  }

  @Test
  void nanDoubleLiteralRejected() {
    // NaN cannot be represented as a Lance filter literal — the scan-side formatter must refuse
    // (same contract as the lookup path), so the filter falls back to Flink-side evaluation
    // instead of being inlined as the broken text 'col = NaN'.
    assertThat(convert(equals("score", DataTypes.DOUBLE(), Double.NaN))).isNull();
  }

  @Test
  void positiveInfinityDoubleLiteralRejected() {
    assertThat(convert(equals("score", DataTypes.DOUBLE(), Double.POSITIVE_INFINITY))).isNull();
  }

  @Test
  void negativeInfinityDoubleLiteralRejected() {
    assertThat(convert(equals("score", DataTypes.DOUBLE(), Double.NEGATIVE_INFINITY))).isNull();
  }

  @Test
  void nanFloatLiteralRejected() {
    assertThat(convert(equals("score", DataTypes.FLOAT(), Float.NaN))).isNull();
  }

  @Test
  void infinityFloatLiteralRejected() {
    assertThat(convert(equals("score", DataTypes.FLOAT(), Float.POSITIVE_INFINITY))).isNull();
  }

  // ----- helpers -----

  private static String convert(ResolvedExpression expr) {
    return LanceFilterExpressionConverter.toLanceFilter(expr);
  }

  private static FieldReferenceExpression field(String name, DataType type, int fieldIndex) {
    return new FieldReferenceExpression(name, type, 0, fieldIndex);
  }

  private static ResolvedExpression equals(String fieldName, DataType type, Object value) {
    return comparison(fieldName, type, value, BuiltInFunctionDefinitions.EQUALS);
  }

  private static ResolvedExpression comparison(
      String fieldName, DataType type, Object value, BuiltInFunctionDefinition def) {
    FieldReferenceExpression fieldRef = field(fieldName, type, 0);
    ValueLiteralExpression literal = new ValueLiteralExpression(value);
    return CallExpression.permanent(
        def, Arrays.<ResolvedExpression>asList(fieldRef, literal), DataTypes.BOOLEAN());
  }
}
