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

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.List;

/** Converts Flink expressions to Lance filter strings, or null if unsupported. */
final class LanceFilterExpressionConverter {

  private LanceFilterExpressionConverter() {}

  static String toLanceFilter(ResolvedExpression expression) {
    try {
      if (expression instanceof CallExpression callExpr) {
        return convertCall(callExpr);
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  private static String convertCall(CallExpression callExpr) {
    FunctionDefinition def = callExpr.getFunctionDefinition();
    List<ResolvedExpression> args = callExpr.getResolvedChildren();

    if (def == BuiltInFunctionDefinitions.EQUALS) return buildComparison(args, "=");
    if (def == BuiltInFunctionDefinitions.NOT_EQUALS) return buildComparison(args, "!=");
    if (def == BuiltInFunctionDefinitions.GREATER_THAN) return buildComparison(args, ">");
    if (def == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) return buildComparison(args, ">=");
    if (def == BuiltInFunctionDefinitions.LESS_THAN) return buildComparison(args, "<");
    if (def == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) return buildComparison(args, "<=");
    if (def == BuiltInFunctionDefinitions.AND) return buildAnd(args);
    if (def == BuiltInFunctionDefinitions.IS_NULL) return buildNullCheck(args, "IS NULL");
    if (def == BuiltInFunctionDefinitions.IS_NOT_NULL) return buildNullCheck(args, "IS NOT NULL");
    return null;
  }

  private static String buildComparison(List<ResolvedExpression> args, String operator) {
    if (args.size() != 2) {
      return null;
    }
    ResolvedExpression left = args.get(0);
    ResolvedExpression right = args.get(1);

    String fieldName;
    String value;
    if (left instanceof FieldReferenceExpression) {
      fieldName = ((FieldReferenceExpression) left).getName();
      value = extractLiteralValue(right);
    } else if (right instanceof FieldReferenceExpression) {
      fieldName = ((FieldReferenceExpression) right).getName();
      value = extractLiteralValue(left);
      operator = flipOperator(operator);
    } else {
      return null;
    }

    if (!isSafeIdentifier(fieldName) || value == null) {
      return null;
    }
    return fieldName + " " + operator + " " + value;
  }

  private static String flipOperator(String operator) {
    return switch (operator) {
      case ">" -> "<";
      case "<" -> ">";
      case ">=" -> "<=";
      case "<=" -> ">=";
      default -> operator;
    };
  }

  private static String buildAnd(List<ResolvedExpression> args) {
    if (args.size() < 2) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < args.size(); i++) {
      String converted = toLanceFilter(args.get(i));
      if (converted == null) {
        return null;
      }
      if (i > 0) {
        out.append(" AND ");
      }
      out.append('(').append(converted).append(')');
    }
    return out.toString();
  }

  private static String buildNullCheck(List<ResolvedExpression> args, String operator) {
    if (args.size() != 1 || !(args.get(0) instanceof FieldReferenceExpression)) {
      return null;
    }
    String fieldName = ((FieldReferenceExpression) args.get(0)).getName();
    if (!isSafeIdentifier(fieldName)) {
      return null;
    }
    return fieldName + " " + operator;
  }

  private static String extractLiteralValue(ResolvedExpression expr) {
    if (!(expr instanceof ValueLiteralExpression literal)) {
      return null;
    }
    Object value = literal.getValueAs(Object.class).orElse(null);
    if (value == null) {
      return null;
    }
    if (value instanceof String strValue) {
      return "'" + strValue.replace("'", "''") + "'";
    }
    if (value instanceof Number) {
      return value.toString();
    }
    if (value instanceof Boolean) {
      return value.toString().toUpperCase();
    }
    return null;
  }

  private static boolean isSafeIdentifier(String name) {
    return name != null && name.matches("[A-Za-z_][A-Za-z0-9_]*");
  }
}
