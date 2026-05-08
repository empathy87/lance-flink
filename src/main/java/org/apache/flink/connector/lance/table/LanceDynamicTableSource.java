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
import org.apache.flink.connector.lance.source.LanceSource;
import org.apache.flink.connector.lance.source.scan.LanceScanOptions;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Dynamic table source for Lance scans. */
// TODO: Add aggregate pushdown when Lance aggregate conversion and runtime support are ready.
public class LanceDynamicTableSource
    implements ScanTableSource,
        SupportsProjectionPushDown,
        SupportsFilterPushDown,
        SupportsLimitPushDown {

  private final LanceOptions options;
  private final LanceScanOptions scanOptions;
  private final DataType physicalDataType;
  private int[] projectedFieldIndices;
  private DataType producedDataType;
  private String pushedFilter;
  private Long pushedLimit;

  public LanceDynamicTableSource(LanceOptions options, DataType physicalDataType) {
    this(options, LanceScanOptions.latest(), physicalDataType);
  }

  public LanceDynamicTableSource(
      LanceOptions options, LanceScanOptions scanOptions, DataType physicalDataType) {
    this.options = options;
    this.scanOptions = scanOptions == null ? LanceScanOptions.latest() : scanOptions;
    this.physicalDataType = physicalDataType;
    this.projectedFieldIndices = null;
    this.producedDataType = physicalDataType;
    this.pushedFilter = null;
    this.pushedLimit = null;
  }

  private LanceDynamicTableSource(LanceDynamicTableSource source) {
    this.options = source.options;
    this.scanOptions = source.scanOptions;
    this.physicalDataType = source.physicalDataType;
    this.projectedFieldIndices =
        source.projectedFieldIndices == null
            ? null
            : Arrays.copyOf(source.projectedFieldIndices, source.projectedFieldIndices.length);
    this.producedDataType = source.producedDataType;
    this.pushedFilter = source.pushedFilter;
    this.pushedLimit = source.pushedLimit;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    RowType sourceRowType = (RowType) physicalDataType.getLogicalType();
    RowType outputRowType = (RowType) producedDataType.getLogicalType();

    List<String> projectedColumnNames = null;
    if (projectedFieldIndices != null) {
      projectedColumnNames =
          Arrays.stream(projectedFieldIndices)
              .mapToObj(i -> sourceRowType.getFieldNames().get(i))
              .collect(Collectors.toList());
    }

    return SourceProvider.of(
        new LanceSource(
            options, outputRowType, projectedColumnNames, pushedFilter, scanOptions, pushedLimit));
  }

  @Override
  public DynamicTableSource copy() {
    return new LanceDynamicTableSource(this);
  }

  @Override
  public String asSummaryString() {
    return "Lance Table Source";
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projectedFields, DataType producedDataType) {
    this.projectedFieldIndices =
        Arrays.stream(projectedFields)
            .mapToInt(
                fieldPath -> {
                  if (fieldPath.length != 1) {
                    throw new IllegalArgumentException("Nested projection is not supported.");
                  }
                  return fieldPath[0];
                })
            .toArray();

    this.producedDataType = producedDataType;
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    List<ResolvedExpression> acceptedFilters = new ArrayList<>();
    List<ResolvedExpression> remainingFilters = new ArrayList<>();
    List<String> pushedFilters = new ArrayList<>();

    for (ResolvedExpression filter : filters) {
      String lanceFilter = LanceFilterExpressionConverter.toLanceFilter(filter);
      if (lanceFilter != null) {
        pushedFilters.add(lanceFilter);
        acceptedFilters.add(filter);
      } else {
        remainingFilters.add(filter);
      }
    }
    this.pushedFilter = pushedFilters.isEmpty() ? null : String.join(" AND ", pushedFilters);

    return Result.of(acceptedFilters, remainingFilters);
  }

  @Override
  public void applyLimit(long limit) {
    this.pushedLimit = limit;
  }

  public LanceOptions getOptions() {
    return options;
  }

  public LanceScanOptions getScanOptions() {
    return scanOptions;
  }

  public DataType getPhysicalDataType() {
    return physicalDataType;
  }
}
