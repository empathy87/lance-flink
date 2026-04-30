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
import org.apache.flink.connector.lance.sink.LanceSinkV2;
import org.apache.flink.connector.lance.sink.LanceUpsertSinkV2;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.List;

/** Dynamic table sink for Lance datasets. */
public class LanceDynamicTableSink implements DynamicTableSink {

  private final LanceOptions options;
  private final DataType physicalDataType;
  private final List<String> primaryKeys;

  public LanceDynamicTableSink(LanceOptions options, DataType physicalDataType) {
    this(options, physicalDataType, List.of());
  }

  public LanceDynamicTableSink(
      LanceOptions options, DataType physicalDataType, List<String> primaryKeys) {
    this.options = options;
    this.physicalDataType = physicalDataType;
    this.primaryKeys = List.copyOf(primaryKeys);
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    if (primaryKeys.isEmpty() || requestedMode.containsOnly(RowKind.INSERT)) {
      return ChangelogMode.insertOnly();
    }
    return ChangelogMode.upsert();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    RowType rowType = (RowType) physicalDataType.getLogicalType();
    if (primaryKeys.isEmpty()) {
      return SinkV2Provider.of(new LanceSinkV2(options, rowType));
    }
    return SinkV2Provider.of(new LanceUpsertSinkV2(options, rowType, primaryKeys));
  }

  @Override
  public DynamicTableSink copy() {
    return new LanceDynamicTableSink(options, physicalDataType, primaryKeys);
  }

  @Override
  public String asSummaryString() {
    return "Lance Table Sink";
  }

  public LanceOptions getOptions() {
    return options;
  }

  public DataType getPhysicalDataType() {
    return physicalDataType;
  }

  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }
}
