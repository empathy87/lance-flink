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
package org.apache.flink.connector.lance.source;

import org.apache.flink.connector.lance.config.LanceOptions;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Bounded FLIP-27 source for Lance datasets. */
public class LanceSource implements Source<RowData, LanceSourceSplit, LanceSourceEnumState> {

  private static final long serialVersionUID = 1L;

  private final LanceOptions options;
  private final RowType rowType;
  private final String[] selectedColumns;
  private final String filter;

  public LanceSource(LanceOptions options, RowType rowType) {
    this(options, rowType, null, null);
  }

  public LanceSource(
      LanceOptions options,
      RowType rowType,
      @Nullable List<String> selectedColumns,
      @Nullable String filter) {
    this.options = Objects.requireNonNull(options, "options");
    this.rowType = Objects.requireNonNull(rowType, "rowType");
    this.selectedColumns = selectedColumns == null ? null : selectedColumns.toArray(new String[0]);
    this.filter = filter == null || filter.isBlank() ? null : filter.trim();
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<RowData, LanceSourceSplit> createReader(SourceReaderContext readerContext) {
    return new LanceSourceReader(
        readerContext, () -> new LanceSourceSplitReader(options, rowType, selectedColumns, filter));
  }

  @Override
  public SplitEnumerator<LanceSourceSplit, LanceSourceEnumState> createEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> enumContext) {
    return new LanceSourceEnumerator(enumContext, options);
  }

  @Override
  public SplitEnumerator<LanceSourceSplit, LanceSourceEnumState> restoreEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> enumContext, LanceSourceEnumState checkpoint) {
    return new LanceSourceEnumerator(enumContext, options, checkpoint.remainingSplits());
  }

  @Override
  public SimpleVersionedSerializer<LanceSourceSplit> getSplitSerializer() {
    return LanceSourceSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<LanceSourceEnumState> getEnumeratorCheckpointSerializer() {
    return LanceSourceEnumStateSerializer.INSTANCE;
  }

  public RowType getRowType() {
    return rowType;
  }

  public LanceOptions getOptions() {
    return options;
  }

  public String[] getSelectedColumns() {
    return selectedColumns == null ? null : Arrays.copyOf(selectedColumns, selectedColumns.length);
  }

  public String getFilter() {
    return filter;
  }
}
