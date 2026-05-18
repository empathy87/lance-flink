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
import org.apache.flink.connector.lance.source.continuous.ContinuousLanceSourceEnumerator;
import org.apache.flink.connector.lance.source.continuous.LanceContinuousEnumState;
import org.apache.flink.connector.lance.source.continuous.LanceContinuousEnumStateSerializer;
import org.apache.flink.connector.lance.source.continuous.LanceContinuousOptions;

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

import java.util.List;
import java.util.Objects;

/** Unbounded Lance source backed by continuous fragment discovery. */
public class ContinuousLanceSource
    implements Source<RowData, LanceSourceSplit, LanceContinuousEnumState> {

  private static final long serialVersionUID = 1L;

  private final LanceOptions options;
  private final RowType rowType;
  private final String[] selectedColumns;
  private final String filter;
  private final LanceContinuousOptions continuousOptions;
  private final Long limit;

  public ContinuousLanceSource(
      LanceOptions options,
      RowType rowType,
      @Nullable List<String> selectedColumns,
      @Nullable String filter,
      LanceContinuousOptions continuousOptions,
      @Nullable Long limit) {
    this.options = Objects.requireNonNull(options, "options");
    this.rowType = Objects.requireNonNull(rowType, "rowType");
    this.selectedColumns = selectedColumns == null ? null : selectedColumns.toArray(new String[0]);
    this.filter = filter == null || filter.isBlank() ? null : filter.trim();
    this.continuousOptions = Objects.requireNonNull(continuousOptions, "continuousOptions");
    this.limit = limit;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<RowData, LanceSourceSplit> createReader(SourceReaderContext readerContext) {
    return new LanceSourceReader(
        readerContext,
        () -> new LanceSourceSplitReader(options, rowType, selectedColumns, filter, limit));
  }

  @Override
  public SplitEnumerator<LanceSourceSplit, LanceContinuousEnumState> createEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> enumContext) {
    return new ContinuousLanceSourceEnumerator(enumContext, options, continuousOptions);
  }

  @Override
  public SplitEnumerator<LanceSourceSplit, LanceContinuousEnumState> restoreEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> enumContext, LanceContinuousEnumState checkpoint) {
    return new ContinuousLanceSourceEnumerator(enumContext, options, continuousOptions, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<LanceSourceSplit> getSplitSerializer() {
    return LanceSourceSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<LanceContinuousEnumState> getEnumeratorCheckpointSerializer() {
    return LanceContinuousEnumStateSerializer.INSTANCE;
  }
}
