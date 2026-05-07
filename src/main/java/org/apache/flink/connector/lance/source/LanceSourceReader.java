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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.table.data.RowData;

import java.util.Map;
import java.util.function.Supplier;

/** Source reader for Lance splits. */
public class LanceSourceReader
    extends SingleThreadMultiplexSourceReaderBase<
        RowData, RowData, LanceSourceSplit, LanceSourceSplitState> {

  private static final RecordEmitter<RowData, RowData, LanceSourceSplitState> EMITTER =
      (record, output, splitState) -> {
        output.collect(record);
        splitState.setRecordsToSkip(splitState.getRecordsToSkip() + 1);
      };

  public LanceSourceReader(
      SourceReaderContext context,
      Supplier<SplitReader<RowData, LanceSourceSplit>> splitReaderSupplier) {
    super(splitReaderSupplier, EMITTER, context.getConfiguration(), context);
  }

  @Override
  public void start() {
    if (getNumberOfCurrentlyAssignedSplits() == 0) {
      context.sendSplitRequest();
    }
  }

  @Override
  protected void onSplitFinished(Map<String, LanceSourceSplitState> finishedSplitStates) {
    context.sendSplitRequest();
  }

  @Override
  protected LanceSourceSplitState initializedState(LanceSourceSplit split) {
    return new LanceSourceSplitState(split);
  }

  @Override
  protected LanceSourceSplit toSplitType(String splitId, LanceSourceSplitState splitState) {
    return splitState.toSplit();
  }
}
