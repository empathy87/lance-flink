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
package org.apache.flink.connector.lance.sink;

import org.apache.flink.connector.lance.config.LanceOptions;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/** Append-only Lance sink based on Flink Sink V2. */
public class LanceSinkV2
    implements Sink<RowData>,
        SupportsCommitter<LanceAppendCommittable>,
        SupportsWriterState<RowData, LanceWriterState>,
        SupportsPreCommitTopology<LanceAppendCommittable, LanceAppendCommittable> {

  private static final long serialVersionUID = 1L;
  private static final LanceAppendCommittableSerializer COMMITTABLE_SERIALIZER =
      new LanceAppendCommittableSerializer();

  private static final LanceWriterStateSerializer WRITER_STATE_SERIALIZER =
      new LanceWriterStateSerializer();

  private final LanceOptions options;
  private final RowType rowType;
  private final boolean overwrite;

  public LanceSinkV2(LanceOptions options, RowType rowType) {
    this(options, rowType, false);
  }

  public LanceSinkV2(LanceOptions options, RowType rowType, boolean overwrite) {
    this.options = options;
    this.rowType = rowType;
    this.overwrite = overwrite;
  }

  @SuppressWarnings("deprecation")
  @Override
  public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
    return newWriter(context.getSubtaskId(), List.of());
  }

  @Override
  public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
    return newWriter(subtaskId(context), List.of());
  }

  @Override
  public StatefulSinkWriter<RowData, LanceWriterState> restoreWriter(
      WriterInitContext context, Collection<LanceWriterState> recoveredState) throws IOException {
    return newWriter(subtaskId(context), recoveredState);
  }

  @Override
  public Committer<LanceAppendCommittable> createCommitter(CommitterInitContext context)
      throws IOException {
    return new LanceAppendCommitter(options, rowType, overwrite);
  }

  @Override
  public SimpleVersionedSerializer<LanceAppendCommittable> getCommittableSerializer() {
    return COMMITTABLE_SERIALIZER;
  }

  @Override
  public SimpleVersionedSerializer<LanceWriterState> getWriterStateSerializer() {
    return WRITER_STATE_SERIALIZER;
  }

  @Override
  public SimpleVersionedSerializer<LanceAppendCommittable> getWriteResultSerializer() {
    return COMMITTABLE_SERIALIZER;
  }

  @Override
  public DataStream<CommittableMessage<LanceAppendCommittable>> addPreCommitTopology(
      DataStream<CommittableMessage<LanceAppendCommittable>> committables) {
    // TODO: Make committer a real parallelism=1 vertex.
    return committables.global();
  }

  private static int subtaskId(WriterInitContext context) {
    return context.getTaskInfo().getIndexOfThisSubtask();
  }

  private LanceAppendWriter newWriter(int subtaskId, Collection<LanceWriterState> recoveredState) {
    return new LanceAppendWriter(options, rowType, subtaskId, recoveredState);
  }

  public LanceOptions getOptions() {
    return options;
  }

  public RowType getRowType() {
    return rowType;
  }

  public boolean isOverwrite() {
    return overwrite;
  }
}
