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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Upsert Lance sink based on Flink Sink V2. */
public class LanceUpsertSinkV2
    implements Sink<RowData>,
        SupportsCommitter<LanceUpsertCommittable>,
        SupportsPreWriteTopology<RowData>,
        SupportsPreCommitTopology<LanceUpsertCommittable, LanceUpsertCommittable> {

  private static final long serialVersionUID = 1L;
  private static final LanceUpsertCommittableSerializer COMMITTABLE_SERIALIZER =
      new LanceUpsertCommittableSerializer();

  private final LanceOptions options;
  private final RowType rowType;
  private final List<String> primaryKeys;
  private final int[] primaryKeyIndexes;
  private final boolean overwrite;

  public LanceUpsertSinkV2(LanceOptions options, RowType rowType, List<String> primaryKeys) {
    this(options, rowType, primaryKeys, false);
  }

  public LanceUpsertSinkV2(
      LanceOptions options, RowType rowType, List<String> primaryKeys, boolean overwrite) {
    if (primaryKeys == null || primaryKeys.isEmpty()) {
      throw new IllegalArgumentException("LanceUpsertSinkV2 requires at least one primary key");
    }
    this.options = options;
    this.rowType = rowType;
    this.primaryKeys = List.copyOf(primaryKeys);
    this.primaryKeyIndexes = resolvePrimaryKeyIndexes(rowType, this.primaryKeys);
    this.overwrite = overwrite;
  }

  @SuppressWarnings("deprecation")
  @Override
  public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
    return newWriter(context.getSubtaskId());
  }

  @Override
  public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
    return newWriter(context.getTaskInfo().getIndexOfThisSubtask());
  }

  private LanceUpsertWriter newWriter(int subtaskId) {
    return new LanceUpsertWriter(options, rowType, subtaskId, primaryKeyIndexes.clone());
  }

  @Override
  public Committer<LanceUpsertCommittable> createCommitter(CommitterInitContext context)
      throws IOException {
    return new LanceUpsertCommitter(options, rowType, primaryKeys, overwrite);
  }

  @Override
  public SimpleVersionedSerializer<LanceUpsertCommittable> getCommittableSerializer() {
    return COMMITTABLE_SERIALIZER;
  }

  @Override
  public SimpleVersionedSerializer<LanceUpsertCommittable> getWriteResultSerializer() {
    return COMMITTABLE_SERIALIZER;
  }

  @Override
  public DataStream<RowData> addPreWriteTopology(DataStream<RowData> input) {
    return input.keyBy(new PrimaryKeySelector(rowType, primaryKeyIndexes), Types.INT);
  }

  @Override
  public DataStream<CommittableMessage<LanceUpsertCommittable>> addPreCommitTopology(
      DataStream<CommittableMessage<LanceUpsertCommittable>> committables) {
    // TODO: Make committer a real parallelism=1 vertex.
    return committables.global();
  }

  public LanceOptions getOptions() {
    return options;
  }

  public RowType getRowType() {
    return rowType;
  }

  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  private static int[] resolvePrimaryKeyIndexes(RowType rowType, List<String> primaryKeys) {
    List<String> fieldNames = rowType.getFieldNames();
    int[] indexes = new int[primaryKeys.size()];
    for (int i = 0; i < primaryKeys.size(); i++) {
      int idx = fieldNames.indexOf(primaryKeys.get(i));
      if (idx < 0) {
        throw new IllegalArgumentException(
            "Primary key column '" + primaryKeys.get(i) + "' not found in row type " + rowType);
      }
      indexes[i] = idx;
    }
    return indexes;
  }

  /** Routes equal primary keys to the same writer subtask. */
  static final class PrimaryKeySelector implements KeySelector<RowData, Integer> {
    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final int[] primaryKeyIndexes;
    private transient RowData.FieldGetter[] fieldGetters;

    PrimaryKeySelector(RowType rowType, int[] primaryKeyIndexes) {
      this.rowType = rowType;
      this.primaryKeyIndexes = primaryKeyIndexes.clone();
    }

    @Override
    public Integer getKey(RowData row) {
      int hash = 1;
      for (RowData.FieldGetter getter : fieldGetters()) {
        hash = 31 * hash + Objects.hashCode(getter.getFieldOrNull(row));
      }
      return hash;
    }

    private RowData.FieldGetter[] fieldGetters() {
      if (fieldGetters == null) {
        fieldGetters = new RowData.FieldGetter[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
          int pos = primaryKeyIndexes[i];
          LogicalType type = rowType.getTypeAt(pos);
          fieldGetters[i] = RowData.createFieldGetter(type, pos);
        }
      }
      return fieldGetters;
    }

    int[] primaryKeyIndexes() {
      return primaryKeyIndexes.clone();
    }
  }
}
