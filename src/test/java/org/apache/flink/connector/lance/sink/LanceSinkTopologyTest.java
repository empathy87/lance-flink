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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the writer/committer topology mandated by the Sink V2 implementations: writers run at
 * any parallelism; committables are forwarded to a single committer via {@code .global()}; the
 * upsert sink additionally key-shuffles input on the primary key so future per-key dedup logic sees
 * all records for a key at one writer subtask.
 */
class LanceSinkTopologyTest {

  private static final RowType ROW_TYPE =
      new RowType(
          List.of(
              new RowType.RowField("id", new BigIntType()),
              new RowType.RowField("name", new VarCharType())));

  @Test
  void testAppendSinkPreCommitTopologyForwardsViaGlobalPartitioner() {
    LanceSinkV2 sink = new LanceSinkV2(LanceOptions.builder().path("/tmp/x").build(), ROW_TYPE);
    DataStream<CommittableMessage<LanceAppendCommittable>> input = committableStream();

    DataStream<CommittableMessage<LanceAppendCommittable>> output =
        sink.addPreCommitTopology(input);

    assertGlobalPartitioned(output);
    assertThat(sink.getWriteResultSerializer())
        .isInstanceOf(LanceAppendCommittableSerializer.class);
  }

  @Test
  void testUpsertSinkPreCommitTopologyForwardsViaGlobalPartitioner() {
    LanceUpsertSinkV2 sink =
        new LanceUpsertSinkV2(
            LanceOptions.builder().path("/tmp/x").build(), ROW_TYPE, List.of("id"));
    DataStream<CommittableMessage<LanceUpsertCommittable>> input = upsertCommittableStream();

    DataStream<CommittableMessage<LanceUpsertCommittable>> output =
        sink.addPreCommitTopology(input);

    assertGlobalPartitioned(output);
    assertThat(sink.getWriteResultSerializer())
        .isInstanceOf(LanceUpsertCommittableSerializer.class);
  }

  @Test
  void testUpsertSinkPreWriteTopologyKeysOnPrimaryKey() {
    LanceUpsertSinkV2 sink =
        new LanceUpsertSinkV2(
            LanceOptions.builder().path("/tmp/x").build(), ROW_TYPE, List.of("id"));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<RowData> input =
        env.fromCollection(
            List.of(row(1L, "a"), row(1L, "b"), row(2L, "c")), TypeInformation.of(RowData.class));

    DataStream<RowData> output = sink.addPreWriteTopology(input);

    assertThat(output).isInstanceOf(KeyedStream.class);
  }

  @Test
  void testPrimaryKeySelectorIsStableAcrossEqualKeys() throws Exception {
    LanceUpsertSinkV2.PrimaryKeySelector selector =
        new LanceUpsertSinkV2.PrimaryKeySelector(ROW_TYPE, new int[] {0});

    int sameKeyA = selector.getKey(row(42L, "first"));
    int sameKeyB = selector.getKey(row(42L, "second"));
    int otherKey = selector.getKey(row(43L, "third"));

    assertThat(sameKeyA).isEqualTo(sameKeyB);
    assertThat(sameKeyA).isNotEqualTo(otherKey);
    assertThat(selector.primaryKeyIndexes()).containsExactly(0);
  }

  @Test
  void testCompoundPrimaryKeySelector() throws Exception {
    RowType compound =
        new RowType(
            List.of(
                new RowType.RowField("k1", new BigIntType()),
                new RowType.RowField("k2", new VarCharType()),
                new RowType.RowField("payload", new VarCharType())));
    LanceUpsertSinkV2.PrimaryKeySelector selector =
        new LanceUpsertSinkV2.PrimaryKeySelector(compound, new int[] {0, 1});

    int keyA = selector.getKey(compoundRow(7L, "alpha", "x"));
    int keyB = selector.getKey(compoundRow(7L, "alpha", "y"));
    int keyC = selector.getKey(compoundRow(7L, "beta", "x"));

    assertThat(keyA).isEqualTo(keyB);
    assertThat(keyA).isNotEqualTo(keyC);
  }

  @Test
  void testUpsertSinkRejectsPrimaryKeyNotInRowType() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                new LanceUpsertSinkV2(
                    LanceOptions.builder().path("/tmp/x").build(), ROW_TYPE, List.of("missing")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing");
  }

  private static void assertGlobalPartitioned(DataStream<?> output) {
    assertThat(output.getTransformation()).isInstanceOf(PartitionTransformation.class);
    PartitionTransformation<?> partitionTx =
        (PartitionTransformation<?>) output.getTransformation();
    assertThat(partitionTx.getPartitioner()).isInstanceOf(GlobalPartitioner.class);
  }

  private static DataStream<CommittableMessage<LanceAppendCommittable>> committableStream() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    return env.fromCollection(
        List.of(),
        TypeInformation.of(
            new org.apache.flink.api.common.typeinfo.TypeHint<
                CommittableMessage<LanceAppendCommittable>>() {}));
  }

  private static DataStream<CommittableMessage<LanceUpsertCommittable>> upsertCommittableStream() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    return env.fromCollection(
        List.of(),
        TypeInformation.of(
            new org.apache.flink.api.common.typeinfo.TypeHint<
                CommittableMessage<LanceUpsertCommittable>>() {}));
  }

  private static RowData row(long id, String name) {
    GenericRowData r = new GenericRowData(2);
    r.setField(0, id);
    r.setField(1, StringData.fromString(name));
    return r;
  }

  private static RowData compoundRow(long k1, String k2, String payload) {
    GenericRowData r = new GenericRowData(3);
    r.setField(0, k1);
    r.setField(1, StringData.fromString(k2));
    r.setField(2, StringData.fromString(payload));
    return r;
  }
}
