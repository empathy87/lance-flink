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
import org.apache.flink.connector.lance.converter.LanceTypeConverter;

import org.lance.Dataset;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL-level checks that exercise V2-specific behaviors that aren't covered by the legacy-style
 * integration tests: parallelism &gt; 1 and the DataStream {@code sinkTo} entry point.
 */
class LanceSinkV2SqlITCase {

  @TempDir Path tempDir;

  @Test
  void testReinsertingTheSameRowsAppendsRatherThanDeduplicates() throws Exception {
    String datasetUri = tempDir.resolve("dup-dataset").toUri().toString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String tableDdl =
        "CREATE TABLE t (id BIGINT, name STRING) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(datasetUri)
            + ")";
    tableEnv.executeSql(tableDdl);

    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await();
    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await();

    // Append is not idempotent on rows — a guard against accidentally turning append into upsert.
    assertThat(rowCount(datasetUri)).isEqualTo(6L);
  }

  @Test
  void testInsertOverwriteReplacesExistingRows() throws Exception {
    String datasetUri = tempDir.resolve("overwrite-dataset").toUri().toString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String tableDdl =
        "CREATE TABLE t (id BIGINT, name STRING) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(datasetUri)
            + ")";
    tableEnv.executeSql(tableDdl);

    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await();
    tableEnv.executeSql("INSERT OVERWRITE t VALUES (10, 'x'), (20, 'y')").await();

    assertThat(rowCount(datasetUri)).isEqualTo(2L);
    assertThat(readIdColumn(datasetUri)).containsExactlyInAnyOrder(10L, 20L);
  }

  @Test
  void testTruncateTableEmptiesDataset() throws Exception {
    String datasetUri = tempDir.resolve("truncate-dataset").toUri().toString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String tableDdl =
        "CREATE TABLE t (id BIGINT, name STRING) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(datasetUri)
            + ")";
    tableEnv.executeSql(tableDdl);

    tableEnv.executeSql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await();
    assertThat(rowCount(datasetUri)).isEqualTo(3L);

    tableEnv.executeSql("TRUNCATE TABLE t");
    assertThat(rowCount(datasetUri)).isEqualTo(0L);

    // Truncated dataset still accepts new appends — schema survives the empty Overwrite.
    tableEnv.executeSql("INSERT INTO t VALUES (10, 'x')").await();
    assertThat(rowCount(datasetUri)).isEqualTo(1L);
    assertThat(readIdColumn(datasetUri)).containsExactly(10L);
  }

  @Test
  void testTruncateTableEmptiesPrimaryKeyTable() throws Exception {
    String datasetUri = tempDir.resolve("truncate-pk").toUri().toString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String tableDdl =
        "CREATE TABLE t (id BIGINT, name STRING, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(datasetUri)
            + ")";
    tableEnv.executeSql(tableDdl);

    // Seed via INSERT OVERWRITE: LanceUpsertCommitter requires the dataset to already exist.
    tableEnv.executeSql("INSERT OVERWRITE t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await();
    assertThat(rowCount(datasetUri)).isEqualTo(3L);

    tableEnv.executeSql("TRUNCATE TABLE t");
    assertThat(rowCount(datasetUri)).isEqualTo(0L);

    // Truncate must leave the dataset (schema + PK metadata) on disk so subsequent upserts
    // can Dataset.open() it without going through INSERT OVERWRITE again.
    tableEnv.executeSql("INSERT INTO t VALUES (10, 'x'), (20, 'y')").await();
    assertThat(rowCount(datasetUri)).isEqualTo(2L);
    assertThat(readIdColumn(datasetUri)).containsExactlyInAnyOrder(10L, 20L);
  }

  @Test
  void testInsertOverwriteRejectedInStreamingMode() {
    String datasetUri = tempDir.resolve("overwrite-stream").toUri().toString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String tableDdl =
        "CREATE TABLE t (id BIGINT, name STRING) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(datasetUri)
            + ")";
    tableEnv.executeSql(tableDdl);

    assertThat(
            org.assertj.core.api.Assertions.catchThrowable(
                () -> tableEnv.executeSql("INSERT OVERWRITE t VALUES (1, 'x')")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Lance doesn't support streaming INSERT OVERWRITE.");
  }

  @Test
  void testInsertOverwriteOnPrimaryKeyTableTruncatesAndAppends() throws Exception {
    String datasetUri = tempDir.resolve("overwrite-pk").toUri().toString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String tableDdl =
        "CREATE TABLE t (id BIGINT, name STRING, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(datasetUri)
            + ")";
    tableEnv.executeSql(tableDdl);

    tableEnv.executeSql("INSERT OVERWRITE t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await();
    tableEnv.executeSql("INSERT OVERWRITE t VALUES (10, 'x'), (20, 'y')").await();

    assertThat(rowCount(datasetUri)).isEqualTo(2L);
    assertThat(readIdColumn(datasetUri)).containsExactlyInAnyOrder(10L, 20L);
  }

  @Test
  void testInsertOverwriteOnPrimaryKeyTableDeduplicatesByKey() throws Exception {
    String datasetUri = tempDir.resolve("overwrite-pk-dedup").toUri().toString();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    String tableDdl =
        "CREATE TABLE t (id BIGINT, name STRING, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector' = 'lance', "
            + "'path' = "
            + sql(datasetUri)
            + ")";
    tableEnv.executeSql(tableDdl);

    tableEnv
        .executeSql("INSERT OVERWRITE t VALUES (1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (3, 'e')")
        .await();

    assertThat(rowCount(datasetUri)).isEqualTo(3L);
    assertThat(readIdColumn(datasetUri)).containsExactlyInAnyOrder(1L, 2L, 3L);
  }

  @Test
  void testDataStreamSinkToWritesRowsThroughV2WithMultipleWriters() throws Exception {
    String datasetUri = tempDir.resolve("ds-dataset").toUri().toString();
    LanceOptions options = LanceOptions.builder().path(datasetUri).writeBatchSize(8).build();
    RowType rowType =
        new RowType(
            List.of(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType())));

    Configuration conf = new Configuration();
    conf.set(CoreOptions.DEFAULT_PARALLELISM, 2);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);

    List<RowData> rows =
        IntStream.range(0, 64)
            .mapToObj(
                i -> {
                  GenericRowData r = new GenericRowData(2);
                  r.setField(0, (long) i);
                  r.setField(1, StringData.fromString("ds-" + i));
                  return (RowData) r;
                })
            .collect(java.util.stream.Collectors.toList());

    env.fromCollection(rows)
        .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(RowData.class))
        // rebalance so writer subtasks both receive rows; each emits committables that the
        // parallelism-1 committer aggregates.
        .rebalance()
        .sinkTo(new LanceSinkV2(options, rowType));
    env.execute("lance-sink-v2-ds-test");

    // Row count must be exact (no duplication from multiple subtasks committing independently).
    assertThat(rowCount(datasetUri)).isEqualTo(64L);

    // And every id must appear exactly once.
    Set<Long> ids = readIdColumn(datasetUri);
    assertThat(ids).hasSize(64).containsAll(LongRange.of(0, 64));
  }

  /**
   * The committer assumes the dataset already exists for primary-key sinks — here we exercise the
   * V2 upsert sink directly via DataStream by seeding the dataset first, then running the upsert
   * pipeline.
   */
  @Test
  void testUpsertSinkV2DataStream() throws Exception {
    String datasetUri = tempDir.resolve("ds-upsert").toUri().toString();
    LanceOptions options = LanceOptions.builder().path(datasetUri).writeBatchSize(8).build();
    RowType rowType =
        new RowType(
            List.of(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType())));

    seedDatasetForUpsert(options, rowType, 10);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);
    env.setParallelism(2);

    List<RowData> upserts = new ArrayList<>();
    upserts.add(SinkTestRows.tagged(org.apache.flink.types.RowKind.UPDATE_AFTER, 0L, "updated-0"));
    upserts.add(SinkTestRows.tagged(org.apache.flink.types.RowKind.UPDATE_AFTER, 5L, "updated-5"));
    upserts.add(SinkTestRows.tagged(org.apache.flink.types.RowKind.INSERT, 100L, "new-100"));
    upserts.add(SinkTestRows.tagged(org.apache.flink.types.RowKind.DELETE, 1L, "to-delete"));

    env.fromCollection(upserts)
        .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(RowData.class))
        .sinkTo(new LanceUpsertSinkV2(options, rowType, List.of("id")));
    env.execute("lance-upsert-sink-v2-ds-test");

    // 10 seeded + 1 new (id=100) - 1 deleted (id=1) = 10
    assertThat(rowCount(datasetUri)).isEqualTo(10L);
  }

  private static void seedDatasetForUpsert(LanceOptions options, RowType rowType, int rowCount)
      throws Exception {
    LanceAppendWriter writer =
        new LanceAppendWriter(options, rowType, 0, java.util.Collections.emptyList());
    java.util.Collection<LanceAppendCommittable> committables;
    try {
      for (long i = 0; i < rowCount; i++) {
        writer.write(SinkTestRows.simple(i, "seed-" + i), null);
      }
      writer.flush(false);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }
    LanceAppendCommitter committer = new LanceAppendCommitter(options, rowType);
    try {
      java.util.List<Committer.CommitRequest<LanceAppendCommittable>> requests = new ArrayList<>();
      for (LanceAppendCommittable c : committables) {
        requests.add(new StubCommitRequest<>(c));
      }
      committer.commit(requests);
    } finally {
      committer.close();
    }
    // Ensure schema converter doesn't get GC'd lazily.
    LanceTypeConverter.toArrowSchema(rowType);
  }

  private static long rowCount(String uri) {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset dataset = Dataset.open(uri, allocator)) {
      return dataset.countRows();
    }
  }

  private static Set<Long> readIdColumn(String uri) throws Exception {
    Set<Long> ids = new HashSet<>();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Dataset dataset = Dataset.open(uri, allocator);
        org.lance.ipc.LanceScanner scanner = dataset.newScan();
        org.apache.arrow.vector.ipc.ArrowReader reader = scanner.scanBatches()) {
      while (reader.loadNextBatch()) {
        org.apache.arrow.vector.VectorSchemaRoot root = reader.getVectorSchemaRoot();
        org.apache.arrow.vector.BigIntVector idVec =
            (org.apache.arrow.vector.BigIntVector) root.getVector("id");
        for (int i = 0; i < root.getRowCount(); i++) {
          ids.add(idVec.get(i));
        }
      }
    }
    return ids;
  }

  /** Tiny helper for "all longs in [from, to)". */
  private static final class LongRange {
    static List<Long> of(long fromInclusive, long toExclusive) {
      List<Long> out = new ArrayList<>((int) (toExclusive - fromInclusive));
      for (long i = fromInclusive; i < toExclusive; i++) {
        out.add(i);
      }
      return out;
    }
  }

  private static String sql(String value) {
    return "'" + value.replace("'", "''") + "'";
  }
}
