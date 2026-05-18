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
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.connector.lance.source.continuous.LanceContinuousOptions;

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.operation.Append;
import org.lance.operation.Overwrite;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.ExceptionUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** End-to-end runtime tests for the continuous Lance source. */
class ContinuousLanceSourceITCase {

  private static final RowType ROW_TYPE = RowType.of(new IntType(false));
  private static final Schema SCHEMA = LanceTypeConverter.toArrowSchema(ROW_TYPE);
  private static final RowDataConverter CONVERTER = new RowDataConverter(ROW_TYPE);
  private static final long AWAIT_TIMEOUT_MS = 30_000L;

  // Sink target. Static because Flink may serialize the SinkFunction; a static field is
  // visible to the sink wherever it runs inside the local mini-cluster JVM.
  private static final List<Integer> COLLECTED = new CopyOnWriteArrayList<>();

  @TempDir private Path tempDir;
  private String datasetUri;
  private JobClient currentJob;

  @BeforeEach
  void setUp() {
    datasetUri = tempDir.resolve("ds").toAbsolutePath().toString();
    COLLECTED.clear();
    currentJob = null;
  }

  @AfterEach
  void tearDown() throws Exception {
    if (currentJob != null) {
      try {
        currentJob.cancel().get(10, TimeUnit.SECONDS);
      } catch (Exception ignored) {
        // Job may have already finished or failed; that's fine for test cleanup.
      }
    }
  }

  // ============================ Tests ============================

  @Test
  void latestFullEmitsBaselineThenOnlyNewAppends() throws Exception {
    commitOverwrite(writeFragment(rows(1, 2)));
    StreamExecutionEnvironment env = newEnv(/* enableCheckpointing= */ false, null);
    currentJob = submit(env, "latest-full", "200ms", null, "phase-latest-full");

    awaitCollectedSize(2);
    assertThat(snapshot()).containsExactlyInAnyOrder(1, 2);

    commitAppend(writeFragment(rows(3, 4)));
    awaitCollectedSize(4);

    assertThat(snapshot()).containsExactlyInAnyOrder(1, 2, 3, 4);
  }

  @Test
  void latestStartupSkipsExistingDataAndEmitsOnlyFutureAppends() throws Exception {
    commitOverwrite(writeFragment(rows(1, 2)));
    StreamExecutionEnvironment env = newEnv(false, null);
    currentJob = submit(env, "latest", "200ms", null, "phase-latest");

    Thread.sleep(800);
    assertThat(snapshot()).isEmpty();
    assertThat(currentJob.getJobStatus().get(5, TimeUnit.SECONDS)).isEqualTo(JobStatus.RUNNING);

    commitAppend(writeFragment(rows(3, 4)));
    awaitCollectedSize(2);
    assertThat(snapshot()).containsExactlyInAnyOrder(3, 4);
  }

  @Test
  void continuousJobKeepsRunningWithNoNewData() throws Exception {
    commitOverwrite(writeFragment(rows(1, 2)));
    StreamExecutionEnvironment env = newEnv(false, null);
    currentJob = submit(env, "latest", "200ms", null, "phase-idle");

    Thread.sleep(1500);
    assertThat(snapshot()).isEmpty();
    assertThat(currentJob.getJobStatus().get(5, TimeUnit.SECONDS)).isEqualTo(JobStatus.RUNNING);
  }

  @Test
  void fragmentRemovalFailsJobWithClearMessage() throws Exception {
    commitOverwrite(writeFragment(rows(1, 2)));
    commitAppend(writeFragment(rows(3, 4)));

    StreamExecutionEnvironment env = newEnv(false, null);
    currentJob = submit(env, "latest", "200ms", null, "phase-removal");

    // Lance recycles fragment id 0 across Overwrites, so we need a fragment-count drop for the
    // removal to be visible to fragment-diff; the test seeded two fragments above for that.
    Thread.sleep(500);
    commitOverwrite(writeFragment(rows(99)));

    Throwable failure =
        catchThrowable(
            () -> currentJob.getJobExecutionResult().get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    currentJob = null; // cleanup already complete via failure
    assertThat(failure).isNotNull();
    String trace = ExceptionUtils.stringifyException(failure);
    assertThat(trace).contains("fragment removals");
    assertThat(trace).contains("compaction");
  }

  @Test
  void checkpointRestoreSurvivesWithoutDuplicates() throws Exception {
    Path savepointDir = tempDir.resolve("savepoints").toAbsolutePath();
    Files.createDirectories(savepointDir);
    String savepointDirUri = "file:" + savepointDir;

    commitOverwrite(writeFragment(rows(1, 2)));

    // ---------- Phase 1: emit baseline + one append, take a savepoint, stop. ----------
    StreamExecutionEnvironment env1 = newEnv(true, savepointDirUri);
    JobClient jc1 = submit(env1, "latest-full", "200ms", null, "phase1");
    try {
      awaitCollectedSize(2);
      commitAppend(writeFragment(rows(3, 4)));
      awaitCollectedSize(4);
      assertThat(snapshot()).containsExactlyInAnyOrder(1, 2, 3, 4);
    } catch (Throwable t) {
      jc1.cancel();
      throw t;
    }

    String savepointPath =
        jc1.stopWithSavepoint(false, savepointDirUri, SavepointFormatType.CANONICAL)
            .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertThat(savepointPath).isNotBlank();

    // ---------- Out-of-band commit while the job is down. ----------
    commitAppend(writeFragment(rows(5, 6)));

    COLLECTED.clear();

    // ---------- Phase 2: restart from savepoint, expect only the post-savepoint rows. ----------
    // The savepoint path goes via Configuration because the embedded LocalExecutor reads
    // savepoint config from Configuration, not from StreamGraph.setSavepointRestoreSettings.
    Configuration conf2 = new Configuration();
    SavepointRestoreSettings.toConfiguration(
        SavepointRestoreSettings.forPath(savepointPath), conf2);
    StreamExecutionEnvironment env2 = newEnv(true, savepointDirUri, conf2);
    currentJob = submit(env2, "latest-full", "200ms", null, "phase2");

    awaitCollectedSize(2);
    assertThat(snapshot()).containsExactlyInAnyOrder(5, 6);
  }

  // ============================ Helpers ============================

  private static StreamExecutionEnvironment newEnv(
      boolean enableCheckpointing, String checkpointStorageUri) {
    return newEnv(enableCheckpointing, checkpointStorageUri, new Configuration());
  }

  private static StreamExecutionEnvironment newEnv(
      boolean enableCheckpointing, String checkpointStorageUri, Configuration conf) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    env.setParallelism(1);
    if (enableCheckpointing) {
      env.enableCheckpointing(200);
      if (checkpointStorageUri != null) {
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorageUri);
      }
    }
    return env;
  }

  private StreamGraph buildStreamGraph(
      StreamExecutionEnvironment env, String startupMode, String discoveryInterval, Long limit) {
    Configuration cfg = new Configuration();
    cfg.setString("scan.startup-mode", startupMode);
    cfg.setString("continuous.discovery-interval", discoveryInterval);
    LanceContinuousOptions cOpts = LanceContinuousOptions.fromConfig(cfg);
    LanceOptions lanceOpts = LanceOptions.builder().path(datasetUri).build();
    ContinuousLanceSource source =
        new ContinuousLanceSource(lanceOpts, ROW_TYPE, null, null, cOpts, limit);

    DataStreamSource<RowData> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "lance-continuous");
    stream.addSink(new CollectorSink());
    return env.getStreamGraph();
  }

  private JobClient submit(
      StreamExecutionEnvironment env,
      String startupMode,
      String discoveryInterval,
      Long limit,
      String jobName)
      throws Exception {
    StreamGraph sg = buildStreamGraph(env, startupMode, discoveryInterval, limit);
    sg.setJobName(jobName);
    return env.executeAsync(sg);
  }

  /** Static sink that appends int payloads into {@link #COLLECTED}. */
  static class CollectorSink implements SinkFunction<RowData> {
    @Override
    public void invoke(RowData value, Context ctx) {
      COLLECTED.add(value.getInt(0));
    }
  }

  private static List<Integer> snapshot() {
    return new ArrayList<>(COLLECTED);
  }

  private static void awaitCollectedSize(int expectedSize)
      throws InterruptedException, TimeoutException {
    long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      if (COLLECTED.size() >= expectedSize) {
        return;
      }
      Thread.sleep(50);
    }
    throw new TimeoutException(
        "Timed out waiting for "
            + expectedSize
            + " collected rows; have "
            + COLLECTED.size()
            + ": "
            + COLLECTED);
  }

  private static List<RowData> rows(int... values) {
    List<RowData> out = new ArrayList<>();
    for (int v : values) {
      out.add(GenericRowData.of(v));
    }
    return out;
  }

  private List<FragmentMetadata> writeFragment(List<RowData> data) {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, alloc)) {
      CONVERTER.toVectorSchemaRoot(data, root);
      return Fragment.write().datasetUri(datasetUri).allocator(alloc).data(root).execute();
    }
  }

  private void commitOverwrite(List<FragmentMetadata> fragments) {
    try (BufferAllocator alloc = new RootAllocator();
        Transaction tx =
            new Transaction.Builder()
                .operation(Overwrite.builder().fragments(fragments).schema(SCHEMA).build())
                .build();
        Dataset ignored = new CommitBuilder(datasetUri, alloc).execute(tx)) {}
  }

  private void commitAppend(List<FragmentMetadata> fragments) {
    try (BufferAllocator alloc = new RootAllocator();
        Transaction tx =
            new Transaction.Builder()
                .operation(Append.builder().fragments(fragments).build())
                .build();
        Dataset ignored = new CommitBuilder(datasetUri, alloc).execute(tx)) {}
  }
}
