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
package org.apache.flink.connector.lance.table.procedures;

import org.apache.flink.connector.lance.table.LanceNamespaceCatalog;

import org.lance.Dataset;
import org.lance.compaction.Compaction;
import org.lance.compaction.CompactionMetrics;
import org.lance.compaction.CompactionOptions;
import org.lance.compaction.CompactionPlan;
import org.lance.compaction.CompactionTask;
import org.lance.compaction.RewriteResult;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Compacts a Lance table synchronously in the caller JVM. */
public class CompactProcedure extends AbstractLanceProcedure {

  public CompactProcedure(LanceNamespaceCatalog catalog) {
    super(catalog);
  }

  @ProcedureHint(
      argument = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(
            name = "target_rows_per_fragment",
            type = @DataTypeHint("BIGINT"),
            isOptional = true),
        @ArgumentHint(
            name = "max_rows_per_group",
            type = @DataTypeHint("BIGINT"),
            isOptional = true),
        @ArgumentHint(
            name = "max_bytes_per_file",
            type = @DataTypeHint("BIGINT"),
            isOptional = true),
        @ArgumentHint(
            name = "materialize_deletions",
            type = @DataTypeHint("BOOLEAN"),
            isOptional = true),
        @ArgumentHint(
            name = "materialize_deletions_threshold",
            type = @DataTypeHint("FLOAT"),
            isOptional = true),
        @ArgumentHint(name = "num_threads", type = @DataTypeHint("INT"), isOptional = true),
        @ArgumentHint(name = "batch_size", type = @DataTypeHint("INT"), isOptional = true)
      },
      output =
          @DataTypeHint(
              "ROW<fragments_added BIGINT, fragments_removed BIGINT, files_added BIGINT, files_removed BIGINT>"))
  public Row[] call(
      ProcedureContext context,
      String table,
      @Nullable Long targetRowsPerFragment,
      @Nullable Long maxRowsPerGroup,
      @Nullable Long maxBytesPerFile,
      @Nullable Boolean materializeDeletions,
      @Nullable Float materializeDeletionsThreshold,
      @Nullable Integer numThreads,
      @Nullable Integer batchSize) {
    LanceProcedureUtils.validateNonEmpty(table, "`table`");
    CompactionOptions options =
        buildOptions(
            targetRowsPerFragment,
            maxRowsPerGroup,
            maxBytesPerFile,
            materializeDeletions,
            materializeDeletionsThreshold,
            numThreads,
            batchSize);

    try (Dataset dataset = catalog.openTable(table)) {
      CompactionMetrics metrics = runCompaction(dataset, options);
      return toRows(metrics);
    }
  }

  private static CompactionOptions buildOptions(
      @Nullable Long targetRowsPerFragment,
      @Nullable Long maxRowsPerGroup,
      @Nullable Long maxBytesPerFile,
      @Nullable Boolean materializeDeletions,
      @Nullable Float materializeDeletionsThreshold,
      @Nullable Integer numThreads,
      @Nullable Integer batchSize) {
    CompactionOptions.Builder builder = CompactionOptions.builder();
    if (targetRowsPerFragment != null) {
      LanceProcedureUtils.validatePositive(targetRowsPerFragment, "target_rows_per_fragment");
      builder.withTargetRowsPerFragment(targetRowsPerFragment);
    }
    if (maxRowsPerGroup != null) {
      LanceProcedureUtils.validatePositive(maxRowsPerGroup, "max_rows_per_group");
      builder.withMaxRowsPerGroup(maxRowsPerGroup);
    }
    if (maxBytesPerFile != null) {
      LanceProcedureUtils.validatePositive(maxBytesPerFile, "max_bytes_per_file");
      builder.withMaxBytesPerFile(maxBytesPerFile);
    }
    if (materializeDeletions != null) {
      builder.withMaterializeDeletions(materializeDeletions);
    }
    if (materializeDeletionsThreshold != null) {
      LanceProcedureUtils.validateRatio(
          materializeDeletionsThreshold, "materialize_deletions_threshold");
      builder.withMaterializeDeletionsThreshold(materializeDeletionsThreshold);
    }
    if (numThreads != null) {
      LanceProcedureUtils.validatePositive(numThreads, "num_threads");
      builder.withNumThreads(numThreads);
    }
    if (batchSize != null) {
      LanceProcedureUtils.validatePositive(batchSize, "batch_size");
      builder.withBatchSize(batchSize);
    }
    return builder.build();
  }

  private static CompactionMetrics runCompaction(Dataset dataset, CompactionOptions options) {
    // TODO: Add distributed Flink compaction once Lance compaction tasks can run safely on workers.
    CompactionPlan plan = Compaction.planCompaction(dataset, options);
    List<CompactionTask> tasks = plan.getCompactionTasks();
    List<RewriteResult> results = new ArrayList<>(tasks.size());
    for (CompactionTask task : tasks) {
      results.add(task.execute(dataset));
    }
    return Compaction.commitCompaction(dataset, results, options);
  }

  private static Row[] toRows(CompactionMetrics metrics) {
    return LanceProcedureUtils.singleRowArray(
        metrics.getFragmentsAdded(),
        metrics.getFragmentsRemoved(),
        metrics.getFilesAdded(),
        metrics.getFilesRemoved());
  }
}
