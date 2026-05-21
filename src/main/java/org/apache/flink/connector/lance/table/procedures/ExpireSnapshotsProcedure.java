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
import org.lance.Version;
import org.lance.cleanup.CleanupPolicy;
import org.lance.cleanup.RemovalStats;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Expires old Lance versions/files according to an explicit cleanup selector. Destructive. */
public class ExpireSnapshotsProcedure extends AbstractLanceProcedure {

  private static final String OUTPUT_TYPE =
      "ROW<bytes_removed BIGINT, old_versions BIGINT, data_files_removed BIGINT, transaction_files_removed BIGINT, index_files_removed BIGINT, deletion_files_removed BIGINT>";

  public ExpireSnapshotsProcedure(LanceNamespaceCatalog catalog) {
    super(catalog);
  }

  @ProcedureHint(
      argument = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(
            name = "before_timestamp_millis",
            type = @DataTypeHint("BIGINT"),
            isOptional = true),
        @ArgumentHint(name = "before_version", type = @DataTypeHint("BIGINT"), isOptional = true),
        @ArgumentHint(name = "retain_last", type = @DataTypeHint("INT"), isOptional = true),
        @ArgumentHint(
            name = "delete_unverified",
            type = @DataTypeHint("BOOLEAN"),
            isOptional = true),
        @ArgumentHint(
            name = "error_if_tagged_old_versions",
            type = @DataTypeHint("BOOLEAN"),
            isOptional = true),
        @ArgumentHint(
            name = "clean_referenced_branches",
            type = @DataTypeHint("BOOLEAN"),
            isOptional = true),
        @ArgumentHint(name = "delete_rate_limit", type = @DataTypeHint("BIGINT"), isOptional = true)
      },
      output = @DataTypeHint(OUTPUT_TYPE))
  public Row[] call(
      ProcedureContext context,
      String table,
      @Nullable Long beforeTimestampMillis,
      @Nullable Long beforeVersion,
      @Nullable Integer retainLast,
      @Nullable Boolean deleteUnverified,
      @Nullable Boolean errorIfTaggedOldVersions,
      @Nullable Boolean cleanReferencedBranches,
      @Nullable Long deleteRateLimit) {
    LanceProcedureUtils.validateNonEmpty(table, "`table`");
    validateArguments(beforeTimestampMillis, beforeVersion, retainLast, deleteRateLimit);

    try (Dataset dataset = catalog.openTable(table)) {
      Long effectiveBeforeVersion =
          resolveEffectiveBeforeVersion(dataset, beforeVersion, retainLast);
      if (effectiveBeforeVersion == null && retainLast != null) {
        // retain_last >= listVersions().size(): keep everything, skip cleanupWithPolicy entirely.
        return zeroStatsRow();
      }
      CleanupPolicy policy =
          buildCleanupPolicy(
              beforeTimestampMillis,
              effectiveBeforeVersion,
              deleteUnverified,
              errorIfTaggedOldVersions,
              cleanReferencedBranches,
              deleteRateLimit);
      // TODO: Add distributed cleanup only if Lance exposes a task-based cleanup plan.
      return toRows(dataset.cleanupWithPolicy(policy));
    }
  }

  private static void validateArguments(
      @Nullable Long beforeTimestampMillis,
      @Nullable Long beforeVersion,
      @Nullable Integer retainLast,
      @Nullable Long deleteRateLimit) {
    LanceProcedureUtils.validateExactlyOneNonNull(
        new LanceProcedureUtils.NamedValue("before_timestamp_millis", beforeTimestampMillis),
        new LanceProcedureUtils.NamedValue("before_version", beforeVersion),
        new LanceProcedureUtils.NamedValue("retain_last", retainLast));
    if (retainLast != null) {
      LanceProcedureUtils.validatePositive(retainLast, "retain_last");
    }
    if (beforeVersion != null) {
      LanceProcedureUtils.validatePositive(beforeVersion, "before_version");
    }
    if (beforeTimestampMillis != null) {
      LanceProcedureUtils.validatePositive(beforeTimestampMillis, "before_timestamp_millis");
    }
    if (deleteRateLimit != null) {
      LanceProcedureUtils.validatePositive(deleteRateLimit, "delete_rate_limit");
    }
  }

  /** Resolves retain_last to a before_version cutoff; returns null when cleanup is a no-op. */
  @Nullable
  private static Long resolveEffectiveBeforeVersion(
      Dataset dataset, @Nullable Long beforeVersion, @Nullable Integer retainLast) {
    if (retainLast == null) {
      return beforeVersion;
    }
    List<Version> sorted = new ArrayList<>(dataset.listVersions());
    sorted.sort(Comparator.comparingLong(Version::getId));
    if (retainLast >= sorted.size()) {
      return null;
    }
    return sorted.get(sorted.size() - retainLast - 1).getId();
  }

  private static CleanupPolicy buildCleanupPolicy(
      @Nullable Long beforeTimestampMillis,
      @Nullable Long effectiveBeforeVersion,
      @Nullable Boolean deleteUnverified,
      @Nullable Boolean errorIfTaggedOldVersions,
      @Nullable Boolean cleanReferencedBranches,
      @Nullable Long deleteRateLimit) {
    CleanupPolicy.Builder policy = CleanupPolicy.builder();
    if (beforeTimestampMillis != null) {
      policy.withBeforeTimestampMillis(beforeTimestampMillis);
    }
    if (effectiveBeforeVersion != null) {
      policy.withBeforeVersion(effectiveBeforeVersion);
    }
    if (deleteUnverified != null) {
      policy.withDeleteUnverified(deleteUnverified);
    }
    if (errorIfTaggedOldVersions != null) {
      policy.withErrorIfTaggedOldVersions(errorIfTaggedOldVersions);
    }
    if (cleanReferencedBranches != null) {
      policy.withCleanReferencedBranches(cleanReferencedBranches);
    }
    if (deleteRateLimit != null) {
      policy.withDeleteRateLimit(deleteRateLimit);
    }
    return policy.build();
  }

  private static Row[] toRows(RemovalStats stats) {
    return LanceProcedureUtils.singleRowArray(
        stats.getBytesRemoved(),
        stats.getOldVersions(),
        stats.getDataFilesRemoved(),
        stats.getTransactionFilesRemoved(),
        stats.getIndexFilesRemoved(),
        stats.getDeletionFilesRemoved());
  }

  private static Row[] zeroStatsRow() {
    return LanceProcedureUtils.singleRowArray(0L, 0L, 0L, 0L, 0L, 0L);
  }
}
