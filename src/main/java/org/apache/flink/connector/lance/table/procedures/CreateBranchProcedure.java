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
import org.lance.Ref;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

/** Creates a Lance branch from HEAD, a version, or a tag resolved to a fixed version. */
public class CreateBranchProcedure extends AbstractLanceProcedure {

  public CreateBranchProcedure(LanceNamespaceCatalog catalog) {
    super(catalog);
  }

  @ProcedureHint(
      argument = {
        @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "branch", type = @DataTypeHint("STRING")),
        @ArgumentHint(name = "from_version", type = @DataTypeHint("BIGINT"), isOptional = true),
        @ArgumentHint(name = "from_tag", type = @DataTypeHint("STRING"), isOptional = true)
      },
      output = @DataTypeHint("ROW<branch STRING, parent_version BIGINT>"))
  public Row[] call(
      ProcedureContext context,
      String table,
      String branch,
      @Nullable Long fromVersion,
      @Nullable String fromTag) {
    LanceProcedureUtils.validateNonEmpty(table, "`table`");
    LanceProcedureUtils.validateNonEmpty(branch, "branch");
    LanceProcedureUtils.validateAtMostOneNonNull(
        new LanceProcedureUtils.NamedValue("from_version", fromVersion),
        new LanceProcedureUtils.NamedValue("from_tag", fromTag));

    if (fromVersion != null) {
      LanceProcedureUtils.validatePositive(fromVersion, "from_version");
    }
    if (fromTag != null) {
      LanceProcedureUtils.validateNonEmpty(fromTag, "from_tag");
    }

    try (Dataset dataset = catalog.openTable(table)) {
      ParentRef parent = resolveParentRef(dataset, fromVersion, fromTag);
      try (Dataset created = dataset.createBranch(branch, parent.ref())) {}
      return LanceProcedureUtils.singleRowArray(branch, parent.parentVersion());
    }
  }

  private static ParentRef resolveParentRef(
      Dataset dataset, @Nullable Long fromVersion, @Nullable String fromTag) {
    if (fromVersion != null) {
      return new ParentRef(fromVersion, Ref.ofMain(fromVersion));
    }
    if (fromTag != null) {
      // Resolve the tag once so parent_version matches the actual branch parent.
      long resolved = dataset.tags().getVersion(fromTag);
      return new ParentRef(resolved, Ref.ofMain(resolved));
    }
    long head = dataset.latestVersion();
    return new ParentRef(head, Ref.ofMain(head));
  }

  private record ParentRef(long parentVersion, Ref ref) {}
}
