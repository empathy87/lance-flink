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

import org.apache.flink.table.procedures.Procedure;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/** Registry for {@code sys.<name>} Lance catalog procedures. */
public final class LanceProcedureRegistry {

  private static final Map<String, Function<LanceNamespaceCatalog, Procedure>> FACTORIES =
      Map.ofEntries(
          Map.entry("compact", CompactProcedure::new),
          Map.entry("expire_snapshots", ExpireSnapshotsProcedure::new),
          Map.entry("optimize_indices", OptimizeIndicesProcedure::new),
          Map.entry("list_indices", ListIndicesProcedure::new),
          Map.entry("create_index", CreateIndexProcedure::new),
          Map.entry("drop_index", DropIndexProcedure::new),
          Map.entry("create_tag", CreateTagProcedure::new),
          Map.entry("delete_tag", DeleteTagProcedure::new),
          Map.entry("rollback_to_version", RollbackToVersionProcedure::new),
          Map.entry("rollback_to_tag", RollbackToTagProcedure::new),
          Map.entry("create_branch", CreateBranchProcedure::new),
          Map.entry("delete_branch", DeleteBranchProcedure::new));

  private LanceProcedureRegistry() {}

  /** Returns the procedure factory, or {@code null} if the name is unknown. */
  @Nullable
  public static Function<LanceNamespaceCatalog, Procedure> lookup(String name) {
    if (name == null) {
      return null;
    }
    return FACTORIES.get(name.toLowerCase(Locale.ROOT));
  }

  /** Returns all registered procedure names, sorted alphabetically. */
  public static List<String> listNames() {
    return FACTORIES.keySet().stream().sorted().toList();
  }
}
