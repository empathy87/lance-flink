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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the procedure name → factory registry. */
class LanceProcedureRegistryTest {

  @Test
  void exposesAllTwelveProcedures() {
    assertThat(LanceProcedureRegistry.listNames())
        .containsExactlyInAnyOrder(
            "compact",
            "expire_snapshots",
            "optimize_indices",
            "list_indices",
            "create_index",
            "drop_index",
            "create_tag",
            "delete_tag",
            "rollback_to_version",
            "rollback_to_tag",
            "create_branch",
            "delete_branch");
  }

  @Test
  void lookupIsCaseInsensitive() {
    assertThat(LanceProcedureRegistry.lookup("compact")).isNotNull();
    assertThat(LanceProcedureRegistry.lookup("COMPACT")).isNotNull();
    assertThat(LanceProcedureRegistry.lookup("Compact")).isNotNull();
  }

  @Test
  void unknownLookupReturnsNull() {
    assertThat(LanceProcedureRegistry.lookup("no_such_procedure")).isNull();
    assertThat(LanceProcedureRegistry.lookup(null)).isNull();
  }
}
