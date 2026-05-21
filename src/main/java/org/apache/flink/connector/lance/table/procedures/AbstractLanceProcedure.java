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

/** Base class for Lance catalog procedures that need access to the owning catalog. */
abstract class AbstractLanceProcedure implements Procedure {

  protected final LanceNamespaceCatalog catalog;

  AbstractLanceProcedure(LanceNamespaceCatalog catalog) {
    this.catalog = catalog;
  }
}
