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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

/** Row factory shared by sink unit tests; matches the {@code (id BIGINT, name STRING)} schema. */
final class SinkTestRows {

  private SinkTestRows() {}

  static RowData simple(long id, String name) {
    return tagged(RowKind.INSERT, id, name);
  }

  static RowData tagged(RowKind kind, long id, String name) {
    GenericRowData row = new GenericRowData(kind, 2);
    row.setField(0, id);
    row.setField(1, StringData.fromString(name));
    return row;
  }
}
