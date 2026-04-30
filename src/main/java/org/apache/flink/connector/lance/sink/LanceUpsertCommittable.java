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

import java.util.Objects;

/** Upsert committable with Arrow IPC payload. */
public final class LanceUpsertCommittable {

  public enum Mode {
    UPSERT,
    DELETE
  }

  private final long committableId;
  private final int subtaskId;
  private final Mode mode;
  private final byte[] arrowIpcBytes;
  private final long rowCount;

  public LanceUpsertCommittable(
      long committableId, int subtaskId, Mode mode, byte[] arrowIpcBytes, long rowCount) {
    this.committableId = committableId;
    this.subtaskId = subtaskId;
    this.mode = Objects.requireNonNull(mode, "mode");
    this.arrowIpcBytes = Objects.requireNonNull(arrowIpcBytes, "arrowIpcBytes").clone();
    this.rowCount = rowCount;
  }

  public long committableId() {
    return committableId;
  }

  public int subtaskId() {
    return subtaskId;
  }

  public Mode mode() {
    return mode;
  }

  public byte[] arrowIpcBytes() {
    return arrowIpcBytes.clone();
  }

  public long rowCount() {
    return rowCount;
  }

  @Override
  public String toString() {
    return "LanceUpsertCommittable{committableId="
        + committableId
        + ", subtaskId="
        + subtaskId
        + ", mode="
        + mode
        + ", rows="
        + rowCount
        + ", bytes="
        + arrowIpcBytes.length
        + '}';
  }
}
