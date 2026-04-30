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

import org.lance.FragmentMetadata;

import java.util.List;

/** Append committable for Lance fragments. */
public final class LanceAppendCommittable {

  private final long committableId;
  private final int subtaskId;
  private final List<FragmentMetadata> fragments;

  public LanceAppendCommittable(
      long committableId, int subtaskId, List<FragmentMetadata> fragments) {
    this.committableId = committableId;
    this.subtaskId = subtaskId;
    this.fragments = List.copyOf(fragments);
  }

  public long committableId() {
    return committableId;
  }

  public int subtaskId() {
    return subtaskId;
  }

  public List<FragmentMetadata> fragments() {
    return fragments;
  }

  @Override
  public String toString() {
    return "LanceAppendCommittable{checkpointId="
        + committableId
        + ", subtaskId="
        + subtaskId
        + ", fragments="
        + fragments.size()
        + '}';
  }
}
