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

import java.util.Objects;

/** Mutable runtime state for a Lance source split. */
public class LanceSourceSplitState {

  private final LanceSourceSplit split;
  private long recordsToSkip;

  public LanceSourceSplitState(LanceSourceSplit split) {
    this.split = Objects.requireNonNull(split, "split");
    this.recordsToSkip = split.recordsToSkip();
  }

  public LanceSourceSplit getSplit() {
    return split;
  }

  public long getRecordsToSkip() {
    return recordsToSkip;
  }

  public void setRecordsToSkip(long recordsToSkip) {
    if (recordsToSkip < 0) {
      throw new IllegalArgumentException("recordsToSkip must be non-negative.");
    }
    this.recordsToSkip = recordsToSkip;
  }

  public LanceSourceSplit toSplit() {
    return split.withRecordsToSkip(recordsToSkip);
  }
}
