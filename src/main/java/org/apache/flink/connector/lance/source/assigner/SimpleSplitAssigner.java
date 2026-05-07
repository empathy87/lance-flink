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
package org.apache.flink.connector.lance.source.assigner;

import org.apache.flink.connector.lance.source.LanceSourceSplit;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** First-in / first-out {@link SplitAssigner}. */
public class SimpleSplitAssigner implements SplitAssigner {

  private final Deque<LanceSourceSplit> remaining;

  public SimpleSplitAssigner() {
    this(Collections.emptyList());
  }

  public SimpleSplitAssigner(Collection<LanceSourceSplit> initial) {
    this.remaining = new ArrayDeque<>(Objects.requireNonNull(initial, "initial"));
  }

  @Override
  public Optional<LanceSourceSplit> getNext() {
    return Optional.ofNullable(remaining.pollFirst());
  }

  @Override
  public void addSplits(Collection<LanceSourceSplit> splits) {
    remaining.addAll(Objects.requireNonNull(splits, "splits"));
  }

  @Override
  public List<LanceSourceSplit> remainingSplits() {
    return new ArrayList<>(remaining);
  }
}
