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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** Assigns Lance source splits to requesting readers. */
public interface SplitAssigner {

  Optional<LanceSourceSplit> getNext();

  void addSplits(Collection<LanceSourceSplit> splits);

  List<LanceSourceSplit> remainingSplits();
}
