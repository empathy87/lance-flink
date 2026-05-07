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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;

import java.util.*;

/** Records returned by a Lance split reader fetch. */
final class LanceRecordsWithSplitIds implements RecordsWithSplitIds<RowData> {

  private static final LanceRecordsWithSplitIds EMPTY =
      new LanceRecordsWithSplitIds(null, Collections.emptyIterator(), Collections.emptySet());

  private String nextSplitId;
  private final Iterator<RowData> records;
  private final Set<String> finishedSplits;

  private LanceRecordsWithSplitIds(
      String splitId, Iterator<RowData> records, Set<String> finishedSplits) {
    this.nextSplitId = splitId;
    this.records = records;
    this.finishedSplits = finishedSplits;
  }

  static LanceRecordsWithSplitIds forRecords(String splitId, List<RowData> records) {
    return new LanceRecordsWithSplitIds(
        Objects.requireNonNull(splitId, "splitId"),
        Objects.requireNonNull(records, "records").iterator(),
        Collections.emptySet());
  }

  static LanceRecordsWithSplitIds finishedSplit(String splitId) {
    return new LanceRecordsWithSplitIds(
        null,
        Collections.emptyIterator(),
        Collections.singleton(Objects.requireNonNull(splitId, "splitId")));
  }

  static LanceRecordsWithSplitIds empty() {
    return EMPTY;
  }

  @Override
  public String nextSplit() {
    String result = nextSplitId;
    nextSplitId = null;
    return result;
  }

  @Override
  public RowData nextRecordFromSplit() {
    return records.hasNext() ? records.next() : null;
  }

  @Override
  public Set<String> finishedSplits() {
    return finishedSplits;
  }
}
