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

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.operation.Overwrite;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class LanceSourceSplitReaderTest {

  private static final RowType ROW_TYPE =
      new RowType(List.of(new RowType.RowField("id", new IntType(false))));
  private static final Schema SCHEMA = LanceTypeConverter.toArrowSchema(ROW_TYPE);
  private static final RowDataConverter CONVERTER = new RowDataConverter(ROW_TYPE);

  @TempDir private Path tempDir;

  @Test
  void limitBudgetIsSharedAcrossSplits() throws Exception {
    String datasetUri = tempDir.resolve("ds").toUri().toString();

    // Three fragment writes of two rows each. Each Fragment.write() call produces its own fragment
    // metadata; we commit them together so the dataset has at least three fragments.
    List<FragmentMetadata> allFragments = new ArrayList<>();
    for (int batch = 0; batch < 3; batch++) {
      int base = batch * 100;
      allFragments.addAll(
          writeFragment(
              List.of(GenericRowData.of(base + 1), GenericRowData.of(base + 2)), datasetUri));
    }
    commitOverwrite(allFragments, datasetUri);

    List<LanceSourceSplit> splits = listSplits(datasetUri);
    assertThat(splits.size()).isGreaterThanOrEqualTo(3);

    LanceOptions opts = LanceOptions.builder().path(datasetUri).readBatchSize(16).build();
    LanceSourceSplitReader reader = new LanceSourceSplitReader(opts, ROW_TYPE, null, null, 2L);
    try {
      reader.handleSplitsChanges(new SplitsAddition<>(splits));

      List<RowData> emitted = new ArrayList<>();
      Set<String> finished = new HashSet<>();
      drain(reader, emitted, finished);

      // limit=2 must cap total emission across every split this reader handled. Without the
      // per-reader budget, each fragment would have emitted up to its own limit and the reader
      // would have produced 2 * splits.size() rows here.
      assertThat(emitted).hasSize(2);
      // Every split, including ones we never opened a scanner for, must be reported finished so
      // the assigner can retire them.
      assertThat(finished).hasSize(splits.size());
    } finally {
      reader.close();
    }
  }

  @Test
  void noLimitReadsEveryRow() throws Exception {
    String datasetUri = tempDir.resolve("ds-no-limit").toUri().toString();
    List<FragmentMetadata> allFragments = new ArrayList<>();
    for (int batch = 0; batch < 3; batch++) {
      int base = batch * 100;
      allFragments.addAll(
          writeFragment(
              List.of(GenericRowData.of(base + 1), GenericRowData.of(base + 2)), datasetUri));
    }
    commitOverwrite(allFragments, datasetUri);

    List<LanceSourceSplit> splits = listSplits(datasetUri);

    LanceOptions opts = LanceOptions.builder().path(datasetUri).readBatchSize(16).build();
    LanceSourceSplitReader reader = new LanceSourceSplitReader(opts, ROW_TYPE, null, null, null);
    try {
      reader.handleSplitsChanges(new SplitsAddition<>(splits));

      List<RowData> emitted = new ArrayList<>();
      Set<String> finished = new HashSet<>();
      drain(reader, emitted, finished);

      assertThat(emitted).hasSize(6);
      assertThat(finished).hasSize(splits.size());
    } finally {
      reader.close();
    }
  }

  private static void drain(
      LanceSourceSplitReader reader, List<RowData> emitted, Set<String> finished) throws Exception {
    while (true) {
      RecordsWithSplitIds<RowData> records = reader.fetch();
      String splitId = records.nextSplit();
      boolean produced = false;
      if (splitId != null) {
        produced = true;
        RowData row;
        while ((row = records.nextRecordFromSplit()) != null) {
          emitted.add(row);
        }
      }
      if (!records.finishedSplits().isEmpty()) {
        produced = true;
        finished.addAll(records.finishedSplits());
      }
      if (!produced) {
        return;
      }
    }
  }

  private static List<LanceSourceSplit> listSplits(String datasetUri) {
    List<LanceSourceSplit> splits = new ArrayList<>();
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      long version = ds.version();
      for (Fragment f : ds.getFragments()) {
        splits.add(LanceSourceSplit.fragment(version, f.getId()));
      }
    }
    return splits;
  }

  private static List<FragmentMetadata> writeFragment(List<RowData> data, String datasetUri) {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, alloc)) {
      CONVERTER.toVectorSchemaRoot(data, root);
      return Fragment.write().datasetUri(datasetUri).allocator(alloc).data(root).execute();
    }
  }

  private static void commitOverwrite(List<FragmentMetadata> fragments, String datasetUri) {
    Overwrite operation = Overwrite.builder().fragments(fragments).schema(SCHEMA).build();
    try (BufferAllocator alloc = new RootAllocator();
        Transaction tx = new Transaction.Builder().operation(operation).build()) {
      new CommitBuilder(datasetUri, alloc).execute(tx);
    }
  }
}
