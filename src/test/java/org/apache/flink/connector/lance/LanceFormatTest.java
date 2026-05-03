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
package org.apache.flink.connector.lance;

import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;

import org.lance.*;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.merge.MergeInsertParams;
import org.lance.namespace.DirectoryNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.operation.Append;
import org.lance.operation.Overwrite;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class LanceFormatTest {

  private static final RowType ROW_TYPE =
      new RowType(
          Arrays.asList(
              new RowType.RowField("id", new IntType(false)),
              new RowType.RowField("age", new IntType())));

  private static final Schema SCHEMA = LanceTypeConverter.toArrowSchema(ROW_TYPE);
  private static final RowDataConverter CONVERTER = new RowDataConverter(ROW_TYPE);

  private static final List<String> TABLE1_ID = List.of("default", "append_single");
  private static final List<String> TABLE2_ID = List.of("default", "append_parallel");
  private static final List<String> TABLE3_ID = List.of("default", "upsert_parallel");

  @TempDir private static Path tempDir;
  private static BufferAllocator allocator;
  private static DirectoryNamespace namespace;
  private static String table1Location;
  private static String table2Location;
  private static String table3Location;

  @BeforeAll
  static void beforeAll() throws IOException {
    allocator = new RootAllocator();
    namespace = new DirectoryNamespace();
    namespace.initialize(Map.of("root", tempDir.toString()), allocator);
    namespace.createNamespace(new CreateNamespaceRequest().id(singletonList("default")));

    byte[] emptyAgeTableBytes = getEmptyTableBytes(allocator);

    namespace.createTable(new CreateTableRequest().id(TABLE1_ID), emptyAgeTableBytes);
    namespace.createTable(new CreateTableRequest().id(TABLE2_ID), emptyAgeTableBytes);
    namespace.createTable(new CreateTableRequest().id(TABLE3_ID), emptyAgeTableBytes);

    DescribeTableRequest request = new DescribeTableRequest();
    table1Location = namespace.describeTable(request.id(TABLE1_ID)).getLocation();
    table2Location = namespace.describeTable(request.id(TABLE2_ID)).getLocation();
    table3Location = namespace.describeTable(request.id(TABLE3_ID)).getLocation();
  }

  @AfterAll
  static void afterAll() {
    namespace.close();
    allocator.close();
  }

  private static byte @NotNull [] getEmptyTableBytes(BufferAllocator allocator) throws IOException {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {

      root.setRowCount(0);

      writer.start();
      writer.writeBatch();
      writer.end();

      return out.toByteArray();
    }
  }

  @Test
  void testAppendSingleWriter() throws Exception {
    List<RowData> data =
        List.of(GenericRowData.of(1, 18), GenericRowData.of(2, 25), GenericRowData.of(3, 31));

    // write fragments to the target location
    List<FragmentMetadata> fragments = writeFragments(data, table1Location);

    // commit the fragments in one transaction
    commitAppend(fragments, table1Location);

    // read and assert the table rows
    assertThat(readRows(table1Location)).containsExactlyInAnyOrderElementsOf(data);
  }

  @Test
  void appendParallelWritersSingleCommit() throws Exception {
    List<RowData> data =
        List.of(
            GenericRowData.of(1, 18),
            GenericRowData.of(2, 25),
            GenericRowData.of(3, 31),
            GenericRowData.of(4, 40),
            GenericRowData.of(5, 50),
            GenericRowData.of(6, 60));
    List<List<RowData>> dataBatches = splitIntoBatches(data, 2);

    // write fragments to the target location in parallel
    List<FragmentMetadata> fragments = writeFragmentsParallel(dataBatches, table2Location);

    // commit the fragments in one transaction
    commitAppend(fragments, table2Location);

    // read and assert the table rows
    assertThat(readRows(table2Location)).containsExactlyInAnyOrderElementsOf(data);
  }

  @Test
  void upsertParallelWritersSingleMerge() throws Exception {
    List<RowData> startingData =
        List.of(
            GenericRowData.of(0, 10),
            GenericRowData.of(1, 18),
            GenericRowData.of(2, 25),
            GenericRowData.of(3, 31));
    List<List<RowData>> upsertData =
        List.of(
            List.of(GenericRowData.of(2, 26), GenericRowData.of(4, 40)),
            List.of(GenericRowData.of(3, 32), GenericRowData.of(5, 50)));
    List<List<RowData>> deleteData = List.of(List.of(GenericRowData.of(0, null)));

    // write fragments to the target location
    List<FragmentMetadata> fragments = writeFragments(startingData, table3Location);

    // commit the fragments in one transaction
    commitAppend(fragments, table3Location);

    // read and assert the table rows
    assertThat(readRows(table3Location)).containsExactlyInAnyOrderElementsOf(startingData);

    // write upsert fragments to the staging location in parallel
    String cdcRoot = getStagingLocation(table3Location, "test-job", 1L);

    String deletesStagingLocation = cdcRoot + "/deletes.lance";
    String upsertsStagingLocation = cdcRoot + "/upserts.lance";

    List<FragmentMetadata> deleteFragments =
        writeFragmentsParallel(deleteData, deletesStagingLocation);
    List<FragmentMetadata> upsertFragments =
        writeFragmentsParallel(upsertData, upsertsStagingLocation);

    // commit the upsert fragments in one transaction
    commitOverwrite(deleteFragments, deletesStagingLocation);
    commitOverwrite(upsertFragments, upsertsStagingLocation);

    // read and assert the staged upsert rows
    assertThat(readRows(upsertsStagingLocation))
        .containsExactlyInAnyOrderElementsOf(upsertData.stream().flatMap(List::stream).toList());

    // upsert rows from target
    commitMergeDeletes(table3Location, deletesStagingLocation, List.of("id"));
    commitMergeUpserts(table3Location, upsertsStagingLocation, List.of("id"));

    // read and assert the table rows
    assertThat(readRows(table3Location))
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                GenericRowData.of(1, 18),
                GenericRowData.of(2, 26),
                GenericRowData.of(3, 32),
                GenericRowData.of(4, 40),
                GenericRowData.of(5, 50)));
  }

  private static void commitAppend(List<FragmentMetadata> fragments, String location) {
    Append operation = Append.builder().fragments(fragments).build();
    Transaction.Builder transactionBuilder = new Transaction.Builder().operation(operation);
    try (BufferAllocator allocator = new RootAllocator();
        Dataset dataset = Dataset.open().allocator(allocator).uri(location).build();
        Transaction transaction = transactionBuilder.readVersion(dataset.version()).build()) {
      new CommitBuilder(dataset).execute(transaction);
    }
  }

  private static void commitOverwrite(List<FragmentMetadata> fragments, String location) {
    Overwrite operation = Overwrite.builder().fragments(fragments).schema(SCHEMA).build();
    Transaction.Builder transactionBuilder = new Transaction.Builder().operation(operation);
    try (BufferAllocator allocator = new RootAllocator();
        Transaction transaction = transactionBuilder.build()) {
      new CommitBuilder(location, allocator).execute(transaction);
    }
  }

  private static List<FragmentMetadata> writeFragments(List<RowData> data, String location) {
    List<FragmentMetadata> fragments;
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      CONVERTER.toVectorSchemaRoot(data, root);
      fragments = Fragment.write().datasetUri(location).allocator(allocator).data(root).execute();
    }
    return fragments;
  }

  private static <T> List<List<T>> splitIntoBatches(List<T> list, int batchSize) {
    List<List<T>> batches = new ArrayList<>();
    for (int i = 0; i < list.size(); i += batchSize) {
      int end = Math.min(i + batchSize, list.size());
      batches.add(list.subList(i, end));
    }
    return batches;
  }

  private static List<FragmentMetadata> writeFragmentsParallel(
      List<List<RowData>> dataBatches, String location)
      throws InterruptedException, ExecutionException {
    ExecutorService executor = Executors.newFixedThreadPool(dataBatches.size());
    try {
      CountDownLatch readyLatch = new CountDownLatch(dataBatches.size());
      CountDownLatch startLatch = new CountDownLatch(1);

      List<Future<List<FragmentMetadata>>> fragmentsFutures =
          dataBatches.stream()
              .map(
                  batch ->
                      executor.submit(
                          () -> {
                            readyLatch.countDown();
                            startLatch.await();
                            return writeFragments(batch, location);
                          }))
              .toList();
      readyLatch.await();
      startLatch.countDown();
      List<FragmentMetadata> fragments = new ArrayList<>();
      for (Future<List<FragmentMetadata>> future : fragmentsFutures) {
        fragments.addAll(future.get());
      }
      return fragments;
    } finally {
      executor.shutdown();
    }
  }

  private static void commitMergeDeletes(
      String targetLocation, String stagingLocation, List<String> primaryKeyColumns)
      throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        Dataset targetDataset = Dataset.open().allocator(allocator).uri(targetLocation).build();
        Dataset stagingDataset = Dataset.open().allocator(allocator).uri(stagingLocation).build();
        LanceScanner scanner = stagingDataset.newScan(new ScanOptions.Builder().build());
        ArrowArrayStream source = ArrowArrayStream.allocateNew(allocator); ) {

      ArrowReader reader = scanner.scanBatches();
      Data.exportArrayStream(allocator, reader, source);

      MergeInsertParams params =
          new MergeInsertParams(primaryKeyColumns)
              .withMatchedDelete()
              .withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing)
              .withNotMatchedBySourceKeep();

      targetDataset.mergeInsert(params, source);
    }
  }

  private static void commitMergeUpserts(
      String targetLocation, String stagingLocation, List<String> primaryKeyColumns)
      throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        Dataset targetDataset = Dataset.open().allocator(allocator).uri(targetLocation).build();
        Dataset stagingDataset = Dataset.open().allocator(allocator).uri(stagingLocation).build();
        LanceScanner scanner = stagingDataset.newScan(new ScanOptions.Builder().build());
        ArrowArrayStream source = ArrowArrayStream.allocateNew(allocator); ) {

      ArrowReader reader = scanner.scanBatches();
      Data.exportArrayStream(allocator, reader, source);

      MergeInsertParams params =
          new MergeInsertParams(primaryKeyColumns)
              .withMatchedUpdateAll()
              .withNotMatchedBySourceKeep();

      targetDataset.mergeInsert(params, source);
    }
  }

  private List<RowData> readRows(String location) throws Exception {
    try (Dataset dataset = Dataset.open().allocator(allocator).uri(location).build()) {
      RowType rowType = LanceTypeConverter.toFlinkRowType(dataset.getSchema());

      RowDataConverter converter = new RowDataConverter(rowType);

      List<RowData> rows = new ArrayList<>();

      try (LanceScanner scanner = dataset.newScan(new ScanOptions.Builder().build());
          ArrowReader reader = scanner.scanBatches()) {

        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();

          rows.addAll(converter.toRowDataList(root));
        }
      }

      return rows;
    }
  }

  private String getStagingLocation(String targetLocation, String jobId, long checkpointId) {
    return trimTrailingSlash(targetLocation)
        + "/_flink_staging/"
        + "job-"
        + jobId
        + "/checkpoint-"
        + checkpointId;
  }

  private String trimTrailingSlash(String path) {
    int end = path.length();

    while (end > 1 && path.charAt(end - 1) == '/') {
      end--;
    }

    return path.substring(0, end);
  }
}
