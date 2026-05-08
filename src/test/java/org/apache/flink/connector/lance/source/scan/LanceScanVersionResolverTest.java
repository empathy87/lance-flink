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
package org.apache.flink.connector.lance.source.scan;

import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.Version;
import org.lance.operation.Append;
import org.lance.operation.Overwrite;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceScanVersionResolverTest {

  private static final RowType ROW_TYPE =
      new RowType(List.of(new RowType.RowField("id", new IntType(false))));
  private static final Schema SCHEMA = LanceTypeConverter.toArrowSchema(ROW_TYPE);
  private static final RowDataConverter CONVERTER = new RowDataConverter(ROW_TYPE);
  private static final String TAG_AT_V2 = "two";

  @TempDir private static Path tempDir;

  private static String datasetUri;
  private static long latestId;
  private static long v1Millis;
  private static long v2Millis;
  private static long v3Millis;

  @BeforeAll
  static void seedDataset() throws Exception {
    datasetUri = tempDir.resolve("ds").toUri().toString();

    // v1: create dataset via Overwrite.
    overwrite(writeFragments(List.of(GenericRowData.of(1))));
    Thread.sleep(50);

    // v2: append a row, then tag this version.
    append(writeFragments(List.of(GenericRowData.of(2))));
    long v2Id = openLatestVersion();
    addTag(TAG_AT_V2, v2Id);
    Thread.sleep(50);

    // v3: append another row.
    append(writeFragments(List.of(GenericRowData.of(3))));

    // Snapshot ids + times for the assertions.
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      latestId = ds.latestVersion();
      for (Version v : ds.listVersions()) {
        long millis = v.getDataTime().toInstant().toEpochMilli();
        if (v.getId() == 1L) {
          v1Millis = millis;
        } else if (v.getId() == v2Id) {
          v2Millis = millis;
        } else if (v.getId() == latestId) {
          v3Millis = millis;
        }
      }
    }
  }

  @Test
  void resolvesLatestToLatestVersion() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      long resolved = LanceScanVersionResolver.resolveVersion(LanceScanOptions.latest(), ds);
      assertThat(resolved).isEqualTo(ds.latestVersion());
    }
  }

  @Test
  void resolvesVersionInRange() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      assertThat(LanceScanVersionResolver.resolveVersion(LanceScanOptions.version(1L), ds))
          .isEqualTo(1L);
      assertThat(LanceScanVersionResolver.resolveVersion(LanceScanOptions.version(latestId), ds))
          .isEqualTo(latestId);
    }
  }

  @Test
  void resolvesVersionAboveLatestRejected() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      long tooHigh = ds.latestVersion() + 100;
      assertThatThrownBy(
              () -> LanceScanVersionResolver.resolveVersion(LanceScanOptions.version(tooHigh), ds))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("out of range")
          .hasMessageContaining("latest version is");
    }
  }

  @Test
  void resolvesExistingTag() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      long resolved =
          LanceScanVersionResolver.resolveVersion(LanceScanOptions.tagName(TAG_AT_V2), ds);
      assertThat(resolved).isNotEqualTo(ds.latestVersion());
      assertThat(resolved).isGreaterThanOrEqualTo(1L).isLessThan(ds.latestVersion());
    }
  }

  @Test
  void resolvesMissingTagRejected() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      assertThatThrownBy(
              () ->
                  LanceScanVersionResolver.resolveVersion(
                      LanceScanOptions.tagName("does-not-exist"), ds))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Lance tag 'does-not-exist' could not be resolved");
    }
  }

  @Test
  void resolvesTimestampBeforeEarliestVersionRejected() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      long beforeEarliest = v1Millis - 1_000L;
      assertThatThrownBy(
              () ->
                  LanceScanVersionResolver.resolveVersion(
                      LanceScanOptions.timestampMillis(beforeEarliest), ds))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("older than the dataset's earliest available version");
    }
  }

  @Test
  void resolvesTimestampAtVersionReturnsThatVersion() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      long resolved =
          LanceScanVersionResolver.resolveVersion(LanceScanOptions.timestampMillis(v2Millis), ds);
      // Exact-timestamp match should land on the v2 commit (its data time equals v2Millis).
      assertThat(resolved).isNotEqualTo(latestId);
    }
  }

  @Test
  void resolvesTimestampBetweenVersionsReturnsEarlier() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      long between = v2Millis + (v3Millis - v2Millis) / 2;
      long resolved =
          LanceScanVersionResolver.resolveVersion(LanceScanOptions.timestampMillis(between), ds);
      // Strictly older than v3 but >= v2: must land on v2.
      assertThat(resolved).isNotEqualTo(latestId);
    }
  }

  @Test
  void resolvesTimestampAtLatestReturnsLatest() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      long resolved =
          LanceScanVersionResolver.resolveVersion(
              LanceScanOptions.timestampMillis(v3Millis + 1_000L), ds);
      assertThat(resolved).isEqualTo(latestId);
    }
  }

  // --- helpers -----------------------------------------------------------------------------

  private static List<FragmentMetadata> writeFragments(List<RowData> data) {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, alloc)) {
      CONVERTER.toVectorSchemaRoot(data, root);
      return Fragment.write().datasetUri(datasetUri).allocator(alloc).data(root).execute();
    }
  }

  private static void overwrite(List<FragmentMetadata> fragments) {
    Overwrite operation = Overwrite.builder().fragments(fragments).schema(SCHEMA).build();
    try (BufferAllocator alloc = new RootAllocator();
        Transaction tx = new Transaction.Builder().operation(operation).build()) {
      new CommitBuilder(datasetUri, alloc).execute(tx);
    }
  }

  private static void append(List<FragmentMetadata> fragments) {
    Append operation = Append.builder().fragments(fragments).build();
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build();
        Transaction tx =
            new Transaction.Builder().operation(operation).readVersion(ds.version()).build()) {
      new CommitBuilder(ds).execute(tx);
    }
  }

  private static long openLatestVersion() {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      return ds.latestVersion();
    }
  }

  private static void addTag(String name, long version) {
    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = Dataset.open().allocator(alloc).uri(datasetUri).build()) {
      ds.tags().create(name, version);
    }
  }
}
