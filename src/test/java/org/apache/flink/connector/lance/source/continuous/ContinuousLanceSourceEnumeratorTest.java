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
package org.apache.flink.connector.lance.source.continuous;

import org.apache.flink.connector.lance.LanceDatasetOpener;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.connector.lance.source.LanceSourceSplit;

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.operation.Append;
import org.lance.operation.Overwrite;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContinuousLanceSourceEnumeratorTest {

  private static final RowType ROW_TYPE = RowType.of(new IntType());
  private static final Schema SCHEMA = LanceTypeConverter.toArrowSchema(ROW_TYPE);
  private static final RowDataConverter CONVERTER = new RowDataConverter(ROW_TYPE);

  @TempDir private Path tempDir;
  private String datasetUri;
  private MockSplitEnumeratorContext<LanceSourceSplit> ctx;

  @BeforeEach
  void setUp() {
    datasetUri = tempDir.resolve("ds").toAbsolutePath().toString();
    ctx = new MockSplitEnumeratorContext<>(1);
  }

  @AfterEach
  void tearDown() throws Exception {
    ctx.close();
  }

  @Test
  void startupAtLatestEmitsNoSplits() {
    commitOverwrite(writeFragment(rows(1, 2, 3)));
    ContinuousLanceSourceEnumerator e = newEnumerator();
    e.start();
    LanceContinuousEnumState state = e.snapshotState(0L);
    assertThat(state.remainingSplits()).isEmpty();
    assertThat(state.lastEnumeratedVersion()).isPositive();
    assertThat(state.schemaFingerprint()).isNotNull();
  }

  @Test
  void appendEmitsFragmentSplits() throws Throwable {
    commitOverwrite(writeFragment(rows(1, 2)));
    ContinuousLanceSourceEnumerator e = newEnumerator();
    e.start();
    long versionAfterStart = e.snapshotState(0L).lastEnumeratedVersion();

    commitAppend(writeFragment(rows(3, 4)));
    commitAppend(writeFragment(rows(5, 6)));

    DiscoveryResult result = e.discover();
    e.onDiscovered(result, null);
    LanceContinuousEnumState state = e.snapshotState(1L);
    assertThat(state.lastEnumeratedVersion()).isGreaterThan(versionAfterStart);
    assertThat(state.remainingSplits()).hasSize(2);
  }

  @Test
  void noNewVersionEmitsNothing() throws Throwable {
    commitOverwrite(writeFragment(rows(1, 2)));
    ContinuousLanceSourceEnumerator e = newEnumerator();
    e.start();
    long version = e.snapshotState(0L).lastEnumeratedVersion();
    DiscoveryResult result = e.discover();
    e.onDiscovered(result, null);
    LanceContinuousEnumState state = e.snapshotState(1L);
    assertThat(state.lastEnumeratedVersion()).isEqualTo(version);
    assertThat(state.remainingSplits()).isEmpty();
  }

  @Test
  void fragmentRemovalFailsTheJob() {
    // Lance recycles fragment id 0 across Overwrites, so we need the fragment *count* to shrink
    // for the removal to be visible to fragment-diff — hence the extra Append in the baseline.
    commitOverwrite(writeFragment(rows(1, 2)));
    commitAppend(writeFragment(rows(3, 4)));
    ContinuousLanceSourceEnumerator e = newEnumerator();
    e.start();

    commitOverwrite(writeFragment(rows(7, 8, 9)));

    assertThatThrownBy(e::discover)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("fragment removals")
        .hasMessageContaining("compaction");
  }

  @Test
  void schemaDriftFailsDiscovery() {
    commitOverwrite(writeFragment(rows(1)));
    LanceContinuousEnumState seed =
        new LanceContinuousEnumState(0L, Set.of(), List.of(), "fingerprint-from-an-older-schema");
    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx, options(), LanceContinuousOptions.fromConfig(new Configuration()), seed);
    e.start();
    assertThatThrownBy(e::discover)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("schema change");
  }

  @Test
  void restoredEnumeratorPreservesPendingSplits() {
    commitOverwrite(writeFragment(rows(1, 2)));
    LanceSourceSplit pending = new LanceSourceSplit(1L, 0, 0L);
    LanceContinuousEnumState seed =
        new LanceContinuousEnumState(5L, Set.of(0, 1), List.of(pending), "fp-x");
    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx, options(), LanceContinuousOptions.fromConfig(new Configuration()), seed);
    e.start();
    LanceContinuousEnumState round = e.snapshotState(0L);
    assertThat(round.lastEnumeratedVersion()).isEqualTo(5L);
    assertThat(round.knownFragmentIds()).containsExactlyInAnyOrder(0, 1);
    assertThat(round.remainingSplits()).containsExactly(pending);
    assertThat(round.schemaFingerprint()).isEqualTo("fp-x");
  }

  @Test
  void latestFullEmitsInitialSplits() {
    commitOverwrite(writeFragment(rows(1, 2)));
    commitAppend(writeFragment(rows(3, 4)));

    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx,
            options(),
            LanceContinuousOptions.fromConfig(configWith("scan.startup-mode", "latest-full")));
    e.start();

    LanceContinuousEnumState state = e.snapshotState(0L);
    // Two fragments at HEAD → both emitted as initial splits, both tracked as known.
    assertThat(state.remainingSplits()).hasSize(2);
    assertThat(state.knownFragmentIds()).hasSize(2);
    assertThat(state.lastEnumeratedVersion()).isPositive();
  }

  @Test
  void latestFullDoesNotRediscoverIfNoNewVersion() throws Throwable {
    commitOverwrite(writeFragment(rows(1, 2)));

    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx,
            options(),
            LanceContinuousOptions.fromConfig(configWith("scan.startup-mode", "latest-full")));
    e.start();
    int initialSplits = e.snapshotState(0L).remainingSplits().size();
    assertThat(initialSplits).isEqualTo(1);

    DiscoveryResult result = e.discover();
    e.onDiscovered(result, null);
    // No commit between start() and discover() → no new splits, no change to knownFragmentIds.
    LanceContinuousEnumState state = e.snapshotState(1L);
    assertThat(state.remainingSplits()).hasSize(1);
    assertThat(state.knownFragmentIds()).hasSize(1);
  }

  @Test
  void fromSnapshotPinsLastEnumeratedVersion() {
    commitOverwrite(writeFragment(rows(1, 2))); // v1, frag 0
    commitAppend(writeFragment(rows(3, 4))); // v2, frag 0+1
    commitAppend(writeFragment(rows(5, 6))); // v3, frag 0+1+2

    Configuration opts = configWith("scan.startup-mode", "from-snapshot");
    opts.setString("scan.startup-snapshot-id", "2");
    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx, options(), LanceContinuousOptions.fromConfig(opts));
    e.start();

    LanceContinuousEnumState state = e.snapshotState(0L);
    assertThat(state.lastEnumeratedVersion()).isEqualTo(2L);
    // FROM_SNAPSHOT (no -full): start emits no splits but seeds knownFragmentIds from v2.
    assertThat(state.remainingSplits()).isEmpty();
    assertThat(state.knownFragmentIds()).hasSize(2);
  }

  @Test
  void fromSnapshotFullEmitsBaselineAtPinnedVersion() {
    commitOverwrite(writeFragment(rows(1, 2))); // v1, frag 0
    commitAppend(writeFragment(rows(3, 4))); // v2, frag 0+1
    commitAppend(writeFragment(rows(5, 6))); // v3, frag 0+1+2

    Configuration opts = configWith("scan.startup-mode", "from-snapshot-full");
    opts.setString("scan.startup-snapshot-id", "2");
    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx, options(), LanceContinuousOptions.fromConfig(opts));
    e.start();

    LanceContinuousEnumState state = e.snapshotState(0L);
    assertThat(state.lastEnumeratedVersion()).isEqualTo(2L);
    // FROM_SNAPSHOT_FULL: baseline = every fragment at v2.
    assertThat(state.remainingSplits()).hasSize(2);
    state.remainingSplits().forEach(s -> assertThat(s.datasetVersion()).isEqualTo(2L));
    assertThat(state.knownFragmentIds()).hasSize(2);
  }

  @Test
  void fromTagResolvesViaLanceTag() throws Exception {
    commitOverwrite(writeFragment(rows(1, 2))); // v1
    commitAppend(writeFragment(rows(3, 4))); // v2

    try (BufferAllocator alloc = new RootAllocator();
        Dataset ds = LanceDatasetOpener.open(alloc, datasetUri)) {
      ds.tags().create("release", 1L);
    }

    Configuration opts = configWith("scan.startup-mode", "from-tag");
    opts.setString("scan.startup-tag-name", "release");
    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx, options(), LanceContinuousOptions.fromConfig(opts));
    e.start();

    LanceContinuousEnumState state = e.snapshotState(0L);
    assertThat(state.lastEnumeratedVersion()).isEqualTo(1L);
    assertThat(state.knownFragmentIds()).hasSize(1);
    assertThat(state.remainingSplits()).isEmpty();
  }

  @Test
  void restoredFingerprintIsDerivedFromBaselineVersionNotHead() throws Throwable {
    commitOverwrite(writeFragment(rows(1, 2))); // v1, frag 0

    // Lazy-init must read from v1, not HEAD, so a later SchemaOperation is caught as drift.
    String v1Fingerprint;
    try (BufferAllocator alloc = new RootAllocator();
        Dataset v1 = LanceDatasetOpener.open(alloc, datasetUri, 1L)) {
      v1Fingerprint = v1.getSchema().toString();
    }

    LanceContinuousEnumState seed =
        new LanceContinuousEnumState(1L, Set.of(0), List.of(), /* fingerprint */ null);
    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx, options(), LanceContinuousOptions.fromConfig(new Configuration()), seed);
    e.start();
    // First discover() lazy-inits the fingerprint by opening v1 (the baseline), not HEAD.
    DiscoveryResult result = e.discover();
    e.onDiscovered(result, null);

    assertThat(e.schemaFingerprintForTest()).isEqualTo(v1Fingerprint);
  }

  @Test
  void restoredEnumeratorDoesNotRediscoverKnownFragments() throws Throwable {
    commitOverwrite(writeFragment(rows(1, 2))); // v1, frag 0
    commitAppend(writeFragment(rows(3, 4))); // v2, frag 0+1

    // The restored enumerator must NOT re-emit fragments already in knownFragmentIds, even
    // though discover() re-opens the dataset.
    LanceContinuousEnumState seed =
        new LanceContinuousEnumState(2L, Set.of(0, 1), List.of(), /* fingerprint lazy */ null);
    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx, options(), LanceContinuousOptions.fromConfig(new Configuration()), seed);
    e.start();

    DiscoveryResult result = e.discover();
    e.onDiscovered(result, null);
    LanceContinuousEnumState round = e.snapshotState(0L);
    assertThat(round.lastEnumeratedVersion()).isEqualTo(2L);
    assertThat(round.knownFragmentIds()).containsExactlyInAnyOrder(0, 1);
    assertThat(round.remainingSplits()).isEmpty();
    // Lazy-init populates the fingerprint from the current dataset.
    assertThat(round.schemaFingerprint()).isNotNull();
  }

  @Test
  void restoredEnumeratorEmitsOnlyNewFragmentsAfterRestore() throws Throwable {
    commitOverwrite(writeFragment(rows(1, 2))); // v1, frag 0
    commitAppend(writeFragment(rows(3, 4))); // v2, frag 0+1

    LanceContinuousEnumState seed = new LanceContinuousEnumState(2L, Set.of(0, 1), List.of(), null);
    ContinuousLanceSourceEnumerator e =
        new ContinuousLanceSourceEnumerator(
            ctx, options(), LanceContinuousOptions.fromConfig(new Configuration()), seed);
    e.start();

    commitAppend(writeFragment(rows(5, 6))); // v3, frag 0+1+2

    DiscoveryResult result = e.discover();
    e.onDiscovered(result, null);
    LanceContinuousEnumState round = e.snapshotState(0L);
    assertThat(round.lastEnumeratedVersion()).isEqualTo(3L);
    // Only fragment id 2 is new — fragments 0 and 1 were already known.
    assertThat(round.remainingSplits()).hasSize(1);
    LanceSourceSplit newSplit = round.remainingSplits().get(0);
    assertThat(newSplit.fragmentId()).isEqualTo(2);
    assertThat(round.knownFragmentIds()).containsExactlyInAnyOrder(0, 1, 2);
  }

  // -- Helpers --

  private ContinuousLanceSourceEnumerator newEnumerator() {
    return new ContinuousLanceSourceEnumerator(
        ctx, options(), LanceContinuousOptions.fromConfig(new Configuration()));
  }

  private LanceOptions options() {
    return LanceOptions.builder().path(datasetUri).build();
  }

  private static Configuration configWith(String key, String value) {
    Configuration cfg = new Configuration();
    cfg.setString(key, value);
    return cfg;
  }

  private static List<RowData> rows(int... values) {
    return java.util.Arrays.stream(values)
        .mapToObj(GenericRowData::of)
        .map(r -> (RowData) r)
        .toList();
  }

  private List<FragmentMetadata> writeFragment(List<RowData> data) {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, alloc)) {
      CONVERTER.toVectorSchemaRoot(data, root);
      return Fragment.write().datasetUri(datasetUri).allocator(alloc).data(root).execute();
    }
  }

  private void commitOverwrite(List<FragmentMetadata> fragments) {
    try (BufferAllocator alloc = new RootAllocator();
        Transaction tx =
            new Transaction.Builder()
                .operation(Overwrite.builder().fragments(fragments).schema(SCHEMA).build())
                .build();
        Dataset ignored = new CommitBuilder(datasetUri, alloc).execute(tx)) {}
  }

  private void commitAppend(List<FragmentMetadata> fragments) {
    try (BufferAllocator alloc = new RootAllocator();
        Transaction tx =
            new Transaction.Builder()
                .operation(Append.builder().fragments(fragments).build())
                .build();
        Dataset ignored = new CommitBuilder(datasetUri, alloc).execute(tx)) {}
  }
}
