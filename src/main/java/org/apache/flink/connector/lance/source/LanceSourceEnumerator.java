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
import org.apache.flink.connector.lance.source.assigner.SimpleSplitAssigner;
import org.apache.flink.connector.lance.source.assigner.SplitAssigner;
import org.apache.flink.connector.lance.source.scan.LanceScanOptions;
import org.apache.flink.connector.lance.source.scan.LanceScanVersionResolver;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ReadOptions;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Discovers Lance fragments and assigns one split per fragment. */
public class LanceSourceEnumerator
    implements SplitEnumerator<LanceSourceSplit, LanceSourceEnumState> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceSourceEnumerator.class);

  private final SplitEnumeratorContext<LanceSourceSplit> context;
  private final LanceOptions options;
  private final LanceScanOptions scanOptions;
  private final SplitAssigner assigner;
  private final boolean restored;

  public LanceSourceEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> context, LanceOptions options) {
    this(context, options, LanceScanOptions.latest(), Collections.emptyList(), false);
  }

  public LanceSourceEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> context,
      LanceOptions options,
      LanceScanOptions scanOptions) {
    this(context, options, scanOptions, Collections.emptyList(), false);
  }

  public LanceSourceEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> context,
      LanceOptions options,
      Collection<LanceSourceSplit> remainingSplits) {
    this(context, options, LanceScanOptions.latest(), remainingSplits, true);
  }

  public LanceSourceEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> context,
      LanceOptions options,
      LanceScanOptions scanOptions,
      Collection<LanceSourceSplit> remainingSplits) {
    this(context, options, scanOptions, remainingSplits, true);
  }

  private LanceSourceEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> context,
      LanceOptions options,
      LanceScanOptions scanOptions,
      Collection<LanceSourceSplit> remainingSplits,
      boolean restored) {
    this.context = Objects.requireNonNull(context, "context");
    this.options = Objects.requireNonNull(options, "options");
    this.scanOptions = scanOptions == null ? LanceScanOptions.latest() : scanOptions;
    this.assigner =
        new SimpleSplitAssigner(Objects.requireNonNull(remainingSplits, "remainingSplits"));
    this.restored = restored;
  }

  @Override
  public void start() {
    if (restored) {
      LOG.info(
          "Restored Lance source enumerator with {} pending splits",
          assigner.remainingSplits().size());
      return;
    }
    List<LanceSourceSplit> discovered = discoverSplits();
    LOG.info("Lance source enumerator discovered {} splits", discovered.size());
    assigner.addSplits(discovered);
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    if (!context.registeredReaders().containsKey(subtaskId)) {
      return;
    }
    Optional<LanceSourceSplit> next = assigner.getNext();
    if (next.isPresent()) {
      context.assignSplit(next.get(), subtaskId);
    } else {
      context.signalNoMoreSplits(subtaskId);
    }
  }

  @Override
  public void addSplitsBack(List<LanceSourceSplit> splits, int subtaskId) {
    assigner.addSplits(splits);
  }

  @Override
  public void addReader(int subtaskId) {}

  @Override
  public LanceSourceEnumState snapshotState(long checkpointId) {
    return new LanceSourceEnumState(assigner.remainingSplits());
  }

  @Override
  public void close() {}

  private List<LanceSourceSplit> discoverSplits() {
    String path = options.getPath();
    if (path == null || path.isBlank()) {
      throw new IllegalArgumentException("Lance dataset path cannot be empty");
    }

    List<LanceSourceSplit> splits = new ArrayList<>();
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset latest = openDataset(path, alloc, null)) {
      long resolvedVersion = LanceScanVersionResolver.resolveVersion(scanOptions, latest);

      if (resolvedVersion == latest.version()) {
        addFragments(latest, resolvedVersion, splits);
      } else {
        try (Dataset versioned = openDataset(path, alloc, resolvedVersion)) {
          addFragments(versioned, resolvedVersion, splits);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to enumerate Lance fragments at " + path + " with " + scanOptions, e);
    }

    return splits;
  }

  private static Dataset openDataset(
      String path, BufferAllocator allocator, @Nullable Long version) {
    if (version == null) {
      return Dataset.open().allocator(allocator).uri(path).build();
    }

    ReadOptions readOptions = new ReadOptions.Builder().setVersion(version).build();
    return Dataset.open().readOptions(readOptions).allocator(allocator).uri(path).build();
  }

  private static void addFragments(
      Dataset dataset, long datasetVersion, List<LanceSourceSplit> splits) {
    for (Fragment frag : dataset.getFragments()) {
      splits.add(LanceSourceSplit.fragment(datasetVersion, frag.getId()));
    }
  }
}
