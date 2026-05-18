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
import org.apache.flink.connector.lance.source.LanceSourceSplit;
import org.apache.flink.connector.lance.source.assigner.SimpleSplitAssigner;
import org.apache.flink.connector.lance.source.assigner.SplitAssigner;
import org.apache.flink.connector.lance.source.scan.LanceScanVersionResolver;

import org.lance.Dataset;
import org.lance.Fragment;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** Discovers new Lance fragment ids by polling HEAD and assigning them as source splits. */
public class ContinuousLanceSourceEnumerator
    implements SplitEnumerator<LanceSourceSplit, LanceContinuousEnumState> {

  private static final Logger LOG = LoggerFactory.getLogger(ContinuousLanceSourceEnumerator.class);

  private final SplitEnumeratorContext<LanceSourceSplit> context;
  private final LanceOptions options;
  private final LanceContinuousOptions continuousOptions;
  private final SplitAssigner assigner;
  private final boolean restored;

  private long lastEnumeratedVersion;
  private Set<Integer> knownFragmentIds;
  @Nullable private String schemaFingerprint;
  private int nextReaderIndex;
  private volatile Throwable fatalDiscoveryError;

  public ContinuousLanceSourceEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> context,
      LanceOptions options,
      LanceContinuousOptions continuousOptions) {
    this(
        context,
        options,
        continuousOptions,
        -1L,
        Collections.emptySet(),
        Collections.emptyList(),
        null,
        false);
  }

  public ContinuousLanceSourceEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> context,
      LanceOptions options,
      LanceContinuousOptions continuousOptions,
      LanceContinuousEnumState state) {
    this(
        context,
        options,
        continuousOptions,
        state.lastEnumeratedVersion(),
        state.knownFragmentIds(),
        state.remainingSplits(),
        state.schemaFingerprint(),
        true);
  }

  private ContinuousLanceSourceEnumerator(
      SplitEnumeratorContext<LanceSourceSplit> context,
      LanceOptions options,
      LanceContinuousOptions continuousOptions,
      long lastEnumeratedVersion,
      Collection<Integer> knownFragmentIds,
      Collection<LanceSourceSplit> remainingSplits,
      @Nullable String schemaFingerprint,
      boolean restored) {
    this.context = Objects.requireNonNull(context, "context");
    this.options = Objects.requireNonNull(options, "options");
    this.continuousOptions = Objects.requireNonNull(continuousOptions, "continuousOptions");
    this.lastEnumeratedVersion = lastEnumeratedVersion;
    this.knownFragmentIds =
        new HashSet<>(Objects.requireNonNull(knownFragmentIds, "knownFragmentIds"));
    this.schemaFingerprint = schemaFingerprint;
    this.assigner =
        new SimpleSplitAssigner(Objects.requireNonNull(remainingSplits, "remainingSplits"));
    this.restored = restored;
  }

  @Override
  public void start() {
    if (restored) {
      LOG.info(
          "Restored continuous Lance enumerator at v{} with {} known fragments and {} pending splits",
          lastEnumeratedVersion,
          knownFragmentIds.size(),
          assigner.remainingSplits().size());
    } else {
      DiscoveryResult initial = resolveStartupPosition();
      lastEnumeratedVersion = initial.discoveredUntilVersion();
      assigner.addSplits(initial.splits());
      LOG.info(
          "Started continuous Lance enumerator at v{} with {} known fragments and {} initial splits",
          lastEnumeratedVersion,
          knownFragmentIds.size(),
          initial.splits().size());
    }
    long intervalMs = continuousOptions.discoveryInterval().toMillis();
    context.callAsync(this::discover, this::onDiscovered, intervalMs, intervalMs);
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    if (fatalDiscoveryError != null) {
      throw new RuntimeException(
          "Continuous Lance discovery has failed; the job cannot continue.", fatalDiscoveryError);
    }
    assignPendingSplits();
  }

  @Override
  public void addSplitsBack(List<LanceSourceSplit> splits, int subtaskId) {
    assigner.addSplits(splits);
    assignPendingSplits();
  }

  @Override
  public void addReader(int subtaskId) {
    assignPendingSplits();
  }

  private void assignPendingSplits() {
    if (fatalDiscoveryError != null) {
      return;
    }
    List<Integer> subtasks = new ArrayList<>(context.registeredReaders().keySet());
    if (subtasks.isEmpty()) {
      return;
    }
    Collections.sort(subtasks);
    if (nextReaderIndex >= subtasks.size()) {
      nextReaderIndex = 0;
    }
    while (true) {
      Optional<LanceSourceSplit> nextSplit = assigner.getNext();
      if (nextSplit.isEmpty()) {
        return;
      }
      context.assignSplit(nextSplit.get(), subtasks.get(nextReaderIndex));
      nextReaderIndex = (nextReaderIndex + 1) % subtasks.size();
    }
  }

  @Override
  public LanceContinuousEnumState snapshotState(long checkpointId) {
    return new LanceContinuousEnumState(
        lastEnumeratedVersion, knownFragmentIds, assigner.remainingSplits(), schemaFingerprint);
  }

  @Override
  public void close() {}

  private DiscoveryResult resolveStartupPosition() {
    boolean emitFull = continuousOptions.startupMode().isFull();
    long resolvedVersion;
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset head = LanceDatasetOpener.open(alloc, options.getPath())) {
      resolvedVersion =
          LanceScanVersionResolver.resolveVersion(continuousOptions.startupScanOptions(), head);
    }
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset pinned = LanceDatasetOpener.open(alloc, options.getPath(), resolvedVersion)) {
      this.schemaFingerprint = computeFingerprint(pinned.getSchema());
      this.knownFragmentIds = collectFragmentIds(pinned);
      return new DiscoveryResult(
          resolvedVersion, emitFull ? emitSplitsAtVersion(pinned, resolvedVersion) : List.of());
    }
  }

  /** Fragment-diff discovery. Visible for tests. */
  DiscoveryResult discover() throws Exception {
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset current = LanceDatasetOpener.open(alloc, options.getPath())) {
      long currentVersion = current.version();
      String currentFingerprint = computeFingerprint(current.getSchema());

      if (schemaFingerprint == null) {
        // Derive missing legacy-state fingerprint from the saved baseline, not from HEAD.
        if (lastEnumeratedVersion <= 0L) {
          schemaFingerprint = currentFingerprint;
        } else {
          try (BufferAllocator baselineAlloc = new RootAllocator(Long.MAX_VALUE);
              Dataset baseline =
                  LanceDatasetOpener.open(
                      baselineAlloc, options.getPath(), lastEnumeratedVersion)) {
            schemaFingerprint = computeFingerprint(baseline.getSchema());
          } catch (Exception e) {
            throw new IllegalStateException(
                "Restored Lance continuous state has no schema fingerprint and the baseline"
                    + " version v"
                    + lastEnumeratedVersion
                    + " cannot be re-opened to derive one: "
                    + e.getMessage()
                    + ". Restart the job from a fresh startup position.",
                e);
          }
        }
      }

      if (currentVersion <= lastEnumeratedVersion) {
        return new DiscoveryResult(lastEnumeratedVersion, List.of());
      }

      if (!schemaFingerprint.equals(currentFingerprint)) {
        throw new IllegalStateException(
            "Lance continuous source detected a schema change between v"
                + lastEnumeratedVersion
                + " and v"
                + currentVersion
                + ". Schema evolution is not supported in continuous mode. Restart the job with a"
                + " compatible Flink schema or from a new startup position.");
      }

      Set<Integer> currentIds = collectFragmentIds(current);

      Set<Integer> removed = new HashSet<>(knownFragmentIds);
      removed.removeAll(currentIds);
      // TODO: Use Lance transaction/delta APIs to tolerate compaction and detect overwrites.
      if (!removed.isEmpty()) {
        throw new IllegalStateException(
            "Continuous Lance source observed fragment removals between v"
                + lastEnumeratedVersion
                + " and v"
                + currentVersion
                + ": "
                + new TreeSet<>(removed)
                + ". This is incompatible with append-only continuous mode. Likely causes:"
                + " compaction (Rewrite), Overwrite, Update, Merge, Delete. Pause concurrent"
                + " writers / compaction on this dataset, or use scan.mode=batch instead.");
      }

      Set<Integer> added = new TreeSet<>(currentIds);
      added.removeAll(knownFragmentIds);
      List<LanceSourceSplit> splits = new ArrayList<>(added.size());
      for (Integer id : added) {
        splits.add(LanceSourceSplit.fragment(currentVersion, id));
      }
      return new DiscoveryResult(currentVersion, splits);
    }
  }

  void onDiscovered(DiscoveryResult result, Throwable err) {
    if (err != null) {
      LOG.error("Continuous Lance discovery failed; failing the source coordinator.", err);
      fatalDiscoveryError = err;
      // Rethrow from the async callback so the source coordinator fails even when readers are idle.
      throw new RuntimeException("Continuous Lance discovery failed", err);
    }
    if (result.discoveredUntilVersion() > lastEnumeratedVersion) {
      lastEnumeratedVersion = result.discoveredUntilVersion();
      for (LanceSourceSplit split : result.splits()) {
        knownFragmentIds.add(split.fragmentId());
      }
    }
    if (!result.splits().isEmpty()) {
      assigner.addSplits(result.splits());
      assignPendingSplits();
    }
  }

  private static List<LanceSourceSplit> emitSplitsAtVersion(Dataset ds, long version) {
    List<LanceSourceSplit> out = new ArrayList<>();
    for (Fragment f : ds.getFragments()) {
      out.add(LanceSourceSplit.fragment(version, f.getId()));
    }
    return out;
  }

  private static Set<Integer> collectFragmentIds(Dataset ds) {
    Set<Integer> ids = new HashSet<>();
    for (Fragment f : ds.getFragments()) {
      ids.add(f.getId());
    }
    return ids;
  }

  private static String computeFingerprint(Schema schema) {
    return schema.toString();
  }

  @Nullable
  String schemaFingerprintForTest() {
    return schemaFingerprint;
  }
}
