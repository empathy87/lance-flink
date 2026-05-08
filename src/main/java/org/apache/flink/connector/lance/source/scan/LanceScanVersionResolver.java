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

import org.lance.Dataset;
import org.lance.Version;

import java.util.List;

/** Resolves Lance scan options to a concrete dataset version. */
public final class LanceScanVersionResolver {

  private LanceScanVersionResolver() {}

  public static long resolveVersion(LanceScanOptions options, Dataset dataset) {
    return switch (options.getMode()) {
      case LATEST -> dataset.latestVersion();
      case VERSION -> resolveVersionId(options.getVersion(), dataset);
      case TAG_NAME -> resolveTagName(options.getTagName(), dataset);
      case TIMESTAMP_MILLIS -> resolveTimestampMillis(options.getTimestampMillis(), dataset);
    };
  }

  private static long resolveVersionId(long versionId, Dataset dataset) {
    long latest = dataset.latestVersion();
    if (versionId < 1 || versionId > latest) {
      throw new IllegalArgumentException(
          "Lance dataset version " + versionId + " is out of range; latest version is " + latest);
    }
    boolean exists = dataset.listVersions().stream().anyMatch(v -> v.getId() == versionId);
    if (!exists) {
      throw new IllegalArgumentException(
          "Lance dataset version " + versionId + " is not available; latest version is " + latest);
    }
    return versionId;
  }

  private static long resolveTagName(String tagName, Dataset dataset) {
    try {
      return dataset.tags().getVersion(tagName);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "Lance tag '" + tagName + "' could not be resolved: " + e.getMessage(), e);
    }
  }

  private static long resolveTimestampMillis(long timestampMillis, Dataset dataset) {
    List<Version> versions = dataset.listVersions();
    if (versions.isEmpty()) {
      throw new IllegalArgumentException("Cannot resolve scan timestamp: dataset has no versions");
    }

    long bestId = -1L;
    long bestMillis = Long.MIN_VALUE;
    long oldestMillis = Long.MAX_VALUE;

    for (Version v : versions) {
      long vMillis = v.getDataTime().toInstant().toEpochMilli();
      oldestMillis = Math.min(oldestMillis, vMillis);

      if (vMillis <= timestampMillis
          && (vMillis > bestMillis || (vMillis == bestMillis && v.getId() > bestId))) {
        bestMillis = vMillis;
        bestId = v.getId();
      }
    }

    if (bestId < 0) {
      throw new IllegalArgumentException(
          "scan.timestamp(-millis) "
              + timestampMillis
              + " is older than the dataset's earliest available version ("
              + oldestMillis
              + " ms)");
    }

    return bestId;
  }
}
