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

import org.apache.flink.connector.lance.source.scan.LanceScanOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Continuous-source discovery and startup options. */
public final class LanceContinuousOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final ConfigOption<Duration> CONTINUOUS_DISCOVERY_INTERVAL =
      ConfigOptions.key("continuous.discovery-interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(10))
          .withDescription(
              "Interval at which the continuous enumerator polls the Lance dataset for new"
                  + " versions. Only used when scan.mode = continuous.");

  public static final ConfigOption<String> SCAN_STARTUP_MODE =
      ConfigOptions.key("scan.startup-mode")
          .stringType()
          .defaultValue(StartupMode.LATEST.configValue())
          .withDescription(
              "Continuous-mode startup position. One of: latest, latest-full, from-snapshot,"
                  + " from-snapshot-full, from-tag, from-tag-full, from-timestamp,"
                  + " from-timestamp-full, from-timestamp-millis, from-timestamp-millis-full."
                  + " The '-full' variants emit the initial snapshot as INSERT rows before"
                  + " starting incremental discovery.");

  public static final ConfigOption<Long> SCAN_STARTUP_SNAPSHOT_ID =
      ConfigOptions.key("scan.startup-snapshot-id")
          .longType()
          .noDefaultValue()
          .withDescription(
              "Required when scan.startup-mode is from-snapshot or from-snapshot-full.");

  public static final ConfigOption<String> SCAN_STARTUP_TAG_NAME =
      ConfigOptions.key("scan.startup-tag-name")
          .stringType()
          .noDefaultValue()
          .withDescription("Required when scan.startup-mode is from-tag or from-tag-full.");

  public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
      ConfigOptions.key("scan.startup-timestamp-millis")
          .longType()
          .noDefaultValue()
          .withDescription(
              "Required when scan.startup-mode is from-timestamp-millis or"
                  + " from-timestamp-millis-full. UTC epoch millis.");

  public static final ConfigOption<String> SCAN_STARTUP_TIMESTAMP =
      ConfigOptions.key("scan.startup-timestamp")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Required when scan.startup-mode is from-timestamp or from-timestamp-full."
                  + " ISO-8601 or 'yyyy-MM-dd HH:mm:ss' (UTC default).");

  public static final Set<ConfigOption<?>> ALL_OPTIONS =
      Set.of(
          CONTINUOUS_DISCOVERY_INTERVAL,
          SCAN_STARTUP_MODE,
          SCAN_STARTUP_SNAPSHOT_ID,
          SCAN_STARTUP_TAG_NAME,
          SCAN_STARTUP_TIMESTAMP_MILLIS,
          SCAN_STARTUP_TIMESTAMP);

  private final Duration discoveryInterval;
  private final StartupMode startupMode;
  private final LanceScanOptions startupScanOptions;

  private LanceContinuousOptions(
      Duration discoveryInterval, StartupMode startupMode, LanceScanOptions startupScanOptions) {
    this.discoveryInterval = Objects.requireNonNull(discoveryInterval, "discoveryInterval");
    this.startupMode = Objects.requireNonNull(startupMode, "startupMode");
    this.startupScanOptions = Objects.requireNonNull(startupScanOptions, "startupScanOptions");
  }

  public static LanceContinuousOptions fromConfig(ReadableConfig config) {
    Duration discoveryInterval = config.get(CONTINUOUS_DISCOVERY_INTERVAL);
    if (discoveryInterval.isZero() || discoveryInterval.isNegative()) {
      throw new IllegalArgumentException(
          CONTINUOUS_DISCOVERY_INTERVAL.key() + " must be positive: " + discoveryInterval);
    }
    StartupMode startupMode = StartupMode.fromString(config.get(SCAN_STARTUP_MODE));
    Long snapshotId = config.getOptional(SCAN_STARTUP_SNAPSHOT_ID).orElse(null);
    String tagName = blankToNull(config.getOptional(SCAN_STARTUP_TAG_NAME).orElse(null));
    Long timestampMillis = config.getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS).orElse(null);
    String timestamp = blankToNull(config.getOptional(SCAN_STARTUP_TIMESTAMP).orElse(null));
    LanceScanOptions startupScanOptions =
        buildStartupScanOptions(startupMode, snapshotId, tagName, timestampMillis, timestamp);
    return new LanceContinuousOptions(discoveryInterval, startupMode, startupScanOptions);
  }

  @Nullable
  private static String blankToNull(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim();
  }

  private static LanceScanOptions buildStartupScanOptions(
      StartupMode mode,
      @Nullable Long snapshotId,
      @Nullable String tagName,
      @Nullable Long timestampMillis,
      @Nullable String timestamp) {
    List<String> setKeys = new ArrayList<>(4);
    if (snapshotId != null) setKeys.add(SCAN_STARTUP_SNAPSHOT_ID.key());
    if (tagName != null) setKeys.add(SCAN_STARTUP_TAG_NAME.key());
    if (timestampMillis != null) setKeys.add(SCAN_STARTUP_TIMESTAMP_MILLIS.key());
    if (timestamp != null) setKeys.add(SCAN_STARTUP_TIMESTAMP.key());
    if (setKeys.size() > 1) {
      throw new IllegalArgumentException(
          "At most one Lance continuous startup selector may be set. Set keys: "
              + String.join(", ", setKeys));
    }
    String setKey = setKeys.isEmpty() ? null : setKeys.get(0);

    return switch (mode) {
      case LATEST, LATEST_FULL -> {
        requireMatchingSelector(mode, setKey, null);
        yield LanceScanOptions.latest();
      }
      case FROM_SNAPSHOT, FROM_SNAPSHOT_FULL -> {
        requireMatchingSelector(mode, setKey, SCAN_STARTUP_SNAPSHOT_ID);
        yield LanceScanOptions.version(snapshotId);
      }
      case FROM_TAG, FROM_TAG_FULL -> {
        requireMatchingSelector(mode, setKey, SCAN_STARTUP_TAG_NAME);
        yield LanceScanOptions.tagName(tagName);
      }
      case FROM_TIMESTAMP, FROM_TIMESTAMP_FULL -> {
        requireMatchingSelector(mode, setKey, SCAN_STARTUP_TIMESTAMP);
        yield LanceScanOptions.timestamp(timestamp);
      }
      case FROM_TIMESTAMP_MILLIS, FROM_TIMESTAMP_MILLIS_FULL -> {
        requireMatchingSelector(mode, setKey, SCAN_STARTUP_TIMESTAMP_MILLIS);
        yield LanceScanOptions.timestampMillis(timestampMillis);
      }
    };
  }

  private static void requireMatchingSelector(
      StartupMode mode, @Nullable String setKey, @Nullable ConfigOption<?> required) {
    if (required == null) {
      if (setKey != null) {
        throw new IllegalArgumentException(
            "scan.startup-mode '"
                + mode.configValue()
                + "' does not accept a startup selector. Remove: "
                + setKey);
      }
      return;
    }
    String expectedKey = required.key();
    if (setKey == null) {
      throw new IllegalArgumentException(
          "scan.startup-mode '" + mode.configValue() + "' requires " + expectedKey);
    }
    if (!setKey.equals(expectedKey)) {
      throw new IllegalArgumentException(
          "scan.startup-mode '"
              + mode.configValue()
              + "' requires "
              + expectedKey
              + " but got: "
              + setKey);
    }
  }

  public Duration discoveryInterval() {
    return discoveryInterval;
  }

  public StartupMode startupMode() {
    return startupMode;
  }

  /** Scan target for the configured startup mode; {@link LanceScanOptions#latest()} for LATEST. */
  public LanceScanOptions startupScanOptions() {
    return startupScanOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LanceContinuousOptions that)) return false;
    return Objects.equals(discoveryInterval, that.discoveryInterval)
        && startupMode == that.startupMode
        && Objects.equals(startupScanOptions, that.startupScanOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(discoveryInterval, startupMode, startupScanOptions);
  }

  @Override
  public String toString() {
    return "LanceContinuousOptions{discoveryInterval="
        + discoveryInterval
        + ", startupMode="
        + startupMode
        + ", startupScanOptions="
        + startupScanOptions
        + '}';
  }
}
