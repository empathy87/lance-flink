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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Source scan options for selecting a Lance dataset version to read. */
public final class LanceScanOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  public enum Mode {
    LATEST,
    VERSION,
    TAG_NAME,
    TIMESTAMP_MILLIS
  }

  public static final ConfigOption<Long> SCAN_VERSION =
      ConfigOptions.key("scan.version")
          .longType()
          .noDefaultValue()
          .withDescription(
              "Read the dataset at this Lance version id. Mutually exclusive with"
                  + " scan.snapshot-id, scan.tag-name, scan.timestamp-millis, and scan.timestamp.");

  /** Paimon-compatible alias for {@link #SCAN_VERSION}. */
  // TODO: Decide whether to keep scan.snapshot-id as a permanent compatibility alias.
  public static final ConfigOption<Long> SCAN_SNAPSHOT_ID =
      ConfigOptions.key("scan.snapshot-id")
          .longType()
          .noDefaultValue()
          .withDescription(
              "Paimon-compatible alias for scan.version. Mutually exclusive with scan.version,"
                  + " scan.tag-name, scan.timestamp-millis, and scan.timestamp.");

  public static final ConfigOption<String> SCAN_TAG_NAME =
      ConfigOptions.key("scan.tag-name")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Read the dataset at the version pointed to by this Lance tag. Mutually exclusive"
                  + " with the other scan.* time-travel options.");

  public static final ConfigOption<Long> SCAN_TIMESTAMP_MILLIS =
      ConfigOptions.key("scan.timestamp-millis")
          .longType()
          .noDefaultValue()
          .withDescription(
              "Read the latest dataset version whose commit time is at or before this UTC"
                  + " epoch-millis value. Mutually exclusive with the other scan.* time-travel"
                  + " options.");

  public static final ConfigOption<String> SCAN_TIMESTAMP =
      ConfigOptions.key("scan.timestamp")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Same as scan.timestamp-millis, but accepts a string. Parsed as UTC unless an"
                  + " explicit offset is provided. Supported formats: 'yyyy-MM-dd',"
                  + " 'yyyy-MM-dd HH:mm:ss', 'yyyy-MM-dd''T''HH:mm:ss', and ISO-8601 with"
                  + " offset. Use scan.timestamp-millis to avoid timezone ambiguity.");

  // TODO: Add scan.branch once split-level branch context is supported.
  public static final Set<ConfigOption<?>> ALL_OPTIONS =
      Set.of(SCAN_VERSION, SCAN_SNAPSHOT_ID, SCAN_TAG_NAME, SCAN_TIMESTAMP_MILLIS, SCAN_TIMESTAMP);

  private final Mode mode;
  private final long version;
  private final @Nullable String tagName;
  private final long timestampMillis;

  private LanceScanOptions(
      Mode mode, long version, @Nullable String tagName, long timestampMillis) {
    this.mode = Objects.requireNonNull(mode, "mode");
    this.version = version;
    this.tagName = tagName;
    this.timestampMillis = timestampMillis;
  }

  public static LanceScanOptions latest() {
    return new LanceScanOptions(Mode.LATEST, 0L, null, 0L);
  }

  public static LanceScanOptions version(long version) {
    if (version < 1) {
      throw new IllegalArgumentException("version must be positive: " + version);
    }
    return new LanceScanOptions(Mode.VERSION, version, null, 0L);
  }

  public static LanceScanOptions tagName(String tagName) {
    return new LanceScanOptions(Mode.TAG_NAME, 0L, requireNonBlank(tagName, "tagName"), 0L);
  }

  public static LanceScanOptions timestampMillis(long timestampMillis) {
    return new LanceScanOptions(Mode.TIMESTAMP_MILLIS, 0L, null, timestampMillis);
  }

  public static LanceScanOptions timestamp(String rawTimestamp) {
    return timestampMillis(parseTimestamp(rawTimestamp));
  }

  /** Parses and validates Lance time-travel scan options. */
  public static LanceScanOptions fromConfig(ReadableConfig config) {
    Long versionValue = config.getOptional(SCAN_VERSION).orElse(null);
    Long snapshotIdValue = config.getOptional(SCAN_SNAPSHOT_ID).orElse(null);
    String tag = config.getOptional(SCAN_TAG_NAME).orElse(null);
    Long timestampMs = config.getOptional(SCAN_TIMESTAMP_MILLIS).orElse(null);
    String timestampStr = config.getOptional(SCAN_TIMESTAMP).orElse(null);

    List<String> setKeys = new ArrayList<>(5);
    if (versionValue != null) setKeys.add(SCAN_VERSION.key());
    if (snapshotIdValue != null) setKeys.add(SCAN_SNAPSHOT_ID.key());
    if (tag != null) setKeys.add(SCAN_TAG_NAME.key());
    if (timestampMs != null) setKeys.add(SCAN_TIMESTAMP_MILLIS.key());
    if (timestampStr != null) setKeys.add(SCAN_TIMESTAMP.key());
    if (setKeys.size() > 1) {
      throw new IllegalArgumentException(
          "Only one Lance time-travel option may be set. Supported options are: "
              + SCAN_VERSION.key()
              + ", "
              + SCAN_SNAPSHOT_ID.key()
              + ", "
              + SCAN_TAG_NAME.key()
              + ", "
              + SCAN_TIMESTAMP_MILLIS.key()
              + ", "
              + SCAN_TIMESTAMP.key()
              + ". Set options: "
              + String.join(", ", setKeys));
    }

    if (versionValue != null) {
      return version(versionValue);
    }
    if (snapshotIdValue != null) {
      return version(snapshotIdValue);
    }
    if (tag != null) {
      return tagName(tag);
    }
    if (timestampMs != null) {
      return timestampMillis(timestampMs);
    }
    if (timestampStr != null) {
      return timestamp(timestampStr);
    }
    return latest();
  }

  public Mode getMode() {
    return mode;
  }

  /** Lance version id; only valid when {@link #getMode()} is {@link Mode#VERSION}. */
  public long getVersion() {
    requireMode(Mode.VERSION);
    return version;
  }

  /** Tag name; only valid when {@link #getMode()} is {@link Mode#TAG_NAME}. */
  public String getTagName() {
    requireMode(Mode.TAG_NAME);
    return Objects.requireNonNull(tagName, "tagName");
  }

  /** Resolved millis; only valid when {@link #getMode()} is {@link Mode#TIMESTAMP_MILLIS}. */
  public long getTimestampMillis() {
    requireMode(Mode.TIMESTAMP_MILLIS);
    return timestampMillis;
  }

  private void requireMode(Mode expected) {
    if (mode != expected) {
      throw new IllegalStateException(
          "Expected scan mode " + expected + " but options are in mode " + mode);
    }
  }

  private static long parseTimestamp(String rawTimestamp) {
    String raw = requireNonBlank(rawTimestamp, "timestamp").trim();

    // ISO timestamp with offset: "2024-01-01T10:15:30Z" or "2024-01-01T10:15:30+02:00".
    try {
      return OffsetDateTime.parse(raw).toInstant().toEpochMilli();
    } catch (DateTimeParseException ignored) {
      // Try next supported format.
    }

    // ISO local date-time: "2024-01-01T10:15:30" treated as UTC.
    try {
      return LocalDateTime.parse(raw).toInstant(ZoneOffset.UTC).toEpochMilli();
    } catch (DateTimeParseException ignored) {
      // Try next supported format.
    }

    // Local date-time with space separator: "2024-01-01 10:15:30" treated as UTC.
    if (raw.contains(" ")) {
      try {
        return LocalDateTime.parse(raw.replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli();
      } catch (DateTimeParseException ignored) {
        // Try next supported format.
      }
    }

    // Date-only: "2024-01-01" treated as 00:00:00 UTC.
    try {
      return LocalDate.parse(raw).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "scan.timestamp '"
              + raw
              + "' could not be parsed; expected 'yyyy-MM-dd', 'yyyy-MM-dd HH:mm:ss', "
              + "'yyyy-MM-dd''T''HH:mm:ss', or ISO-8601 with offset",
          e);
    }
  }

  private static String requireNonBlank(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LanceScanOptions that)) return false;
    return mode == that.mode
        && version == that.version
        && timestampMillis == that.timestampMillis
        && Objects.equals(tagName, that.tagName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, version, tagName, timestampMillis);
  }

  @Override
  public String toString() {
    return switch (mode) {
      case LATEST -> "LanceScanOptions{LATEST}";
      case VERSION -> "LanceScanOptions{version=" + version + "}";
      case TAG_NAME -> "LanceScanOptions{tagName='" + tagName + "'}";
      case TIMESTAMP_MILLIS -> "LanceScanOptions{timestampMillis=" + timestampMillis + "}";
    };
  }
}
