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

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceScanOptionsTest {

  @Test
  void emptyConfigYieldsLatest() {
    LanceScanOptions options = LanceScanOptions.fromConfig(new Configuration());
    assertThat(options.getMode()).isEqualTo(LanceScanOptions.Mode.LATEST);
    assertThat(options).isEqualTo(LanceScanOptions.latest());
  }

  @Test
  void versionIsParsed() {
    Configuration cfg = new Configuration();
    cfg.set(LanceScanOptions.SCAN_VERSION, 9L);
    LanceScanOptions options = LanceScanOptions.fromConfig(cfg);
    assertThat(options.getMode()).isEqualTo(LanceScanOptions.Mode.VERSION);
    assertThat(options.getVersion()).isEqualTo(9L);
    assertThat(options).isEqualTo(LanceScanOptions.version(9L));
  }

  @Test
  void versionZeroOrNegativeRejected() {
    assertThatThrownBy(() -> LanceScanOptions.version(0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("version must be positive");
    assertThatThrownBy(() -> LanceScanOptions.version(-5L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("version must be positive");
  }

  @Test
  void snapshotIdIsParsedAsVersionAlias() {
    Configuration cfg = new Configuration();
    cfg.set(LanceScanOptions.SCAN_SNAPSHOT_ID, 7L);
    LanceScanOptions options = LanceScanOptions.fromConfig(cfg);
    assertThat(options.getMode()).isEqualTo(LanceScanOptions.Mode.VERSION);
    assertThat(options.getVersion()).isEqualTo(7L);
    assertThat(options).isEqualTo(LanceScanOptions.version(7L));
  }

  @Test
  void tagNameIsParsed() {
    Configuration cfg = new Configuration();
    cfg.set(LanceScanOptions.SCAN_TAG_NAME, "release-1");
    LanceScanOptions options = LanceScanOptions.fromConfig(cfg);
    assertThat(options.getMode()).isEqualTo(LanceScanOptions.Mode.TAG_NAME);
    assertThat(options.getTagName()).isEqualTo("release-1");
  }

  @Test
  void timestampMillisIsParsed() {
    Configuration cfg = new Configuration();
    cfg.set(LanceScanOptions.SCAN_TIMESTAMP_MILLIS, 1_700_000_000_000L);
    LanceScanOptions options = LanceScanOptions.fromConfig(cfg);
    assertThat(options.getMode()).isEqualTo(LanceScanOptions.Mode.TIMESTAMP_MILLIS);
    assertThat(options.getTimestampMillis()).isEqualTo(1_700_000_000_000L);
  }

  @Test
  void timestampStringWithSpaceIsParsedAsUtc() {
    LanceScanOptions options = LanceScanOptions.timestamp("2026-05-08 12:34:56");
    // 2026-05-08T12:34:56Z = 1778243696000
    assertThat(options.getMode()).isEqualTo(LanceScanOptions.Mode.TIMESTAMP_MILLIS);
    assertThat(options.getTimestampMillis()).isEqualTo(1778243696000L);
  }

  @Test
  void timestampStringWithTSeparatorIsParsedAsUtc() {
    LanceScanOptions options = LanceScanOptions.timestamp("2026-05-08T12:34:56");
    assertThat(options.getTimestampMillis()).isEqualTo(1778243696000L);
  }

  @Test
  void timestampStringWithOffsetIsParsedExactly() {
    LanceScanOptions options = LanceScanOptions.timestamp("2026-05-08T12:34:56+02:00");
    // 2026-05-08T10:34:56Z = 1778236496000
    assertThat(options.getTimestampMillis()).isEqualTo(1778236496000L);
  }

  @Test
  void timestampStringWithZuluOffsetIsParsedAsUtc() {
    LanceScanOptions options = LanceScanOptions.timestamp("2026-05-08T12:34:56Z");
    assertThat(options.getTimestampMillis()).isEqualTo(1778243696000L);
  }

  @Test
  void dateOnlyStringIsParsedAsUtcMidnight() {
    LanceScanOptions options = LanceScanOptions.timestamp("2026-05-08");
    // 2026-05-08T00:00:00Z = 1778198400000
    assertThat(options.getMode()).isEqualTo(LanceScanOptions.Mode.TIMESTAMP_MILLIS);
    assertThat(options.getTimestampMillis()).isEqualTo(1778198400000L);
  }

  @Test
  void garbageTimestampIsRejected() {
    assertThatThrownBy(() -> LanceScanOptions.timestamp("not a date"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scan.timestamp");
  }

  @Test
  void blankTagIsRejected() {
    assertThatThrownBy(() -> LanceScanOptions.tagName("  "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void mutualExclusionVersionAndSnapshotId() {
    Configuration cfg = new Configuration();
    cfg.set(LanceScanOptions.SCAN_VERSION, 1L);
    cfg.set(LanceScanOptions.SCAN_SNAPSHOT_ID, 1L);
    assertThatThrownBy(() -> LanceScanOptions.fromConfig(cfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Only one Lance time-travel option may be set")
        .hasMessageContaining("Set options: scan.version, scan.snapshot-id");
  }

  @Test
  void mutualExclusionVersionAndTagName() {
    Configuration cfg = new Configuration();
    cfg.set(LanceScanOptions.SCAN_VERSION, 1L);
    cfg.set(LanceScanOptions.SCAN_TAG_NAME, "old");
    assertThatThrownBy(() -> LanceScanOptions.fromConfig(cfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Only one Lance time-travel option may be set")
        .hasMessageContaining("scan.version")
        .hasMessageContaining("scan.tag-name");
  }

  @Test
  void mutualExclusionAllFive() {
    Configuration cfg = new Configuration();
    cfg.set(LanceScanOptions.SCAN_VERSION, 1L);
    cfg.set(LanceScanOptions.SCAN_SNAPSHOT_ID, 1L);
    cfg.set(LanceScanOptions.SCAN_TAG_NAME, "t");
    cfg.set(LanceScanOptions.SCAN_TIMESTAMP_MILLIS, 100L);
    cfg.set(LanceScanOptions.SCAN_TIMESTAMP, "2026-05-08 00:00:00");
    assertThatThrownBy(() -> LanceScanOptions.fromConfig(cfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Only one Lance time-travel option may be set")
        .hasMessageContaining("Supported options are:")
        .hasMessageContaining("scan.version")
        .hasMessageContaining("scan.snapshot-id")
        .hasMessageContaining("scan.tag-name")
        .hasMessageContaining("scan.timestamp-millis")
        .hasMessageContaining("scan.timestamp");
  }

  @Test
  void getVersionRequiresMatchingMode() {
    assertThatThrownBy(() -> LanceScanOptions.tagName("foo").getVersion())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expected scan mode VERSION");
  }

  @Test
  void getTagNameRequiresMatchingMode() {
    assertThatThrownBy(() -> LanceScanOptions.version(1L).getTagName())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expected scan mode TAG_NAME");
  }

  @Test
  void getTimestampMillisRequiresMatchingMode() {
    assertThatThrownBy(() -> LanceScanOptions.latest().getTimestampMillis())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expected scan mode TIMESTAMP_MILLIS");
  }

  @Test
  void equalityAndHashCode() {
    assertThat(LanceScanOptions.version(7L)).isEqualTo(LanceScanOptions.version(7L));
    assertThat(LanceScanOptions.version(7L).hashCode())
        .isEqualTo(LanceScanOptions.version(7L).hashCode());
    assertThat(LanceScanOptions.version(7L)).isNotEqualTo(LanceScanOptions.version(8L));
    assertThat(LanceScanOptions.tagName("a")).isNotEqualTo(LanceScanOptions.tagName("b"));
    assertThat(LanceScanOptions.latest()).isNotEqualTo(LanceScanOptions.version(1L));
    // Timestamp built from millis or from the string form with the same epoch are equal: both
    // discard the input shape once parsed.
    assertThat(LanceScanOptions.timestampMillis(1778243696000L))
        .isEqualTo(LanceScanOptions.timestamp("2026-05-08 12:34:56"));
  }

  @Test
  void toStringIsReadable() {
    assertThat(LanceScanOptions.latest().toString()).isEqualTo("LanceScanOptions{LATEST}");
    assertThat(LanceScanOptions.version(7L).toString()).isEqualTo("LanceScanOptions{version=7}");
    assertThat(LanceScanOptions.tagName("rel").toString())
        .isEqualTo("LanceScanOptions{tagName='rel'}");
    assertThat(LanceScanOptions.timestampMillis(123L).toString())
        .isEqualTo("LanceScanOptions{timestampMillis=123}");
    assertThat(LanceScanOptions.timestamp("2026-05-08").toString())
        .isEqualTo("LanceScanOptions{timestampMillis=1778198400000}");
  }
}
