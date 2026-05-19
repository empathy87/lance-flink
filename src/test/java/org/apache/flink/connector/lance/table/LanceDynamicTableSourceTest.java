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
package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.lookup.LanceLookupConfig;
import org.apache.flink.connector.lance.source.continuous.LanceContinuousOptions;
import org.apache.flink.connector.lance.source.scan.LanceScanOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceDynamicTableSourceTest {

  @Test
  void defaultConstructorIsBatch() {
    LanceDynamicTableSource s = batchSource(LanceScanOptions.latest(), dataType());
    assertThat(s.isContinuous()).isFalse();
    assertThat(s.getContinuousOptions()).isNull();
    assertThat(s.getScanOptions()).isEqualTo(LanceScanOptions.latest());
  }

  @Test
  void changelogModeIsInsertOnlyInBatchMode() {
    LanceDynamicTableSource s = batchSource(LanceScanOptions.latest(), dataType());
    assertThat(s.getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());
  }

  @Test
  void changelogModeIsInsertOnlyInContinuousMode() {
    LanceContinuousOptions c = LanceContinuousOptions.fromConfig(new Configuration());
    LanceDynamicTableSource s = continuousSource(c, dataType());
    assertThat(s.getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());
    assertThat(s.isContinuous()).isTrue();
    assertThat(s.getContinuousOptions()).isSameAs(c);
  }

  @Test
  void copyPreservesContinuousOptions() {
    LanceContinuousOptions c = LanceContinuousOptions.fromConfig(new Configuration());
    LanceDynamicTableSource s = continuousSource(c, dataType());
    DynamicTableSource copy = s.copy();
    assertThat(copy).isInstanceOf(LanceDynamicTableSource.class);
    assertThat(((LanceDynamicTableSource) copy).getContinuousOptions()).isSameAs(c);
    assertThat(((LanceDynamicTableSource) copy).isContinuous()).isTrue();
  }

  @Test
  void copyPreservesBatchScanOptions() {
    LanceScanOptions scan = LanceScanOptions.version(7);
    LanceDynamicTableSource s = batchSource(scan, dataType());
    DynamicTableSource copy = s.copy();
    assertThat(((LanceDynamicTableSource) copy).getScanOptions()).isEqualTo(scan);
    assertThat(((LanceDynamicTableSource) copy).isContinuous()).isFalse();
  }

  @Test
  void implementsLookupTableSource() {
    LanceDynamicTableSource source = batchSource(LanceScanOptions.latest(), dataType());
    assertThat(source).isInstanceOf(LookupTableSource.class);
  }

  @Test
  void copyPreservesLookupConfig() {
    Configuration cfg = new Configuration();
    cfg.set(org.apache.flink.connector.lance.lookup.LanceLookupOptions.ALLOW_FULL_SCAN, true);
    LanceLookupConfig lookup = LanceLookupConfig.fromConfig(cfg);
    LanceDynamicTableSource s =
        LanceDynamicTableSource.forBatch(options(), LanceScanOptions.latest(), lookup, dataType());
    DynamicTableSource copy = s.copy();
    assertThat(((LanceDynamicTableSource) copy).getLookupConfig().allowFullScan()).isTrue();
  }

  @Test
  void getLookupRuntimeProviderRejectsContinuousMode() {
    LanceContinuousOptions c = LanceContinuousOptions.fromConfig(new Configuration());
    LanceDynamicTableSource s = continuousSource(c, dataType());
    assertThatThrownBy(() -> s.getLookupRuntimeProvider(keysContext(new int[][] {{0}})))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.mode = continuous");
  }

  @Test
  void getLookupRuntimeProviderRejectsNestedKeyPath() {
    LanceDynamicTableSource s = batchSource(LanceScanOptions.latest(), dataType());
    assertThatThrownBy(() -> s.getLookupRuntimeProvider(keysContext(new int[][] {{0, 1}})))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Nested lookup keys are not supported")
        .hasMessageContaining("key path length 2");
  }

  @Test
  void getLookupRuntimeProviderRejectsScanVersion() {
    LanceDynamicTableSource s = batchSource(LanceScanOptions.version(7), dataType());
    assertThatThrownBy(() -> s.getLookupRuntimeProvider(keysContext(new int[][] {{0}})))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("does not honor time-travel scan options")
        .hasMessageContaining("version=7");
  }

  @Test
  void getLookupRuntimeProviderRejectsScanTagName() {
    LanceDynamicTableSource s = batchSource(LanceScanOptions.tagName("v1"), dataType());
    assertThatThrownBy(() -> s.getLookupRuntimeProvider(keysContext(new int[][] {{0}})))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("does not honor time-travel scan options")
        .hasMessageContaining("tagName='v1'");
  }

  @Test
  void getLookupRuntimeProviderRejectsScanTimestampMillis() {
    LanceDynamicTableSource s =
        batchSource(LanceScanOptions.timestampMillis(1_700_000_000_000L), dataType());
    assertThatThrownBy(() -> s.getLookupRuntimeProvider(keysContext(new int[][] {{0}})))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("does not honor time-travel scan options")
        .hasMessageContaining("timestampMillis=1700000000000");
  }

  @Test
  void getLookupRuntimeProviderRejectsPushedLimit() {
    LanceDynamicTableSource s = batchSource(LanceScanOptions.latest(), dataType());
    s.applyLimit(10);
    assertThatThrownBy(() -> s.getLookupRuntimeProvider(keysContext(new int[][] {{0}})))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("cannot honor a pushed LIMIT")
        .hasMessageContaining("10");
  }

  @Test
  void applyProjectionProducesLookupFunctionProvider() {
    // Shallow wiring check — that applyProjection + getLookupRuntimeProvider yields the expected
    // provider type. The deep inspection of projected columns / produced row type lives in the
    // lookup package alongside the LanceLookupFunction accessors.
    LanceDynamicTableSource source =
        batchSource(
            LanceScanOptions.latest(),
            DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("email", DataTypes.STRING())));
    source.applyProjection(
        new int[][] {{2}, {0}},
        DataTypes.ROW(
            DataTypes.FIELD("email", DataTypes.STRING()),
            DataTypes.FIELD("id", DataTypes.BIGINT())));

    LookupTableSource.LookupRuntimeProvider provider =
        source.getLookupRuntimeProvider(keysContext(new int[][] {{1}}));
    assertThat(provider).isInstanceOf(LookupFunctionProvider.class);
  }

  @Test
  void metadataTableSourceRejectsLookupRuntime() {
    LanceMetadataTableSource metadata =
        new LanceMetadataTableSource(
            "/tmp/lance",
            MetadataTableType.SNAPSHOTS,
            java.util.Map.of("connector", "lance", "path", "/tmp/lance"));
    assertThat(metadata).isInstanceOf(LookupTableSource.class);
    assertThatThrownBy(() -> metadata.getLookupRuntimeProvider(keysContext(new int[][] {{0}})))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("metadata-type = snapshots")
        .hasMessageContaining("not supported");
  }

  private static LookupTableSource.LookupContext keysContext(int[][] keys) {
    return new LookupTableSource.LookupContext() {
      @Override
      public int[][] getKeys() {
        return keys;
      }

      @Override
      public <T> org.apache.flink.api.common.typeinfo.TypeInformation<T> createTypeInformation(
          DataType producedDataType) {
        throw new UnsupportedOperationException("not used in tests");
      }

      @Override
      public <T> org.apache.flink.api.common.typeinfo.TypeInformation<T> createTypeInformation(
          org.apache.flink.table.types.logical.LogicalType producedLogicalType) {
        throw new UnsupportedOperationException("not used in tests");
      }

      @Override
      public DynamicTableSource.DataStructureConverter createDataStructureConverter(
          DataType producedDataType) {
        throw new UnsupportedOperationException("not used in tests");
      }
    };
  }

  private static LanceOptions options() {
    return LanceOptions.builder().path("/tmp/lance").build();
  }

  private static DataType dataType() {
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT()), DataTypes.FIELD("name", DataTypes.STRING()));
  }

  private static LanceLookupConfig lookupDefaults() {
    return LanceLookupConfig.fromConfig(new Configuration());
  }

  private static LanceDynamicTableSource batchSource(
      LanceScanOptions scanOptions, DataType physicalDataType) {
    return LanceDynamicTableSource.forBatch(
        options(), scanOptions, lookupDefaults(), physicalDataType);
  }

  private static LanceDynamicTableSource continuousSource(
      LanceContinuousOptions continuousOptions, DataType physicalDataType) {
    return LanceDynamicTableSource.forContinuous(
        options(), continuousOptions, lookupDefaults(), physicalDataType);
  }
}
