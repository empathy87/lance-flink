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
import org.apache.flink.connector.lance.source.continuous.LanceContinuousOptions;
import org.apache.flink.connector.lance.source.scan.LanceScanOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LanceDynamicTableSourceTest {

  @Test
  void defaultConstructorIsBatch() {
    LanceDynamicTableSource s = LanceDynamicTableSource.forBatch(options(), dataType());
    assertThat(s.isContinuous()).isFalse();
    assertThat(s.getContinuousOptions()).isNull();
    assertThat(s.getScanOptions()).isEqualTo(LanceScanOptions.latest());
  }

  @Test
  void changelogModeIsInsertOnlyInBatchMode() {
    LanceDynamicTableSource s = LanceDynamicTableSource.forBatch(options(), dataType());
    assertThat(s.getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());
  }

  @Test
  void changelogModeIsInsertOnlyInContinuousMode() {
    LanceContinuousOptions c = LanceContinuousOptions.fromConfig(new Configuration());
    LanceDynamicTableSource s = LanceDynamicTableSource.forContinuous(options(), c, dataType());
    assertThat(s.getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());
    assertThat(s.isContinuous()).isTrue();
    assertThat(s.getContinuousOptions()).isSameAs(c);
  }

  @Test
  void copyPreservesContinuousOptions() {
    LanceContinuousOptions c = LanceContinuousOptions.fromConfig(new Configuration());
    LanceDynamicTableSource s = LanceDynamicTableSource.forContinuous(options(), c, dataType());
    DynamicTableSource copy = s.copy();
    assertThat(copy).isInstanceOf(LanceDynamicTableSource.class);
    assertThat(((LanceDynamicTableSource) copy).getContinuousOptions()).isSameAs(c);
    assertThat(((LanceDynamicTableSource) copy).isContinuous()).isTrue();
  }

  @Test
  void copyPreservesBatchScanOptions() {
    LanceScanOptions scan = LanceScanOptions.version(7);
    LanceDynamicTableSource s = LanceDynamicTableSource.forBatch(options(), scan, dataType());
    DynamicTableSource copy = s.copy();
    assertThat(((LanceDynamicTableSource) copy).getScanOptions()).isEqualTo(scan);
    assertThat(((LanceDynamicTableSource) copy).isContinuous()).isFalse();
  }

  private static LanceOptions options() {
    return LanceOptions.builder().path("/tmp/lance").build();
  }

  private static DataType dataType() {
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT()), DataTypes.FIELD("name", DataTypes.STRING()));
  }
}
