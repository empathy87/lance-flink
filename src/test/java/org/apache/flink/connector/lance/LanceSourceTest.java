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
package org.apache.flink.connector.lance;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.source.LanceSource;
import org.apache.flink.connector.lance.source.LanceSourceSplit;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** LanceSource unit tests. */
class LanceSourceTest {

  @TempDir Path tempDir;

  private String datasetPath;
  private RowType rowType;

  @BeforeEach
  void setUp() {
    datasetPath = tempDir.resolve("test_dataset").toString();

    // Create test RowType
    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("id", new BigIntType()));
    fields.add(new RowType.RowField("content", new VarCharType()));
    fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
    rowType = new RowType(fields);
  }

  @Test
  @DisplayName("Test LanceSource configuration build")
  void testSourceConfiguration() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).readBatchSize(512).build();

    LanceSource source =
        new LanceSource(options, rowType, Arrays.asList("id", "content"), "id > 10");

    assertThat(source.getOptions().getPath()).isEqualTo(datasetPath);
    assertThat(source.getOptions().getReadBatchSize()).isEqualTo(512);
    assertThat(source.getSelectedColumns()).containsExactly("id", "content");
    assertThat(source.getFilter()).isEqualTo("id > 10");
    assertThat(source.getRowType()).isEqualTo(rowType);
  }

  @Test
  @DisplayName("Test LanceSource construction via LanceOptions")
  void testSourceFromOptions() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).readBatchSize(256).build();

    LanceSource source = new LanceSource(options, rowType, Arrays.asList("id"), "id < 100");

    assertThat(source.getOptions().getPath()).isEqualTo(datasetPath);
    assertThat(source.getOptions().getReadBatchSize()).isEqualTo(256);
    assertThat(source.getSelectedColumns()).containsExactly("id");
    assertThat(source.getFilter()).isEqualTo("id < 100");
  }

  @Test
  @DisplayName("Test LanceSourceSplit creation")
  void testLanceSourceSplit() {
    LanceSourceSplit fragmentSplit = LanceSourceSplit.fragment(1, 7);
    assertThat(fragmentSplit.splitId()).isEqualTo("v1-frag-7");
    assertThat(fragmentSplit.datasetVersion()).isEqualTo(1L);
    assertThat(fragmentSplit.fragmentId()).isEqualTo(7);
    assertThat(fragmentSplit.recordsToSkip()).isZero();
  }

  @Test
  @DisplayName("Test LanceSourceSplit equality and resume")
  void testLanceSourceSplitEquality() {
    LanceSourceSplit a = LanceSourceSplit.fragment(1, 1);
    LanceSourceSplit b = LanceSourceSplit.fragment(1, 1);
    LanceSourceSplit c = LanceSourceSplit.fragment(1, 2);

    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
    assertThat(a).isNotEqualTo(c);
    assertThat(a.withRecordsToSkip(10)).isNotEqualTo(a);
    assertThat(a.withRecordsToSkip(10).recordsToSkip()).isEqualTo(10);
  }

  @Test
  @DisplayName("Test default configuration values")
  void testDefaultConfiguration() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    assertThat(options.getReadBatchSize()).isEqualTo(1024);

    LanceSource source = new LanceSource(options, rowType);
    assertThat(source.getSelectedColumns()).isNull();
    assertThat(source.getFilter()).isNull();
  }

  @Test
  @DisplayName("Test configuration validation - invalid batch size")
  void testInvalidBatchSize() {
    assertThatThrownBy(() -> LanceOptions.builder().path(datasetPath).readBatchSize(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("batch-size");
  }

  @Test
  @DisplayName("Test vector type RowType")
  void testVectorRowType() {
    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("id", new BigIntType()));
    fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
    RowType vectorRowType = new RowType(fields);

    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    LanceSource source = new LanceSource(options, vectorRowType);

    assertThat(source.getRowType().getFieldCount()).isEqualTo(2);
    assertThat(source.getRowType().getTypeAt(1)).isInstanceOf(ArrayType.class);
  }
}
