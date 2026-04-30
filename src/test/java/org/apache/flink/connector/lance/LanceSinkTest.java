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
import org.apache.flink.connector.lance.sink.LanceSinkV2;
import org.apache.flink.connector.lance.sink.LanceUpsertSinkV2;

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
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Sink V2 unit tests covering construction and option validation. */
class LanceSinkTest {

  @TempDir Path tempDir;

  private String datasetPath;
  private RowType rowType;

  @BeforeEach
  void setUp() {
    datasetPath = tempDir.resolve("test_sink_dataset").toString();

    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("id", new BigIntType()));
    fields.add(new RowType.RowField("content", new VarCharType()));
    fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
    rowType = new RowType(fields);
  }

  @Test
  @DisplayName("Test LanceSinkV2 holds options and row type")
  void testSinkV2Construction() {
    LanceOptions options =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(512)
            .writeMaxRowsPerFile(500000)
            .build();

    LanceSinkV2 sink = new LanceSinkV2(options, rowType);

    assertThat(sink.getOptions().getPath()).isEqualTo(datasetPath);
    assertThat(sink.getOptions().getWriteBatchSize()).isEqualTo(512);
    assertThat(sink.getOptions().getWriteMaxRowsPerFile()).isEqualTo(500000);
    assertThat(sink.getRowType()).isEqualTo(rowType);
  }

  @Test
  @DisplayName("Test LanceUpsertSinkV2 holds options, row type and primary keys")
  void testUpsertSinkV2Construction() {
    LanceOptions options =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(256)
            .writeMaxRowsPerFile(100000)
            .build();

    LanceUpsertSinkV2 sink = new LanceUpsertSinkV2(options, rowType, List.of("id"));

    assertThat(sink.getOptions().getPath()).isEqualTo(datasetPath);
    assertThat(sink.getOptions().getWriteBatchSize()).isEqualTo(256);
    assertThat(sink.getOptions().getWriteMaxRowsPerFile()).isEqualTo(100000);
    assertThat(sink.getRowType()).isEqualTo(rowType);
    assertThat(sink.getPrimaryKeys()).containsExactly("id");
  }

  @Test
  @DisplayName("Test LanceUpsertSinkV2 rejects empty primary keys")
  void testUpsertSinkV2RejectsEmptyPrimaryKeys() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    assertThatThrownBy(() -> new LanceUpsertSinkV2(options, rowType, Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("primary key");

    assertThatThrownBy(() -> new LanceUpsertSinkV2(options, rowType, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("primary key");
  }

  @Test
  @DisplayName("Test default Sink configuration values")
  void testDefaultSinkConfiguration() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    assertThat(options.getWriteBatchSize()).isEqualTo(1024);
    assertThat(options.getWriteMaxRowsPerFile()).isEqualTo(1000000);
  }

  @Test
  @DisplayName("Test configuration validation - invalid write batch size")
  void testInvalidWriteBatchSize() {
    assertThatThrownBy(() -> LanceOptions.builder().path(datasetPath).writeBatchSize(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("batch-size");
  }

  @Test
  @DisplayName("Test configuration validation - invalid max rows per file")
  void testInvalidMaxRowsPerFile() {
    assertThatThrownBy(
            () -> LanceOptions.builder().path(datasetPath).writeMaxRowsPerFile(-1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("max-rows-per-file");
  }

  @Test
  @DisplayName("Test vector type write configuration")
  void testVectorWriteConfiguration() {
    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("id", new BigIntType()));
    fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
    RowType vectorRowType = new RowType(fields);

    LanceOptions options = LanceOptions.builder().path(datasetPath).writeBatchSize(100).build();

    LanceSinkV2 sink = new LanceSinkV2(options, vectorRowType);

    assertThat(sink.getRowType().getFieldCount()).isEqualTo(2);
    assertThat(sink.getRowType().getTypeAt(1)).isInstanceOf(ArrayType.class);
  }
}
