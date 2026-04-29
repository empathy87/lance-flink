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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Lance SQL integration tests. */
class LanceSqlITCase {

  @TempDir Path tempDir;

  private String datasetPath;
  private String warehousePath;

  @BeforeEach
  void setUp() {
    datasetPath = tempDir.resolve("test_sql_dataset").toString();
    warehousePath = tempDir.resolve("test_warehouse").toString();
  }

  @Test
  @DisplayName("Test LanceDynamicTableFactory identifier")
  void testFactoryIdentifier() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    assertThat(factory.factoryIdentifier()).isEqualTo("lance");
  }

  @Test
  @DisplayName("Test LanceDynamicTableFactory required options")
  void testRequiredOptions() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    Set<String> requiredOptionKeys = new HashSet<>();
    factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

    assertThat(requiredOptionKeys).contains("path");
  }

  @Test
  @DisplayName("Test LanceDynamicTableFactory optional options")
  void testOptionalOptions() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    Set<String> optionalOptionKeys = new HashSet<>();
    factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

    assertThat(optionalOptionKeys)
        .contains(
            "read.batch-size",
            "read.columns",
            "read.filter",
            "write.batch-size",
            "write.mode",
            "write.max-rows-per-file",
            "index.type",
            "index.column",
            "vector.column",
            "vector.metric");
  }

  @Test
  @DisplayName("Test LanceDynamicTableSource creation")
  void testDynamicTableSourceCreation() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).readBatchSize(512).build();

    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("id", new BigIntType()));
    fields.add(new RowType.RowField("content", new VarCharType()));
    fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
    RowType rowType = new RowType(fields);

    DataType dataType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("content", DataTypes.STRING()),
            DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

    LanceDynamicTableSource source = new LanceDynamicTableSource(options, dataType);

    assertThat(source.getOptions()).isEqualTo(options);
    assertThat(source.getPhysicalDataType()).isEqualTo(dataType);
    assertThat(source.asSummaryString()).isEqualTo("Lance Table Source");
  }

  @Test
  @DisplayName("Test LanceDynamicTableSink creation")
  void testDynamicTableSinkCreation() {
    LanceOptions options =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(256)
            .writeMode(LanceOptions.WriteMode.APPEND)
            .build();

    DataType dataType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("content", DataTypes.STRING()),
            DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

    LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);

    assertThat(sink.getOptions()).isEqualTo(options);
    assertThat(sink.getPhysicalDataType()).isEqualTo(dataType);
    assertThat(sink.asSummaryString()).isEqualTo("Lance Table Sink");
  }

  @Test
  @DisplayName("Test LanceDynamicTableSource copy")
  void testDynamicTableSourceCopy() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    DataType dataType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()));

    LanceDynamicTableSource source = new LanceDynamicTableSource(options, dataType);
    LanceDynamicTableSource copiedSource = (LanceDynamicTableSource) source.copy();

    assertThat(copiedSource).isNotSameAs(source);
    assertThat(copiedSource.getOptions()).isEqualTo(source.getOptions());
  }

  @Test
  @DisplayName("Test LanceDynamicTableSink copy")
  void testDynamicTableSinkCopy() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    DataType dataType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()));

    LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);
    LanceDynamicTableSink copiedSink = (LanceDynamicTableSink) sink.copy();

    assertThat(copiedSink).isNotSameAs(sink);
    assertThat(copiedSink.getOptions()).isEqualTo(sink.getOptions());
  }

  @Test
  @DisplayName("Test LanceCatalogFactory identifier")
  void testCatalogFactoryIdentifier() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    assertThat(factory.factoryIdentifier()).isEqualTo("lance");
  }

  @Test
  @DisplayName("Test LanceCatalogFactory required options")
  void testCatalogRequiredOptions() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    Set<String> requiredOptionKeys = new HashSet<>();
    factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

    assertThat(requiredOptionKeys).contains("warehouse");
  }

  @Test
  @DisplayName("Test LanceCatalogFactory optional options")
  void testCatalogOptionalOptions() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    Set<String> optionalOptionKeys = new HashSet<>();
    factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

    assertThat(optionalOptionKeys).contains("default-database");
  }

  @Test
  @DisplayName("Test configuration options definition")
  void testConfigOptions() {
    assertThat(LanceDynamicTableFactory.PATH.key()).isEqualTo("path");
    assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.key()).isEqualTo("read.batch-size");
    assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.defaultValue()).isEqualTo(1024);
    assertThat(LanceDynamicTableFactory.WRITE_BATCH_SIZE.key()).isEqualTo("write.batch-size");
    assertThat(LanceDynamicTableFactory.WRITE_MODE.key()).isEqualTo("write.mode");
    assertThat(LanceDynamicTableFactory.WRITE_MODE.defaultValue()).isEqualTo("append");
    assertThat(LanceDynamicTableFactory.INDEX_TYPE.key()).isEqualTo("index.type");
    assertThat(LanceDynamicTableFactory.INDEX_TYPE.defaultValue()).isEqualTo("IVF_PQ");
    assertThat(LanceDynamicTableFactory.VECTOR_METRIC.key()).isEqualTo("vector.metric");
    assertThat(LanceDynamicTableFactory.VECTOR_METRIC.defaultValue()).isEqualTo("L2");
  }

  @Test
  @DisplayName("Test Catalog configuration options definition")
  void testCatalogConfigOptions() {
    assertThat(LanceCatalogFactory.WAREHOUSE.key()).isEqualTo("warehouse");
    assertThat(LanceCatalogFactory.DEFAULT_DATABASE.key()).isEqualTo("default-database");
    assertThat(LanceCatalogFactory.DEFAULT_DATABASE.defaultValue()).isEqualTo("default");
  }

  @Test
  @DisplayName("Test vector search UDF configuration")
  void testVectorSearchFunctionConfiguration() {
    LanceVectorSearchFunction function = new LanceVectorSearchFunction();
    assertThat(function).isNotNull();
  }
}
