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
import org.apache.flink.connector.lance.source.scan.LanceScanOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Lance SQL integration tests. */
class LanceSqlITCase {

  @TempDir Path tempDir;

  private String datasetPath;

  @BeforeEach
  void setUp() {
    datasetPath = tempDir.resolve("test_sql_dataset").toString();
  }

  @Test
  void testFactoryIdentifier() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    assertThat(factory.factoryIdentifier()).isEqualTo("lance");
  }

  @Test
  void testRequiredOptions() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    Set<String> requiredOptionKeys = new HashSet<>();
    factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

    assertThat(requiredOptionKeys).contains("path");
  }

  @Test
  void testOptionalOptions() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    Set<String> optionalOptionKeys = new HashSet<>();
    factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

    assertThat(optionalOptionKeys)
        .containsExactlyInAnyOrder(
            "read.batch-size",
            "write.batch-size",
            "write.max-rows-per-file",
            "metadata-type",
            "scan.version",
            "scan.snapshot-id",
            "scan.tag-name",
            "scan.timestamp-millis",
            "scan.timestamp",
            "scan.mode",
            "continuous.discovery-interval",
            "scan.startup-mode",
            "scan.startup-snapshot-id",
            "scan.startup-tag-name",
            "scan.startup-timestamp-millis",
            "scan.startup-timestamp",
            "lookup.allow-full-scan",
            "lookup.cache",
            "lookup.partial-cache.max-rows",
            "lookup.partial-cache.expire-after-write",
            "lookup.partial-cache.expire-after-access",
            "lookup.partial-cache.cache-missing-key");
    // lookup.max-retries is intentionally not exposed because lookup retry is not implemented.
    assertThat(optionalOptionKeys).doesNotContain("lookup.max-retries");
    assertThat(optionalOptionKeys)
        .doesNotContain(
            "read.columns",
            "read.filter",
            "index.type",
            "index.column",
            "index.num-partitions",
            "index.num-sub-vectors",
            "vector.column",
            "vector.metric",
            "vector.nprobes");
  }

  @Test
  void testDynamicTableSourceCreation() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).readBatchSize(512).build();

    DataType dataType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("content", DataTypes.STRING()),
            DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

    LanceDynamicTableSource source =
        LanceDynamicTableSource.forBatch(
            options, LanceScanOptions.latest(), lookupDefaults(), dataType);

    assertThat(source.getOptions()).isEqualTo(options);
    assertThat(source.getPhysicalDataType()).isEqualTo(dataType);
    assertThat(source.asSummaryString()).isEqualTo("Lance Table Source");
  }

  @Test
  void testDynamicTableSinkCreation() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).writeBatchSize(256).build();

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
  void testDynamicTableSourceCopy() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();
    DataType dataType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()));

    LanceDynamicTableSource source =
        LanceDynamicTableSource.forBatch(
            options, LanceScanOptions.latest(), lookupDefaults(), dataType);
    LanceDynamicTableSource copiedSource = (LanceDynamicTableSource) source.copy();

    assertThat(copiedSource).isNotSameAs(source);
    assertThat(copiedSource.getOptions()).isEqualTo(source.getOptions());
  }

  @Test
  void testDynamicTableSinkCopy() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();
    DataType dataType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()));

    LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);
    LanceDynamicTableSink copiedSink = (LanceDynamicTableSink) sink.copy();

    assertThat(copiedSink).isNotSameAs(sink);
    assertThat(copiedSink.getOptions()).isEqualTo(sink.getOptions());
  }

  @Test
  void testCatalogFactoryIdentifier() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    assertThat(factory.factoryIdentifier()).isEqualTo("lance");
  }

  @Test
  void testCatalogRequiredOptions() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    Set<String> requiredOptionKeys = new HashSet<>();
    factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

    assertThat(requiredOptionKeys).contains("warehouse");
  }

  @Test
  void testCatalogOptionalOptions() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    Set<String> optionalOptionKeys = new HashSet<>();
    factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

    assertThat(optionalOptionKeys).contains("default-database");
  }

  @Test
  void testConfigOptions() {
    assertThat(LanceDynamicTableFactory.PATH.key()).isEqualTo("path");
    assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.key()).isEqualTo("read.batch-size");
    assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.defaultValue()).isEqualTo(1024);
    assertThat(LanceDynamicTableFactory.WRITE_BATCH_SIZE.key()).isEqualTo("write.batch-size");
    assertThat(LanceDynamicTableFactory.WRITE_BATCH_SIZE.defaultValue()).isEqualTo(1024);
    assertThat(LanceDynamicTableFactory.WRITE_MAX_ROWS_PER_FILE.key())
        .isEqualTo("write.max-rows-per-file");
  }

  @Test
  void testCatalogConfigOptions() {
    assertThat(LanceCatalogFactory.WAREHOUSE.key()).isEqualTo("warehouse");
    assertThat(LanceCatalogFactory.DEFAULT_DATABASE.key()).isEqualTo("default-database");
    assertThat(LanceCatalogFactory.DEFAULT_DATABASE.defaultValue()).isEqualTo("default");
  }

  @Test
  void testVectorSearchFunctionConfiguration() {
    LanceVectorSearchFunction function = new LanceVectorSearchFunction();
    assertThat(function).isNotNull();
  }

  private static LanceLookupConfig lookupDefaults() {
    return LanceLookupConfig.fromConfig(new Configuration());
  }
}
