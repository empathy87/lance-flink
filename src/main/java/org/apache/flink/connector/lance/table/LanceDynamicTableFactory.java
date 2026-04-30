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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.*;

/** Dynamic table source/sink factory for Lance. */
public class LanceDynamicTableFactory
    implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  public static final String IDENTIFIER = "lance";

  // ==================== Configuration Options Definition ====================

  public static final ConfigOption<String> PATH =
      ConfigOptions.key("path").stringType().noDefaultValue().withDescription("Lance dataset path");

  public static final ConfigOption<Integer> READ_BATCH_SIZE =
      ConfigOptions.key("read.batch-size")
          .intType()
          .defaultValue(1024)
          .withDescription("Read batch size");

  public static final ConfigOption<String> READ_COLUMNS =
      ConfigOptions.key("read.columns")
          .stringType()
          .noDefaultValue()
          .withDescription("Columns to read, comma separated");

  public static final ConfigOption<String> READ_FILTER =
      ConfigOptions.key("read.filter")
          .stringType()
          .noDefaultValue()
          .withDescription("Data filter condition");

  public static final ConfigOption<Integer> WRITE_BATCH_SIZE =
      ConfigOptions.key("write.batch-size")
          .intType()
          .defaultValue(1024)
          .withDescription("Write batch size");

  public static final ConfigOption<Integer> WRITE_MAX_ROWS_PER_FILE =
      ConfigOptions.key("write.max-rows-per-file")
          .intType()
          .defaultValue(1000000)
          .withDescription("Maximum rows per file");

  public static final ConfigOption<String> INDEX_TYPE =
      ConfigOptions.key("index.type")
          .stringType()
          .defaultValue("IVF_PQ")
          .withDescription("Vector index type");

  public static final ConfigOption<String> INDEX_COLUMN =
      ConfigOptions.key("index.column")
          .stringType()
          .noDefaultValue()
          .withDescription("Index column name");

  public static final ConfigOption<Integer> INDEX_NUM_PARTITIONS =
      ConfigOptions.key("index.num-partitions")
          .intType()
          .defaultValue(256)
          .withDescription("IVF partition count");

  public static final ConfigOption<Integer> INDEX_NUM_SUB_VECTORS =
      ConfigOptions.key("index.num-sub-vectors")
          .intType()
          .noDefaultValue()
          .withDescription("PQ sub-vector count");

  public static final ConfigOption<String> VECTOR_COLUMN =
      ConfigOptions.key("vector.column")
          .stringType()
          .noDefaultValue()
          .withDescription("Vector column name");

  public static final ConfigOption<String> VECTOR_METRIC =
      ConfigOptions.key("vector.metric")
          .stringType()
          .defaultValue("L2")
          .withDescription("Distance metric type: L2, Cosine, Dot");

  public static final ConfigOption<Integer> VECTOR_NPROBES =
      ConfigOptions.key("vector.nprobes")
          .intType()
          .defaultValue(20)
          .withDescription("IVF search probe count");

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(PATH);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(
        READ_BATCH_SIZE,
        READ_COLUMNS,
        READ_FILTER,
        WRITE_BATCH_SIZE,
        WRITE_MAX_ROWS_PER_FILE,
        INDEX_TYPE,
        INDEX_COLUMN,
        INDEX_NUM_PARTITIONS,
        INDEX_NUM_SUB_VECTORS,
        VECTOR_COLUMN,
        VECTOR_METRIC,
        VECTOR_NPROBES);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    ReadableConfig config = helper.getOptions();
    LanceOptions options = buildLanceOptions(config);

    return new LanceDynamicTableSource(
        options, context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType());
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    ReadableConfig config = helper.getOptions();
    LanceOptions options = buildLanceOptions(config);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

    return new LanceDynamicTableSink(options, schema.toPhysicalRowDataType(), primaryKeys(schema));
  }

  private static List<String> primaryKeys(ResolvedSchema schema) {
    return schema.getPrimaryKey().map(pk -> List.copyOf(pk.getColumns())).orElse(List.of());
  }

  private LanceOptions buildLanceOptions(ReadableConfig config) {
    LanceOptions.Builder builder = LanceOptions.builder();

    // Common configuration
    builder.path(config.get(PATH));

    // Source configuration
    builder.readBatchSize(config.get(READ_BATCH_SIZE));
    config
        .getOptional(READ_COLUMNS)
        .ifPresent(
            columns -> {
              if (!columns.isEmpty()) {
                List<String> readColumns =
                    Arrays.stream(columns.split(","))
                        .map(String::trim)
                        .filter(column -> !column.isEmpty())
                        .toList();

                if (!readColumns.isEmpty()) {
                  builder.readColumns(readColumns);
                }
              }
            });
    config.getOptional(READ_FILTER).ifPresent(builder::readFilter);

    // Sink configuration
    builder.writeBatchSize(config.get(WRITE_BATCH_SIZE));
    builder.writeMaxRowsPerFile(config.get(WRITE_MAX_ROWS_PER_FILE));

    // Index configuration
    builder.indexType(LanceOptions.IndexType.fromValue(config.get(INDEX_TYPE)));
    config.getOptional(INDEX_COLUMN).ifPresent(builder::indexColumn);
    builder.indexNumPartitions(config.get(INDEX_NUM_PARTITIONS));
    config.getOptional(INDEX_NUM_SUB_VECTORS).ifPresent(builder::indexNumSubVectors);

    // Vector search configuration
    config.getOptional(VECTOR_COLUMN).ifPresent(builder::vectorColumn);
    builder.vectorMetric(LanceOptions.MetricType.fromValue(config.get(VECTOR_METRIC)));
    builder.vectorNprobes(config.get(VECTOR_NPROBES));

    return builder.build();
  }
}
