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
import org.apache.flink.connector.lance.source.LanceScanMode;
import org.apache.flink.connector.lance.source.LanceSourceOptions;
import org.apache.flink.connector.lance.source.continuous.LanceContinuousOptions;
import org.apache.flink.connector.lance.source.scan.LanceScanOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Dynamic table source/sink factory for Lance. */
// TODO: Add index/vector options back when index creation or vector search is supported.
public class LanceDynamicTableFactory
    implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  public static final String IDENTIFIER = "lance";

  public static final ConfigOption<String> PATH =
      ConfigOptions.key("path")
          .stringType()
          .noDefaultValue()
          .withDescription("Lance dataset path.");

  public static final ConfigOption<Integer> READ_BATCH_SIZE =
      ConfigOptions.key("read.batch-size")
          .intType()
          .defaultValue(1024)
          .withDescription("Read batch size.");

  public static final ConfigOption<Integer> WRITE_BATCH_SIZE =
      ConfigOptions.key("write.batch-size")
          .intType()
          .defaultValue(1024)
          .withDescription("Write batch size.");

  public static final ConfigOption<Integer> WRITE_MAX_ROWS_PER_FILE =
      ConfigOptions.key("write.max-rows-per-file")
          .intType()
          .defaultValue(1000000)
          .withDescription("Maximum rows per file.");

  public static final ConfigOption<String> METADATA_TYPE =
      ConfigOptions.key("metadata-type")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Selects a virtual metadata view (snapshots | tags | branches | fragments |"
                  + " options) instead of the base table data. Set by the catalog when resolving"
                  + " a <table>$<suffix> name.");

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
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(READ_BATCH_SIZE);
    options.add(WRITE_BATCH_SIZE);
    options.add(WRITE_MAX_ROWS_PER_FILE);
    options.add(METADATA_TYPE);
    options.add(LanceSourceOptions.SCAN_MODE);
    options.addAll(LanceScanOptions.ALL_OPTIONS);
    options.addAll(LanceContinuousOptions.ALL_OPTIONS);
    return Set.copyOf(options);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    ReadableConfig config = helper.getOptions();
    String metadataType = config.getOptional(METADATA_TYPE).orElse(null);
    if (metadataType != null) {
      return createMetadataTableSource(config, context, metadataType);
    }

    LanceScanMode scanMode = parseScanMode(config);
    LanceOptions options = buildLanceOptions(config);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    DataType physicalDataType = schema.toPhysicalRowDataType();

    if (scanMode == LanceScanMode.BATCH) {
      rejectIfPresent(
          config,
          LanceContinuousOptions.ALL_OPTIONS,
          "Lance continuous-source option",
          "is only supported when scan.mode = continuous (current: scan.mode = batch (the"
              + " default)).");
      LanceScanOptions scanOptions = buildScanOptions(config);
      return LanceDynamicTableSource.forBatch(options, scanOptions, physicalDataType);
    }

    rejectIfPresent(
        config,
        LanceScanOptions.ALL_OPTIONS,
        "Lance batch scan option",
        "is not supported when scan.mode = continuous. Use scan.startup-mode plus the matching"
            + " scan.startup-* option instead.");
    LanceContinuousOptions continuousOptions = buildContinuousOptions(config);
    if (schema.getPrimaryKey().isPresent()) {
      // TODO: Re-enable when Lance Java exposes transaction/delta row streams.
      throw new ValidationException(
          "Continuous reads from PK / upsert Lance tables are not supported. Fragment-diff"
              + " discovery cannot represent updates or deletes safely. Use scan.mode=batch for PK"
              + " reads.");
    }
    return LanceDynamicTableSource.forContinuous(options, continuousOptions, physicalDataType);
  }

  private static DynamicTableSource createMetadataTableSource(
      ReadableConfig config, Context context, String metadataType) {
    MetadataTableType type =
        MetadataTableType.fromSuffix(metadataType)
            .orElseThrow(
                () ->
                    new ValidationException(
                        "Unknown Lance metadata-type '"
                            + metadataType
                            + "'. Supported: snapshots, tags, branches, fragments, options."));
    if (parseScanMode(config) == LanceScanMode.CONTINUOUS) {
      throw new ValidationException(
          "scan.mode = continuous is not supported on metadata tables (metadata-type is set);"
              + " metadata reads always run at HEAD.");
    }
    rejectIfPresent(
        config,
        LanceScanOptions.ALL_OPTIONS,
        "Lance scan option",
        "is not supported on metadata tables (metadata-type is set).");
    rejectIfPresent(
        config,
        LanceContinuousOptions.ALL_OPTIONS,
        "Lance continuous-source option",
        "is only supported when scan.mode = continuous (current: metadata tables (metadata-type is"
            + " set)).");

    Map<String, String> sourceTableOptions = new HashMap<>(context.getCatalogTable().getOptions());
    sourceTableOptions.remove(METADATA_TYPE.key());
    return new LanceMetadataTableSource(config.get(PATH), type, sourceTableOptions);
  }

  private static LanceScanMode parseScanMode(ReadableConfig config) {
    try {
      return LanceScanMode.fromString(config.get(LanceSourceOptions.SCAN_MODE));
    } catch (IllegalArgumentException e) {
      throw new ValidationException(e.getMessage(), e);
    }
  }

  private static void rejectIfPresent(
      ReadableConfig config,
      Iterable<? extends ConfigOption<?>> options,
      String prefix,
      String reason) {
    for (ConfigOption<?> option : options) {
      if (config.getOptional(option).isPresent()) {
        throw new ValidationException(prefix + " '" + option.key() + "' " + reason);
      }
    }
  }

  private static LanceScanOptions buildScanOptions(ReadableConfig config) {
    try {
      return LanceScanOptions.fromConfig(config);
    } catch (IllegalArgumentException e) {
      throw new ValidationException("Invalid Lance scan options: " + e.getMessage(), e);
    }
  }

  private static LanceContinuousOptions buildContinuousOptions(ReadableConfig config) {
    try {
      return LanceContinuousOptions.fromConfig(config);
    } catch (IllegalArgumentException e) {
      throw new ValidationException("Invalid Lance continuous options: " + e.getMessage(), e);
    }
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    ReadableConfig config = helper.getOptions();
    if (config.getOptional(METADATA_TYPE).isPresent()) {
      throw new ValidationException("Lance metadata tables are read-only.");
    }
    rejectIfPresent(
        config,
        List.of(LanceSourceOptions.SCAN_MODE),
        "Lance source option",
        "is only supported for reads.");
    rejectIfPresent(
        config, LanceScanOptions.ALL_OPTIONS, "Lance scan option", "is only supported for reads.");
    rejectIfPresent(
        config,
        LanceContinuousOptions.ALL_OPTIONS,
        "Lance continuous-source option",
        "is only supported for reads.");
    LanceOptions options = buildLanceOptions(config);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

    return new LanceDynamicTableSink(options, schema.toPhysicalRowDataType(), primaryKeys(schema));
  }

  private static List<String> primaryKeys(ResolvedSchema schema) {
    return schema.getPrimaryKey().map(pk -> List.copyOf(pk.getColumns())).orElse(List.of());
  }

  private static LanceOptions buildLanceOptions(ReadableConfig config) {
    try {
      return LanceOptions.builder()
          .path(config.get(PATH))
          .readBatchSize(config.get(READ_BATCH_SIZE))
          .writeBatchSize(config.get(WRITE_BATCH_SIZE))
          .writeMaxRowsPerFile(config.get(WRITE_MAX_ROWS_PER_FILE))
          .build();
    } catch (IllegalArgumentException e) {
      throw new ValidationException("Invalid Lance table options: " + e.getMessage(), e);
    }
  }
}
