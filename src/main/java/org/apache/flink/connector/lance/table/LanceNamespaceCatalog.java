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

import org.apache.flink.connector.lance.LanceDatasetOpener;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.source.scan.LanceScanOptions;
import org.apache.flink.connector.lance.source.scan.LanceScanVersionResolver;

import org.lance.Dataset;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.RenameTableRequest;
import org.lance.namespace.model.TableExistsRequest;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.lance.converter.LanceTypeConverter.toArrowSchema;

/** Catalog backed by a Lance namespace. */
public class LanceNamespaceCatalog extends AbstractCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(LanceNamespaceCatalog.class);

  public static final String DEFAULT_DATABASE = "default";
  private static final String CONNECTOR_OPTION = "connector";
  private static final String PATH_OPTION = "path";
  static final String PRIMARY_KEY_OPTION = "primary-key";
  static final String PRIMARY_KEY_METADATA = "flink.primary-keys";
  private static final String PRIMARY_KEY_DELIMITER = ",";

  private final LanceNamespace namespace;
  private final BufferAllocator allocator;
  private final Map<String, String> options;
  private boolean opened;
  private boolean closed;

  public LanceNamespaceCatalog(
      String name, String defaultDatabase, LanceNamespace namespace, Map<String, String> options) {
    super(name, defaultDatabase);
    this.namespace = Objects.requireNonNull(namespace, "namespace");
    this.options = new HashMap<>(Objects.requireNonNull(options, "options"));
    this.allocator = new RootAllocator();
    LOG.info("Creating LanceNamespaceCatalog: namespaceClass={}", namespace.getClass().getName());
  }

  @Override
  public void open() throws CatalogException {
    if (closed) {
      throw new CatalogException("Cannot open a closed Lance namespace catalog");
    }
    if (opened) {
      return;
    }

    try {
      namespace.initialize(options, allocator);
      createDatabaseIfMissing(getDefaultDatabase());
      opened = true;
    } catch (Exception e) {
      closeNamespaceAndAllocator(e);
      closed = true;
      throw new CatalogException("Failed to open Lance namespace catalog", e);
    }
  }

  private void closeNamespaceAndAllocator(Exception originalError) {
    if (namespace instanceof Closeable closeable) {
      try {
        closeable.close();
      } catch (Exception closeError) {
        originalError.addSuppressed(closeError);
      }
    }

    try {
      allocator.close();
    } catch (Exception closeError) {
      originalError.addSuppressed(closeError);
    }
  }

  @Override
  public void close() throws CatalogException {
    if (closed) {
      return;
    }
    closed = true;
    opened = false;

    CatalogException error = null;

    if (namespace instanceof Closeable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        error = new CatalogException("Failed to close Lance namespace", e);
      }
    }

    try {
      allocator.close();
    } catch (Exception e) {
      if (error != null) {
        error.addSuppressed(e);
      } else {
        error = new CatalogException("Failed to close Arrow allocator", e);
      }
    }

    if (error != null) {
      throw error;
    }
  }

  // Database Operations

  @Override
  public List<String> listDatabases() throws CatalogException {
    List<String> databases = new ArrayList<>();
    ListNamespacesRequest request = new ListNamespacesRequest();
    String pageToken = null;
    try {
      do {
        if (pageToken != null) {
          request.setPageToken(pageToken);
        }
        ListNamespacesResponse response = namespace.listNamespaces(request);
        databases.addAll(response.getNamespaces());
        pageToken = response.getPageToken();
      } while (pageToken != null && !pageToken.isEmpty());
    } catch (RuntimeException e) {
      throw new CatalogException("Failed to list databases", e);
    }
    return databases;
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
    if (databaseExists(databaseName)) {
      return new CatalogDatabaseImpl(Collections.emptyMap(), null);
    }
    throw new DatabaseNotExistException(getName(), databaseName);
  }

  @Override
  public boolean databaseExists(String databaseName) {
    try {
      namespace.namespaceExists(new NamespaceExistsRequest().id(singletonList(databaseName)));
      return true;
    } catch (RuntimeException e) {
      if (isNamespaceNotFound(e)) {
        return false;
      }
      throw new CatalogException("Failed to check if database exists: " + databaseName, e);
    }
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    try {
      namespace.createNamespace(new CreateNamespaceRequest().id(singletonList(name)));
    } catch (RuntimeException e) {
      if (isNamespaceAlreadyExists(e)) {
        if (!ignoreIfExists) {
          throw new DatabaseAlreadyExistException(getName(), name);
        }
        return;
      }
      throw new CatalogException("Failed to create database: " + name, e);
    }
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    try {
      namespace.dropNamespace(new DropNamespaceRequest().id(singletonList(name)));
    } catch (RuntimeException e) {
      if (isNamespaceNotFound(e)) {
        if (!ignoreIfNotExists) {
          throw new DatabaseNotExistException(getName(), name);
        }
        return;
      }
      if (isNamespaceNotEmpty(e)) {
        if (!cascade) {
          throw new DatabaseNotEmptyException(getName(), name);
        }
        throw new CatalogException(
            "Lance namespace catalog does not support dropping non-empty databases with CASCADE");
      }
      throw new CatalogException("Failed to drop database: " + name, e);
    }
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(name)) {
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(getName(), name);
      }
      return;
    }

    throw new CatalogException("Lance namespace catalog does not support altering databases");
  }

  // Table Operations

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    List<String> tables = new ArrayList<>();
    ListTablesRequest request = new ListTablesRequest().id(singletonList(databaseName));
    String pageToken = null;
    try {
      do {
        if (pageToken != null) {
          request.setPageToken(pageToken);
        }
        ListTablesResponse response = namespace.listTables(request);
        tables.addAll(response.getTables());
        pageToken = response.getPageToken();
      } while (pageToken != null && !pageToken.isEmpty());
    } catch (RuntimeException e) {
      if (isNamespaceNotFound(e)) {
        throw new DatabaseNotExistException(getName(), databaseName);
      }
      throw new CatalogException("Failed to list tables in database: " + databaseName, e);
    }
    return tables;
  }

  @Override
  public List<String> listViews(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
    return Collections.emptyList();
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    ObjectPath baseTablePath = resolveBaseTable(tablePath);
    try {
      namespace.tableExists(new TableExistsRequest().id(tableId(baseTablePath)));
      return true;
    } catch (RuntimeException e) {
      if (isNamespaceNotFound(e)) {
        return false;
      }
      throw new CatalogException("Failed to check if table exists: " + tablePath, e);
    }
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    Optional<MetadataTableType> metadataType = parseMetadataSuffix(tablePath);
    if (metadataType.isPresent()) {
      ObjectPath baseTablePath = resolveBaseTable(tablePath);
      String datasetPath = resolveDatasetPath(baseTablePath);
      return metadataTableOf(metadataType.get(), datasetPath);
    }
    String datasetPath = resolveDatasetPath(tablePath);
    return loadTable(tablePath, datasetPath, Collections.emptyMap(), null);
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath, long timestamp)
      throws TableNotExistException, CatalogException {
    if (parseMetadataSuffix(tablePath).isPresent()) {
      throw new CatalogException(
          "Time travel is not supported on Lance metadata tables: " + tablePath);
    }
    String datasetPath = resolveDatasetPath(tablePath);
    long resolvedVersion = resolveVersionForTimestamp(tablePath, datasetPath, timestamp);

    return loadTable(
        tablePath,
        datasetPath,
        Collections.singletonMap(
            LanceScanOptions.SCAN_VERSION.key(), Long.toString(resolvedVersion)),
        resolvedVersion);
  }

  private static Optional<MetadataTableType> parseMetadataSuffix(ObjectPath tablePath) {
    return parseMetadataSuffix(tablePath.getObjectName());
  }

  private static Optional<MetadataTableType> parseMetadataSuffix(String tableName) {
    return MetadataTableType.splitName(tableName)
        .flatMap(parts -> MetadataTableType.fromSuffix(parts.suffix()));
  }

  /**
   * Returns the base table path for recognized metadata tables; leaves unknown suffixes unchanged.
   */
  private static ObjectPath resolveBaseTable(ObjectPath tablePath) {
    return MetadataTableType.splitName(tablePath.getObjectName())
        .filter(parts -> MetadataTableType.fromSuffix(parts.suffix()).isPresent())
        .map(parts -> new ObjectPath(tablePath.getDatabaseName(), parts.baseName()))
        .orElse(tablePath);
  }

  private static void rejectMetadataMutation(ObjectPath tablePath, String operation) {
    parseMetadataSuffix(tablePath)
        .ifPresent(
            type -> {
              throw new CatalogException(
                  "Cannot "
                      + operation
                      + " Lance metadata table '"
                      + tablePath
                      + "': $"
                      + type.suffix()
                      + " is a read-only virtual view.");
            });
  }

  private static CatalogBaseTable metadataTableOf(MetadataTableType type, String datasetPath) {
    Map<String, String> options = new HashMap<>();
    options.put(CONNECTOR_OPTION, LanceDynamicTableFactory.IDENTIFIER);
    options.put(PATH_OPTION, datasetPath);
    options.put(LanceDynamicTableFactory.METADATA_TYPE.key(), type.suffix());
    return CatalogTable.of(
        type.schema(),
        "Lance metadata view ($" + type.suffix() + ")",
        Collections.emptyList(),
        options);
  }

  private long resolveVersionForTimestamp(
      ObjectPath tablePath, String datasetPath, long timestamp) {
    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      return LanceScanVersionResolver.resolveVersion(
          LanceScanOptions.timestampMillis(timestamp), dataset);
    } catch (IllegalArgumentException e) {
      throw new CatalogException(
          "Cannot time-travel to timestamp "
              + timestamp
              + " on table "
              + tablePath
              + ": "
              + e.getMessage(),
          e);
    } catch (RuntimeException e) {
      throw new CatalogException(
          "Failed to open Lance dataset for time travel: " + tablePath + " at " + datasetPath, e);
    }
  }

  private CatalogBaseTable loadTable(
      ObjectPath tablePath,
      String datasetPath,
      Map<String, String> extraOptions,
      @Nullable Long schemaVersion)
      throws CatalogException {
    RowType rowType;
    List<String> primaryKeys;
    try (Dataset dataset =
        schemaVersion == null
            ? LanceDatasetOpener.open(allocator, datasetPath)
            : LanceDatasetOpener.open(allocator, datasetPath, schemaVersion)) {
      org.apache.arrow.vector.types.pojo.Schema arrowSchema = dataset.getSchema();
      rowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
      primaryKeys = readPrimaryKeysFromMetadata(arrowSchema.getCustomMetadata());
      validatePrimaryKeysExist(rowType, primaryKeys);
    } catch (Exception e) {
      String versionHint = schemaVersion == null ? "latest version" : "version " + schemaVersion;
      throw new CatalogException(
          "Failed to load Lance dataset "
              + versionHint
              + " for table: "
              + tablePath
              + " at location: "
              + datasetPath,
          e);
    }

    Schema schema = toFlinkSchema(primaryKeys, rowType);

    Map<String, String> options = buildTableOptions(datasetPath);
    options.putAll(extraOptions);
    return CatalogTable.of(schema, "", Collections.emptyList(), options);
  }

  private String resolveDatasetPath(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    DescribeTableResponse response;
    try {
      response = namespace.describeTable(new DescribeTableRequest().id(tableId(tablePath)));
    } catch (RuntimeException e) {
      if (isNamespaceNotFound(e)) {
        throw new TableNotExistException(getName(), tablePath);
      }
      throw new CatalogException("Failed to get table: " + tablePath, e);
    }

    String location = response.getLocation();
    if (location == null || location.isBlank()) {
      throw new CatalogException(
          "Lance namespace returned an empty table location for table: " + tablePath);
    }

    return normalizeDatasetPath(location);
  }

  private static Map<String, String> buildTableOptions(String datasetPath) {
    Map<String, String> options = new HashMap<>();
    options.put(CONNECTOR_OPTION, LanceDynamicTableFactory.IDENTIFIER);
    options.put(PATH_OPTION, datasetPath);
    return options;
  }

  private static Schema toFlinkSchema(List<String> primaryKeys, RowType rowType) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    for (RowType.RowField field : rowType.getFields()) {
      schemaBuilder.column(field.getName(), LanceTypeConverter.toDataType(field.getType()));
    }
    if (!primaryKeys.isEmpty()) {
      schemaBuilder.primaryKey(primaryKeys);
    }
    return schemaBuilder.build();
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    rejectMetadataMutation(tablePath, "create");
    if (!databaseExists(tablePath.getDatabaseName())) {
      throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
    }
    if (ignoreIfExists && tableExists(tablePath)) {
      return;
    }

    TableSchemaSpec spec = validateAndExtractSchemaSpec(table);
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        attachPrimaryKeyMetadata(toArrowSchema(spec.rowType), spec.primaryKeys);

    byte[] ipcBytes;
    try {
      ipcBytes = serializeEmptyStream(arrowSchema);
    } catch (Exception e) {
      throw new CatalogException("Failed to serialize schema as Arrow IPC stream", e);
    }

    try {
      namespace.createTable(new CreateTableRequest().id(tableId(tablePath)), ipcBytes);
    } catch (RuntimeException e) {
      if (isNamespaceAlreadyExists(e)) {
        if (!ignoreIfExists) {
          throw new TableAlreadyExistException(getName(), tablePath);
        }
        return;
      }
      throw new CatalogException("Failed to create table: " + tablePath, e);
    }
  }

  private TableSchemaSpec validateAndExtractSchemaSpec(CatalogBaseTable table) {
    Map<String, String> options = new HashMap<>(table.getOptions());
    List<String> optionPrimaryKeys = parsePrimaryKeyOption(options.remove(PRIMARY_KEY_OPTION));

    if (options.containsKey(PATH_OPTION)) {
      throw new CatalogException(
          String.format(
              "Table option '%s' is not supported when creating tables in Lance namespace catalog. "
                  + "Table locations are managed by the configured Lance namespace. "
                  + "For CREATE TABLE LIKE, use EXCLUDING OPTIONS.",
              PATH_OPTION));
    }

    if (!options.isEmpty()) {
      throw new CatalogException(
          "Unsupported table options in Lance namespace catalog: "
              + new ArrayList<>(options.keySet())
              + ". Only '"
              + PRIMARY_KEY_OPTION
              + "' is supported. "
              + "For CREATE TABLE LIKE, use EXCLUDING OPTIONS.");
    }

    if (!(table instanceof ResolvedCatalogBaseTable<?> resolved)) {
      throw new CatalogException(
          "Expected ResolvedCatalogBaseTable, got: " + table.getClass().getName());
    }

    CatalogBaseTable origin = resolved.getOrigin();
    if (origin instanceof CatalogTable catalogTable && !catalogTable.getPartitionKeys().isEmpty()) {
      throw new CatalogException("Lance namespace catalog does not support partitioned tables");
    }

    if (!resolved.getResolvedSchema().getWatermarkSpecs().isEmpty()) {
      throw new CatalogException("Lance namespace catalog does not support watermark definitions");
    }

    if (resolved.getResolvedSchema().getColumns().stream()
        .anyMatch(column -> !column.isPhysical())) {
      throw new CatalogException(
          "Lance namespace catalog does not support computed or metadata columns");
    }

    LogicalType logicalType = resolved.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    if (!(logicalType instanceof RowType rowType)) {
      throw new CatalogException("Resolved schema is not a RowType: " + logicalType);
    }

    List<String> declaredPrimaryKeys =
        resolved
            .getResolvedSchema()
            .getPrimaryKey()
            .map(pk -> List.copyOf(pk.getColumns()))
            .orElse(Collections.emptyList());

    List<String> primaryKeys = mergePrimaryKeys(declaredPrimaryKeys, optionPrimaryKeys);

    if (!primaryKeys.isEmpty()) {
      validatePrimaryKeyColumns(rowType, primaryKeys);
      rowType = makeColumnsNotNull(rowType, primaryKeys);
    }

    return new TableSchemaSpec(rowType, primaryKeys);
  }

  private static List<String> parsePrimaryKeyOption(String value) {
    if (value == null || value.isBlank()) {
      return Collections.emptyList();
    }

    List<String> keys =
        Arrays.stream(value.split(PRIMARY_KEY_DELIMITER))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();

    Set<String> seen = new HashSet<>();
    for (String key : keys) {
      if (!seen.add(key)) {
        throw new CatalogException("Duplicate primary key column: " + key);
      }
    }

    return List.copyOf(keys);
  }

  private static List<String> mergePrimaryKeys(List<String> fromDdl, List<String> fromOption) {
    if (fromOption.isEmpty()) {
      return fromDdl;
    }
    if (fromDdl.isEmpty()) {
      return fromOption;
    }
    if (!fromDdl.equals(fromOption)) {
      throw new CatalogException(
          "Primary key declared in DDL "
              + fromDdl
              + " differs from table option '"
              + PRIMARY_KEY_OPTION
              + "' "
              + fromOption);
    }
    return fromDdl;
  }

  private static void validatePrimaryKeyColumns(RowType rowType, List<String> primaryKeys) {
    Set<String> fields =
        rowType.getFields().stream().map(RowType.RowField::getName).collect(Collectors.toSet());
    for (String primaryKey : primaryKeys) {
      if (!fields.contains(primaryKey)) {
        throw new CatalogException(
            "Primary key column '" + primaryKey + "' does not exist in table schema");
      }
    }
  }

  private static RowType makeColumnsNotNull(RowType rowType, List<String> columnNames) {
    Set<String> notNullSet = new HashSet<>(columnNames);
    List<RowType.RowField> fields = new ArrayList<>(rowType.getFields().size());
    for (RowType.RowField field : rowType.getFields()) {
      if (notNullSet.contains(field.getName()) && field.getType().isNullable()) {
        LogicalType notNull = field.getType().copy(false);
        fields.add(
            field
                .getDescription()
                .map(desc -> new RowType.RowField(field.getName(), notNull, desc))
                .orElseGet(() -> new RowType.RowField(field.getName(), notNull)));
      } else {
        fields.add(field);
      }
    }
    return new RowType(rowType.isNullable(), fields);
  }

  private record TableSchemaSpec(RowType rowType, List<String> primaryKeys) {}

  private static org.apache.arrow.vector.types.pojo.Schema attachPrimaryKeyMetadata(
      org.apache.arrow.vector.types.pojo.Schema arrowSchema, List<String> primaryKeys) {
    if (primaryKeys.isEmpty()) {
      return arrowSchema;
    }
    // TODO: Replace comma-separated primary-key metadata with native Lance keys or JSON encoding.
    Map<String, String> metadata = new HashMap<>(arrowSchema.getCustomMetadata());
    metadata.put(PRIMARY_KEY_METADATA, String.join(PRIMARY_KEY_DELIMITER, primaryKeys));
    return new org.apache.arrow.vector.types.pojo.Schema(arrowSchema.getFields(), metadata);
  }

  private static List<String> readPrimaryKeysFromMetadata(Map<String, String> customMetadata) {
    if (customMetadata == null) {
      return Collections.emptyList();
    }
    String packed = customMetadata.get(PRIMARY_KEY_METADATA);
    if (packed == null || packed.isBlank()) {
      return Collections.emptyList();
    }
    String[] parts = packed.split(PRIMARY_KEY_DELIMITER);
    List<String> keys = new ArrayList<>(parts.length);
    for (String p : parts) {
      String trimmed = p.trim();
      if (!trimmed.isEmpty()) {
        keys.add(trimmed);
      }
    }
    return keys;
  }

  private static void validatePrimaryKeysExist(RowType rowType, List<String> primaryKeys) {
    Set<String> fields =
        rowType.getFields().stream().map(RowType.RowField::getName).collect(Collectors.toSet());

    for (String primaryKey : primaryKeys) {
      if (!fields.contains(primaryKey)) {
        throw new CatalogException(
            "Primary key column '"
                + primaryKey
                + "' is stored in Lance metadata "
                + "but does not exist in dataset schema");
      }
    }
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    rejectMetadataMutation(tablePath, "drop");
    try {
      namespace.dropTable(new DropTableRequest().id(tableId(tablePath)));
    } catch (RuntimeException e) {
      if (isNamespaceNotFound(e)) {
        if (!ignoreIfNotExists) {
          throw new TableNotExistException(getName(), tablePath);
        }
        return;
      }
      throw new CatalogException("Failed to drop table: " + tablePath, e);
    }
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    rejectMetadataMutation(tablePath, "rename");
    parseMetadataSuffix(newTableName)
        .ifPresent(
            type -> {
              throw new CatalogException(
                  "Cannot rename Lance table '"
                      + tablePath
                      + "' to reserved metadata name '"
                      + newTableName
                      + "': $"
                      + type.suffix()
                      + " is a read-only virtual view.");
            });
    if (!tableExists(tablePath)) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath);
      }
      return;
    }

    try {
      namespace.renameTable(
          new RenameTableRequest().id(tableId(tablePath)).newTableName(newTableName));
    } catch (RuntimeException e) {
      if (isNamespaceUnsupported(e)) {
        throw new CatalogException(
            "Table rename is not supported by this Lance namespace implementation ("
                + namespace.getClass().getSimpleName()
                + "). Configure a Lance namespace that supports table rename.",
            e);
      }
      if (isNamespaceNotFound(e)) {
        if (!ignoreIfNotExists) {
          throw new TableNotExistException(getName(), tablePath);
        }
        return;
      }
      if (isNamespaceAlreadyExists(e)) {
        throw new CatalogException(
            "Cannot rename Lance table '"
                + tablePath
                + "' to '"
                + newTableName
                + "': a table with the target name already exists.",
            e);
      }
      throw new CatalogException(
          "Failed to rename Lance table '" + tablePath + "' to '" + newTableName + "'", e);
    }
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    rejectMetadataMutation(tablePath, "alter");
    if (!tableExists(tablePath)) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath);
      }
      return;
    }

    throw new CatalogException(
        "ALTER TABLE without explicit table changes is not supported by the Lance namespace"
            + " catalog. Use the SQL DDL path so Flink can provide the TableChange list.");
  }

  @Override
  public void alterTable(
      ObjectPath tablePath,
      CatalogBaseTable newTable,
      List<TableChange> tableChanges,
      boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    rejectMetadataMutation(tablePath, "alter");
    if (!tableExists(tablePath)) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath);
      }
      return;
    }
    if (tableChanges == null || tableChanges.isEmpty()) {
      return;
    }

    String datasetPath = resolveDatasetPath(tablePath);
    try (Dataset dataset = LanceDatasetOpener.open(allocator, datasetPath)) {
      org.apache.arrow.vector.types.pojo.Schema currentArrow = dataset.getSchema();
      RowType currentRowType = LanceTypeConverter.toFlinkRowType(currentArrow);
      List<String> currentPrimaryKeys =
          readPrimaryKeysFromMetadata(currentArrow.getCustomMetadata());

      LanceTableAlterPlanner.AlterPlan plan =
          LanceTableAlterPlanner.plan(currentRowType, currentPrimaryKeys, tableChanges);

      if (plan.isEmpty()) {
        return;
      }

      applyAlterPlan(dataset, plan);
    } catch (CatalogException e) {
      throw e;
    } catch (Exception e) {
      throw new CatalogException(
          "Failed to apply ALTER TABLE to Lance dataset: " + tablePath + " at " + datasetPath, e);
    }
  }

  private static void applyAlterPlan(Dataset dataset, LanceTableAlterPlanner.AlterPlan plan) {
    // TODO: Allow mixed-kind ALTER once Lance exposes atomic multi-op schema commits.
    // TODO: Re-plan and retry ALTER once Lance exposes typed schema-conflict errors.
    if (!plan.columnsToAdd().isEmpty()) {
      dataset.addColumns(new org.apache.arrow.vector.types.pojo.Schema(plan.columnsToAdd()));
    }
    if (!plan.columnsToDrop().isEmpty()) {
      dataset.dropColumns(plan.columnsToDrop());
    }
    if (!plan.columnsToRename().isEmpty()) {
      dataset.alterColumns(plan.columnsToRename());
    }
  }

  private static boolean isNamespaceUnsupported(RuntimeException e) {
    if (e instanceof org.lance.namespace.errors.UnsupportedOperationException) {
      return true;
    }
    return messageContains(e, "not supported");
  }

  // Partition / Function / Statistics (unsupported)

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> filters) {
    return Collections.emptyList();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException {
    throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
    return false;
  }

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists) {
    throw new CatalogException("Lance namespace catalog does not support partitions");
  }

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) {
    throw new CatalogException("Lance namespace catalog does not support partitions");
  }

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists) {
    throw new CatalogException("Lance namespace catalog does not support partitions");
  }

  @Override
  public List<String> listFunctions(String dbName)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(dbName)) {
      throw new DatabaseNotExistException(getName(), dbName);
    }
    return Collections.emptyList();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException {
    throw new FunctionNotExistException(getName(), functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(
      ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws CatalogException {
    throw new CatalogException("Lance namespace catalog does not support user-defined functions");
  }

  @Override
  public void alterFunction(
      ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new CatalogException("Lance namespace catalog does not support user-defined functions");
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new CatalogException("Lance namespace catalog does not support user-defined functions");
  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public void alterTableStatistics(
      ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new CatalogException("Lance namespace catalog does not support updating statistics");
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new CatalogException("Lance namespace catalog does not support updating statistics");
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics,
      boolean ignoreIfNotExists)
      throws CatalogException {
    throw new CatalogException("Lance namespace catalog does not support updating statistics");
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists)
      throws CatalogException {
    throw new CatalogException("Lance namespace catalog does not support updating statistics");
  }

  private void createDatabaseIfMissing(String database) {
    if (databaseExists(database)) {
      return;
    }
    try {
      namespace.createNamespace(new CreateNamespaceRequest().id(singletonList(database)));
    } catch (RuntimeException e) {
      if (!isNamespaceAlreadyExists(e)) {
        throw new CatalogException("Failed to create database: " + database, e);
      }
    }
  }

  // TODO: Replace message matching with structured Lance namespace errors.
  private static boolean isNamespaceNotFound(RuntimeException e) {
    return messageContains(e, "not found");
  }

  private static boolean isNamespaceAlreadyExists(RuntimeException e) {
    return messageContains(e, "already exists");
  }

  private static boolean isNamespaceNotEmpty(RuntimeException e) {
    return messageContains(e, "not empty");
  }

  private static boolean messageContains(Throwable t, String needle) {
    String lower = needle.toLowerCase(Locale.ROOT);
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      String msg = cur.getMessage();
      if (msg != null && msg.toLowerCase(Locale.ROOT).contains(lower)) {
        return true;
      }
      if (cur.getCause() == cur) {
        break;
      }
    }
    return false;
  }

  private static List<String> tableId(ObjectPath tablePath) {
    return List.of(tablePath.getDatabaseName(), tablePath.getObjectName());
  }

  private static String normalizeDatasetPath(String location) {
    try {
      URI uri = URI.create(location);
      if ("file".equalsIgnoreCase(uri.getScheme())) {
        return Paths.get(uri).toString();
      }
    } catch (IllegalArgumentException e) {
      // Treat location as a plain filesystem path.
    }
    return location;
  }

  private byte[] serializeEmptyStream(org.apache.arrow.vector.types.pojo.Schema arrowSchema)
      throws Exception {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      root.setRowCount(0);
      writer.start();
      writer.writeBatch();
      writer.end();
      return out.toByteArray();
    }
  }
}
