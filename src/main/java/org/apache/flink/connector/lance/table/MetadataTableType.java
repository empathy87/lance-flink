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

import org.lance.Branch;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Tag;
import org.lance.Version;
import org.lance.fragment.DeletionFile;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/** Supported Lance metadata table types and their schemas. */
public enum MetadataTableType {
  SNAPSHOTS("snapshots") {
    @Override
    public DataType rowDataType() {
      return DataTypes.ROW(
          DataTypes.FIELD("version_id", DataTypes.BIGINT().notNull()),
          DataTypes.FIELD("commit_time", DataTypes.TIMESTAMP_LTZ(3)),
          DataTypes.FIELD("metadata", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));
    }

    @Override
    public List<RowData> fetch(Dataset dataset, Map<String, String> sourceTableOptions) {
      List<RowData> out = new ArrayList<>();
      for (Version v : dataset.listVersions()) {
        GenericRowData row = new GenericRowData(3);
        row.setField(0, v.getId());
        Instant commitTime = v.getDataTime().toInstant();
        row.setField(1, TimestampData.fromInstant(commitTime));
        row.setField(2, toStringMap(v.getMetadata()));
        out.add(row);
      }
      return out;
    }
  },

  TAGS("tags") {
    @Override
    public DataType rowDataType() {
      return DataTypes.ROW(
          DataTypes.FIELD("tag_name", DataTypes.STRING().notNull()),
          DataTypes.FIELD("version_id", DataTypes.BIGINT()),
          DataTypes.FIELD("branch_name", DataTypes.STRING()),
          DataTypes.FIELD("manifest_size", DataTypes.INT()));
    }

    @Override
    public List<RowData> fetch(Dataset dataset, Map<String, String> sourceTableOptions) {
      List<RowData> out = new ArrayList<>();
      for (Tag t : dataset.tags().list()) {
        GenericRowData row = new GenericRowData(4);
        row.setField(0, StringData.fromString(t.getName()));
        row.setField(1, t.getVersion());
        row.setField(2, optionalString(t.getBranch()));
        row.setField(3, t.getManifestSize());
        out.add(row);
      }
      return out;
    }
  },

  BRANCHES("branches") {
    @Override
    public DataType rowDataType() {
      return DataTypes.ROW(
          DataTypes.FIELD("branch_name", DataTypes.STRING().notNull()),
          DataTypes.FIELD("parent_branch_name", DataTypes.STRING()),
          DataTypes.FIELD("parent_version_id", DataTypes.BIGINT()),
          DataTypes.FIELD("created_at", DataTypes.TIMESTAMP_LTZ(3)),
          DataTypes.FIELD("manifest_size", DataTypes.INT()));
    }

    @Override
    public List<RowData> fetch(Dataset dataset, Map<String, String> sourceTableOptions) {
      List<RowData> out = new ArrayList<>();
      for (Branch b : dataset.branches().list()) {
        GenericRowData row = new GenericRowData(5);
        row.setField(0, StringData.fromString(b.getName()));
        row.setField(1, optionalString(b.getParentBranch()));
        row.setField(2, b.getParentVersion());
        row.setField(3, TimestampData.fromEpochMillis(b.getCreateAt() * 1000L));
        row.setField(4, b.getManifestSize());
        out.add(row);
      }
      return out;
    }
  },

  FRAGMENTS("fragments") {
    @Override
    public DataType rowDataType() {
      return DataTypes.ROW(
          DataTypes.FIELD("fragment_id", DataTypes.INT().notNull()),
          DataTypes.FIELD("num_rows", DataTypes.BIGINT()),
          DataTypes.FIELD("physical_rows", DataTypes.BIGINT()),
          DataTypes.FIELD("num_deleted_rows", DataTypes.BIGINT()),
          DataTypes.FIELD("num_files", DataTypes.INT()),
          DataTypes.FIELD("has_deletion_file", DataTypes.BOOLEAN()));
    }

    @Override
    public List<RowData> fetch(Dataset dataset, Map<String, String> sourceTableOptions) {
      List<RowData> out = new ArrayList<>();
      for (Fragment frag : dataset.getFragments()) {
        FragmentMetadata meta = frag.metadata();
        GenericRowData row = new GenericRowData(6);
        row.setField(0, meta.getId());
        row.setField(1, meta.getNumRows());
        row.setField(2, meta.getPhysicalRows());
        row.setField(3, meta.getNumDeletions());
        row.setField(4, meta.getFiles() == null ? 0 : meta.getFiles().size());
        DeletionFile df = meta.getDeletionFile();
        row.setField(5, df != null);
        out.add(row);
      }
      return out;
    }
  },

  OPTIONS("options") {
    @Override
    public DataType rowDataType() {
      return DataTypes.ROW(
          DataTypes.FIELD("option_key", DataTypes.STRING().notNull()),
          DataTypes.FIELD("option_value", DataTypes.STRING()));
    }

    @Override
    public List<RowData> fetch(Dataset dataset, Map<String, String> sourceTableOptions) {
      List<RowData> out = new ArrayList<>();
      Map<String, String> sorted = new TreeMap<>(sourceTableOptions);
      for (Map.Entry<String, String> e : sorted.entrySet()) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, StringData.fromString(e.getKey()));
        row.setField(1, e.getValue() == null ? null : StringData.fromString(e.getValue()));
        out.add(row);
      }
      return out;
    }
  };

  private final String suffix;

  MetadataTableType(String suffix) {
    this.suffix = suffix;
  }

  public String suffix() {
    return suffix;
  }

  public abstract DataType rowDataType();

  public abstract List<RowData> fetch(Dataset dataset, Map<String, String> sourceTableOptions);

  public RowType rowType() {
    return (RowType) rowDataType().getLogicalType();
  }

  public Schema schema() {
    Schema.Builder builder = Schema.newBuilder();
    for (RowType.RowField field : rowType().getFields()) {
      builder.column(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
    }
    return builder.build();
  }

  public static Optional<MetadataTableType> fromSuffix(String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return Optional.empty();
    }
    String lower = suffix.toLowerCase(Locale.ROOT);
    for (MetadataTableType t : values()) {
      if (t.suffix.equals(lower)) {
        return Optional.of(t);
      }
    }
    return Optional.empty();
  }

  /** Splits {@code <table>$<suffix>} on the last {@code $}. */
  public static Optional<TableNameParts> splitName(String tableName) {
    if (tableName == null) {
      return Optional.empty();
    }
    int idx = tableName.lastIndexOf('$');
    if (idx <= 0 || idx == tableName.length() - 1) {
      return Optional.empty();
    }
    return Optional.of(
        new TableNameParts(tableName.substring(0, idx), tableName.substring(idx + 1)));
  }

  private static StringData optionalString(Optional<String> value) {
    return value.map(StringData::fromString).orElse(null);
  }

  private static GenericMapData toStringMap(Map<String, String> input) {
    if (input == null || input.isEmpty()) {
      return new GenericMapData(new HashMap<>());
    }
    Map<StringData, StringData> converted = new LinkedHashMap<>();
    for (Map.Entry<String, String> e : input.entrySet()) {
      converted.put(
          StringData.fromString(e.getKey()),
          e.getValue() == null ? null : StringData.fromString(e.getValue()));
    }
    return new GenericMapData(converted);
  }

  /** Result of splitting a {@code table$suffix} table name. */
  public static final class TableNameParts {
    private final String baseName;
    private final String suffix;

    public TableNameParts(String baseName, String suffix) {
      this.baseName = baseName;
      this.suffix = suffix;
    }

    public String baseName() {
      return baseName;
    }

    public String suffix() {
      return suffix;
    }
  }
}
