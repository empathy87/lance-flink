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
package org.apache.flink.connector.lance.config;

import java.io.Serializable;
import java.util.Objects;

/**
 * Lance connector configuration POJO.
 *
 * <p>Holds typed values for source / sink / vector index / vector search / catalog. The
 * authoritative {@code ConfigOption<?>} keys live on the Flink factories ({@link
 * org.apache.flink.connector.lance.table.LanceDynamicTableFactory}, {@link
 * org.apache.flink.connector.lance.table.LanceCatalogFactory}); this class is a plain bag built via
 * {@link Builder}.
 */
public class LanceOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Vector index type. */
  public enum IndexType {
    IVF_PQ("IVF_PQ"),
    IVF_HNSW("IVF_HNSW"),
    IVF_FLAT("IVF_FLAT");

    private final String value;

    IndexType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static IndexType fromValue(String value) {
      for (IndexType type : values()) {
        if (type.value.equalsIgnoreCase(value)) {
          return type;
        }
      }
      throw new IllegalArgumentException(
          "Unsupported index type: " + value + ", supported types: IVF_PQ, IVF_HNSW, IVF_FLAT");
    }
  }

  /** Distance metric type. */
  public enum MetricType {
    L2("L2"),
    COSINE("Cosine"),
    DOT("Dot");

    private final String value;

    MetricType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static MetricType fromValue(String value) {
      for (MetricType type : values()) {
        if (type.value.equalsIgnoreCase(value)) {
          return type;
        }
      }
      throw new IllegalArgumentException(
          "Unsupported metric type: " + value + ", supported types: L2, Cosine, Dot");
    }
  }

  private final String path;
  private final int readBatchSize;
  private final int writeBatchSize;
  private final int writeMaxRowsPerFile;
  private final IndexType indexType;
  private final String indexColumn;
  private final int indexNumPartitions;
  private final Integer indexNumSubVectors;
  private final int indexNumBits;
  private final int indexMaxLevel;
  private final int indexM;
  private final int indexEfConstruction;
  private final String vectorColumn;
  private final MetricType vectorMetric;
  private final int vectorNprobes;
  private final int vectorEf;
  private final Integer vectorRefineFactor;
  private final String defaultDatabase;
  private final String warehouse;

  private LanceOptions(Builder builder) {
    this.path = builder.path;
    this.readBatchSize = builder.readBatchSize;
    this.writeBatchSize = builder.writeBatchSize;
    this.writeMaxRowsPerFile = builder.writeMaxRowsPerFile;
    this.indexType = builder.indexType;
    this.indexColumn = builder.indexColumn;
    this.indexNumPartitions = builder.indexNumPartitions;
    this.indexNumSubVectors = builder.indexNumSubVectors;
    this.indexNumBits = builder.indexNumBits;
    this.indexMaxLevel = builder.indexMaxLevel;
    this.indexM = builder.indexM;
    this.indexEfConstruction = builder.indexEfConstruction;
    this.vectorColumn = builder.vectorColumn;
    this.vectorMetric = builder.vectorMetric;
    this.vectorNprobes = builder.vectorNprobes;
    this.vectorEf = builder.vectorEf;
    this.vectorRefineFactor = builder.vectorRefineFactor;
    this.defaultDatabase = builder.defaultDatabase;
    this.warehouse = builder.warehouse;
  }

  public String getPath() {
    return path;
  }

  public int getReadBatchSize() {
    return readBatchSize;
  }

  public int getWriteBatchSize() {
    return writeBatchSize;
  }

  public int getWriteMaxRowsPerFile() {
    return writeMaxRowsPerFile;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public String getIndexColumn() {
    return indexColumn;
  }

  public int getIndexNumPartitions() {
    return indexNumPartitions;
  }

  public Integer getIndexNumSubVectors() {
    return indexNumSubVectors;
  }

  public int getIndexNumBits() {
    return indexNumBits;
  }

  public int getIndexMaxLevel() {
    return indexMaxLevel;
  }

  public int getIndexM() {
    return indexM;
  }

  public int getIndexEfConstruction() {
    return indexEfConstruction;
  }

  public String getVectorColumn() {
    return vectorColumn;
  }

  public MetricType getVectorMetric() {
    return vectorMetric;
  }

  public int getVectorNprobes() {
    return vectorNprobes;
  }

  public int getVectorEf() {
    return vectorEf;
  }

  public Integer getVectorRefineFactor() {
    return vectorRefineFactor;
  }

  public String getDefaultDatabase() {
    return defaultDatabase;
  }

  public String getWarehouse() {
    return warehouse;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Configuration builder */
  public static class Builder {
    private String path;
    private int readBatchSize = 1024;
    private int writeBatchSize = 1024;
    private int writeMaxRowsPerFile = 1000000;
    private IndexType indexType = IndexType.IVF_PQ;
    private String indexColumn;
    private int indexNumPartitions = 256;
    private Integer indexNumSubVectors;
    private int indexNumBits = 8;
    private int indexMaxLevel = 7;
    private int indexM = 16;
    private int indexEfConstruction = 100;
    private String vectorColumn;
    private MetricType vectorMetric = MetricType.L2;
    private int vectorNprobes = 20;
    private int vectorEf = 100;
    private Integer vectorRefineFactor;
    private String defaultDatabase = "default";
    private String warehouse;

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder readBatchSize(int readBatchSize) {
      this.readBatchSize = readBatchSize;
      return this;
    }

    public Builder writeBatchSize(int writeBatchSize) {
      this.writeBatchSize = writeBatchSize;
      return this;
    }

    public Builder writeMaxRowsPerFile(int writeMaxRowsPerFile) {
      this.writeMaxRowsPerFile = writeMaxRowsPerFile;
      return this;
    }

    public Builder indexType(IndexType indexType) {
      this.indexType = indexType;
      return this;
    }

    public Builder indexColumn(String indexColumn) {
      this.indexColumn = indexColumn;
      return this;
    }

    public Builder indexNumPartitions(int indexNumPartitions) {
      this.indexNumPartitions = indexNumPartitions;
      return this;
    }

    public Builder indexNumSubVectors(Integer indexNumSubVectors) {
      this.indexNumSubVectors = indexNumSubVectors;
      return this;
    }

    public Builder indexNumBits(int indexNumBits) {
      this.indexNumBits = indexNumBits;
      return this;
    }

    public Builder indexMaxLevel(int indexMaxLevel) {
      this.indexMaxLevel = indexMaxLevel;
      return this;
    }

    public Builder indexM(int indexM) {
      this.indexM = indexM;
      return this;
    }

    public Builder indexEfConstruction(int indexEfConstruction) {
      this.indexEfConstruction = indexEfConstruction;
      return this;
    }

    public Builder vectorColumn(String vectorColumn) {
      this.vectorColumn = vectorColumn;
      return this;
    }

    public Builder vectorMetric(MetricType vectorMetric) {
      this.vectorMetric = vectorMetric;
      return this;
    }

    public Builder vectorNprobes(int vectorNprobes) {
      this.vectorNprobes = vectorNprobes;
      return this;
    }

    public Builder vectorEf(int vectorEf) {
      this.vectorEf = vectorEf;
      return this;
    }

    public Builder vectorRefineFactor(Integer vectorRefineFactor) {
      this.vectorRefineFactor = vectorRefineFactor;
      return this;
    }

    public Builder defaultDatabase(String defaultDatabase) {
      this.defaultDatabase = defaultDatabase;
      return this;
    }

    public Builder warehouse(String warehouse) {
      this.warehouse = warehouse;
      return this;
    }

    /** Build LanceOptions instance with validation */
    public LanceOptions build() {
      validate();
      return new LanceOptions(this);
    }

    /** Validate configuration */
    private void validate() {
      if (readBatchSize <= 0) {
        throw new IllegalArgumentException(
            "read.batch-size must be greater than 0, current value: " + readBatchSize);
      }
      if (writeBatchSize <= 0) {
        throw new IllegalArgumentException(
            "write.batch-size must be greater than 0, current value: " + writeBatchSize);
      }
      if (writeMaxRowsPerFile <= 0) {
        throw new IllegalArgumentException(
            "write.max-rows-per-file must be greater than 0, current value: "
                + writeMaxRowsPerFile);
      }
      if (indexNumPartitions <= 0) {
        throw new IllegalArgumentException(
            "index.num-partitions must be greater than 0, current value: " + indexNumPartitions);
      }
      if (indexNumSubVectors != null && indexNumSubVectors <= 0) {
        throw new IllegalArgumentException(
            "index.num-sub-vectors must be greater than 0, current value: " + indexNumSubVectors);
      }
      if (indexNumBits <= 0 || indexNumBits > 16) {
        throw new IllegalArgumentException(
            "index.num-bits must be between 1 and 16, current value: " + indexNumBits);
      }
      if (indexMaxLevel <= 0) {
        throw new IllegalArgumentException(
            "index.max-level must be greater than 0, current value: " + indexMaxLevel);
      }
      if (indexM <= 0) {
        throw new IllegalArgumentException(
            "index.m must be greater than 0, current value: " + indexM);
      }
      if (indexEfConstruction <= 0) {
        throw new IllegalArgumentException(
            "index.ef-construction must be greater than 0, current value: " + indexEfConstruction);
      }
      if (vectorNprobes <= 0) {
        throw new IllegalArgumentException(
            "vector.nprobes must be greater than 0, current value: " + vectorNprobes);
      }
      if (vectorEf <= 0) {
        throw new IllegalArgumentException(
            "vector.ef must be greater than 0, current value: " + vectorEf);
      }
      if (vectorRefineFactor != null && vectorRefineFactor <= 0) {
        throw new IllegalArgumentException(
            "vector.refine-factor must be greater than 0, current value: " + vectorRefineFactor);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LanceOptions that = (LanceOptions) o;
    return readBatchSize == that.readBatchSize
        && writeBatchSize == that.writeBatchSize
        && writeMaxRowsPerFile == that.writeMaxRowsPerFile
        && indexNumPartitions == that.indexNumPartitions
        && indexNumBits == that.indexNumBits
        && indexMaxLevel == that.indexMaxLevel
        && indexM == that.indexM
        && indexEfConstruction == that.indexEfConstruction
        && vectorNprobes == that.vectorNprobes
        && vectorEf == that.vectorEf
        && Objects.equals(path, that.path)
        && indexType == that.indexType
        && Objects.equals(indexColumn, that.indexColumn)
        && Objects.equals(indexNumSubVectors, that.indexNumSubVectors)
        && Objects.equals(vectorColumn, that.vectorColumn)
        && vectorMetric == that.vectorMetric
        && Objects.equals(vectorRefineFactor, that.vectorRefineFactor)
        && Objects.equals(defaultDatabase, that.defaultDatabase)
        && Objects.equals(warehouse, that.warehouse);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        path,
        readBatchSize,
        writeBatchSize,
        writeMaxRowsPerFile,
        indexType,
        indexColumn,
        indexNumPartitions,
        indexNumSubVectors,
        indexNumBits,
        indexMaxLevel,
        indexM,
        indexEfConstruction,
        vectorColumn,
        vectorMetric,
        vectorNprobes,
        vectorEf,
        vectorRefineFactor,
        defaultDatabase,
        warehouse);
  }

  @Override
  public String toString() {
    return "LanceOptions{"
        + "path='"
        + path
        + '\''
        + ", readBatchSize="
        + readBatchSize
        + ", writeBatchSize="
        + writeBatchSize
        + ", writeMaxRowsPerFile="
        + writeMaxRowsPerFile
        + ", indexType="
        + indexType
        + ", indexColumn='"
        + indexColumn
        + '\''
        + ", indexNumPartitions="
        + indexNumPartitions
        + ", indexNumSubVectors="
        + indexNumSubVectors
        + ", indexNumBits="
        + indexNumBits
        + ", indexMaxLevel="
        + indexMaxLevel
        + ", indexM="
        + indexM
        + ", indexEfConstruction="
        + indexEfConstruction
        + ", vectorColumn='"
        + vectorColumn
        + '\''
        + ", vectorMetric="
        + vectorMetric
        + ", vectorNprobes="
        + vectorNprobes
        + ", vectorEf="
        + vectorEf
        + ", vectorRefineFactor="
        + vectorRefineFactor
        + ", defaultDatabase='"
        + defaultDatabase
        + '\''
        + ", warehouse='"
        + warehouse
        + '\''
        + '}';
  }
}
