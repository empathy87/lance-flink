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

import org.lance.Dataset;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.RowData;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Single-split input format for Lance metadata tables. */
public class LanceMetadataInputFormat extends GenericInputFormat<RowData> {

  private static final long serialVersionUID = 1L;

  private final String path;
  private final MetadataTableType type;
  private final Map<String, String> sourceTableOptions;

  private transient List<RowData> rows;
  private transient int position;

  public LanceMetadataInputFormat(
      String path, MetadataTableType type, Map<String, String> sourceTableOptions) {
    this.path = path;
    this.type = type;
    this.sourceTableOptions = new HashMap<>(sourceTableOptions);
  }

  @Override
  public GenericInputSplit[] createInputSplits(int minNumSplits) {
    return new GenericInputSplit[] {new GenericInputSplit(0, 1)};
  }

  @Override
  public DefaultInputSplitAssigner getInputSplitAssigner(GenericInputSplit[] splits) {
    return new DefaultInputSplitAssigner(splits);
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
    return null;
  }

  @Override
  public void open(GenericInputSplit split) throws IOException {
    super.open(split);
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        Dataset ds = LanceDatasetOpener.open(alloc, path)) {
      rows = type.fetch(ds, sourceTableOptions);
    } catch (Exception e) {
      throw new IOException("Failed to read Lance metadata (" + type.suffix() + ") at " + path, e);
    }
    position = 0;
  }

  @Override
  public boolean reachedEnd() {
    return position >= rows.size();
  }

  @Override
  public RowData nextRecord(RowData reuse) {
    return rows.get(position++);
  }
}
