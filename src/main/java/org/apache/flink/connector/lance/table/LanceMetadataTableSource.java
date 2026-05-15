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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;

import java.util.HashMap;
import java.util.Map;

/** Read-only source for Lance metadata tables. */
public class LanceMetadataTableSource implements ScanTableSource {

  private final String path;
  private final MetadataTableType type;
  private final Map<String, String> sourceTableOptions;

  public LanceMetadataTableSource(
      String path, MetadataTableType type, Map<String, String> sourceTableOptions) {
    this.path = path;
    this.type = type;
    this.sourceTableOptions = new HashMap<>(sourceTableOptions);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    return InputFormatProvider.of(new LanceMetadataInputFormat(path, type, sourceTableOptions));
  }

  @Override
  public DynamicTableSource copy() {
    return new LanceMetadataTableSource(path, type, sourceTableOptions);
  }

  @Override
  public String asSummaryString() {
    return "Lance Metadata Table Source (" + type.suffix() + ")";
  }
}
