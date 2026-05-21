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
package org.apache.flink.connector.lance.table.procedures;

import org.apache.flink.connector.lance.LanceDatasetOpener;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.sink.LanceAppendCommittable;
import org.apache.flink.connector.lance.sink.LanceAppendCommitter;
import org.apache.flink.connector.lance.sink.LanceAppendWriter;

import org.lance.Dataset;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Shared fixtures for the {@code CALL sys.*(...)} ITCases. Subclasses share the {@code id BIGINT,
 * name STRING} table schema, the {@code my_catalog} Lance namespace catalog over a per-test
 * {@code @TempDir} warehouse, and the seeding helpers that drive fragment-shaped fixtures through
 * the sink API.
 */
abstract class AbstractLanceProcedureITCase {

  protected static final String CATALOG = "my_catalog";
  protected static final String DATABASE = "default";

  protected static final RowType ROW_TYPE =
      new RowType(
          List.of(
              new RowType.RowField("id", new BigIntType()),
              new RowType.RowField("name", new VarCharType())));

  @TempDir protected Path tempDir;

  protected BufferAllocator allocator;

  @BeforeEach
  void openAllocator() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void closeAllocator() {
    allocator.close();
  }

  protected TableEnvironment newTableEnv() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    return TableEnvironment.create(settings);
  }

  protected void createCatalogAndTable(TableEnvironment tEnv, String tableName) {
    String warehouseUri = tempDir.toUri().toString();
    tEnv.executeSql(
        "CREATE CATALOG "
            + CATALOG
            + " WITH ('type' = 'lance', 'warehouse' = '"
            + warehouseUri
            + "')");
    tEnv.executeSql("USE CATALOG " + CATALOG);
    tEnv.executeSql("CREATE TABLE " + tableName + " (id BIGINT, name STRING)");
  }

  protected String resolveTablePath(TableEnvironment tEnv, String tableName) {
    try {
      return tEnv.getCatalog(CATALOG)
          .orElseThrow()
          .getTable(new ObjectPath(DATABASE, tableName))
          .getOptions()
          .get("path");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Writes {@code fragments * rowsPerFragment} rows directly via the sink API. */
  protected void seedSmallFragments(String datasetPath, int fragments, int rowsPerFragment)
      throws Exception {
    LanceOptions options =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(rowsPerFragment * 2) // single flush per fragment
            .writeMaxRowsPerFile(rowsPerFragment)
            .build();

    LanceAppendWriter writer = new LanceAppendWriter(options, ROW_TYPE, 0, Collections.emptyList());
    Collection<LanceAppendCommittable> committables;
    try {
      long counter = 0;
      for (int f = 0; f < fragments; f++) {
        for (int r = 0; r < rowsPerFragment; r++) {
          writer.write(simpleRow(counter++, "row-" + counter), null);
        }
      }
      writer.flush(false);
      committables = writer.prepareCommit();
    } finally {
      writer.close();
    }

    LanceAppendCommitter committer = new LanceAppendCommitter(options, ROW_TYPE);
    try {
      List<Committer.CommitRequest<LanceAppendCommittable>> requests = new ArrayList<>();
      for (LanceAppendCommittable c : committables) {
        requests.add(new StubCommitRequest<>(c));
      }
      committer.commit(requests);
    } finally {
      committer.close();
    }
  }

  protected int countFragments(String path) {
    try (Dataset dataset = LanceDatasetOpener.open(allocator, path)) {
      return dataset.getFragments().size();
    }
  }

  protected static RowData simpleRow(long id, String name) {
    GenericRowData row = new GenericRowData(RowKind.INSERT, 2);
    row.setField(0, id);
    row.setField(1, StringData.fromString(name));
    return row;
  }

  protected static List<Row> collectRows(TableResult result) {
    List<Row> out = new ArrayList<>();
    try (CloseableIterator<Row> it = result.collect()) {
      while (it.hasNext()) {
        out.add(it.next());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return out;
  }

  protected interface ThrowingRunnable {
    void run() throws Exception;
  }

  /**
   * Runs {@code runnable}, returning the flattened message chain of any {@link Throwable} it
   * raises, or {@code null} on clean completion. Kept on {@code Throwable} rather than {@code
   * Exception} so Flink's wrapped planner/runtime errors surface intact.
   */
  @Nullable
  protected static String collectSqlErrorMessage(ThrowingRunnable runnable) {
    try {
      runnable.run();
      return null;
    } catch (Throwable t) {
      Throwable cur = t;
      StringBuilder sb = new StringBuilder();
      while (cur != null) {
        if (cur.getMessage() != null) {
          sb.append(cur.getMessage()).append('\n');
        }
        if (cur.getCause() == cur) {
          break;
        }
        cur = cur.getCause();
      }
      return sb.toString();
    }
  }

  /** Local equivalent of {@code sink.StubCommitRequest} (which is package-private). */
  protected static final class StubCommitRequest<T> implements Committer.CommitRequest<T> {
    private final T committable;

    StubCommitRequest(T committable) {
      this.committable = committable;
    }

    @Override
    public T getCommittable() {
      return committable;
    }

    @Override
    public int getNumberOfRetries() {
      return 0;
    }

    @Override
    public void signalFailedWithKnownReason(Throwable t) {}

    @Override
    public void signalFailedWithUnknownReason(Throwable t) {}

    @Override
    public void retryLater() {}

    @Override
    public void updateAndRetryLater(T committable) {}

    @Override
    public void signalAlreadyCommitted() {}
  }
}
