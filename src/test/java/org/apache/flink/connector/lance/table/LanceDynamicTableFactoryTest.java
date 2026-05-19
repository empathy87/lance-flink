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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LanceDynamicTableFactoryTest {

  // -- Batch mode (default) keeps working --
  @Test
  void batchModeIsDefault() {
    DynamicTableSource source = createSource(baseOptions());
    assertThat(source).isInstanceOf(LanceDynamicTableSource.class);
    assertThat(((LanceDynamicTableSource) source).isContinuous()).isFalse();
  }

  @Test
  void batchModeWithSnapshotIdStillWorks() {
    Map<String, String> options = baseOptions();
    options.put("scan.snapshot-id", "5");
    assertThat(createSource(options)).isInstanceOf(LanceDynamicTableSource.class);
  }

  // -- Continuous-only options rejected in batch --
  @Test
  void batchModeRejectsContinuousDiscoveryInterval() {
    Map<String, String> o = baseOptions();
    o.put("continuous.discovery-interval", "5s");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("continuous.discovery-interval")
        .hasMessageContaining("scan.mode = continuous");
  }

  @Test
  void batchModeRejectsStartupMode() {
    Map<String, String> o = baseOptions();
    o.put("scan.startup-mode", "latest-full");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.startup-mode");
  }

  // -- Continuous mode builds the source --
  @Test
  void continuousModeBuildsContinuousSource() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    DynamicTableSource s = createSource(o);
    assertThat(s).isInstanceOf(LanceDynamicTableSource.class);
    LanceDynamicTableSource lance = (LanceDynamicTableSource) s;
    assertThat(lance.isContinuous()).isTrue();
    assertThat(lance.getContinuousOptions()).isNotNull();
  }

  @Test
  void continuousLatestFullBuildsSource() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "latest-full");
    assertThat(((LanceDynamicTableSource) createSource(o)).isContinuous()).isTrue();
  }

  @Test
  void continuousFromSnapshotBuildsSource() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "from-snapshot");
    o.put("scan.startup-snapshot-id", "5");
    assertThat(((LanceDynamicTableSource) createSource(o)).isContinuous()).isTrue();
  }

  // -- Continuous rejects batch selectors --
  @Test
  void continuousRejectsBatchSnapshotIdSelector() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.snapshot-id", "5");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.snapshot-id")
        .hasMessageContaining("scan.mode = continuous");
  }

  @Test
  void continuousRejectsBatchVersionSelector() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.version", "5");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.version");
  }

  @Test
  void continuousRejectsBatchTagNameSelector() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.tag-name", "release");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.tag-name");
  }

  @Test
  void continuousRejectsBatchTimestampSelector() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.timestamp-millis", "1700000000000");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.timestamp-millis");
  }

  // -- Required-selector pairing --
  @Test
  void fromSnapshotRequiresStartupSnapshotId() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "from-snapshot");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("from-snapshot")
        .hasMessageContaining("scan.startup-snapshot-id");
  }

  @Test
  void fromTagRequiresStartupTagName() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "from-tag");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.startup-tag-name");
  }

  @Test
  void fromTimestampRequiresStartupTimestamp() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "from-timestamp");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.startup-timestamp");
  }

  @Test
  void fromTimestampMillisRequiresStartupTimestampMillis() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "from-timestamp-millis");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.startup-timestamp-millis");
  }

  @Test
  void fromTagWithBlankTagNameFailsAsMissingSelector() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "from-tag");
    o.put("scan.startup-tag-name", "   ");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.startup-tag-name");
  }

  @Test
  void fromTimestampWithBlankTimestampFailsAsMissingSelector() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "from-timestamp");
    o.put("scan.startup-timestamp", "");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.startup-timestamp");
  }

  @Test
  void fromSnapshotRejectsTagSelector() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "from-snapshot");
    o.put("scan.startup-tag-name", "release");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("from-snapshot")
        .hasMessageContaining("requires scan.startup-snapshot-id");
  }

  @Test
  void latestRejectsAnyStartupSelector() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "latest");
    o.put("scan.startup-snapshot-id", "5");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("does not accept a startup selector");
  }

  @Test
  void twoStartupSelectorsAreMutuallyExclusive() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("scan.startup-mode", "from-snapshot");
    o.put("scan.startup-snapshot-id", "5");
    o.put("scan.startup-tag-name", "release");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("At most one")
        .hasMessageContaining("startup selector");
  }

  @Test
  void discoveryIntervalMustBePositive() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("continuous.discovery-interval", "0s");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must be positive");
  }

  // -- PK / upsert tables rejected in continuous mode --
  @Test
  void continuousModeWithPrimaryKeyRejected() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    assertThatThrownBy(
            () ->
                new LanceDynamicTableFactory()
                    .createDynamicTableSource(context(o, /* withPrimaryKey */ true)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Continuous reads from PK")
        .hasMessageContaining("scan.mode=batch");
  }

  // -- Sink rejects all source-side options --
  @Test
  void sinkRejectsScanModeContinuous() {
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    assertThatThrownBy(() -> createSink(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("only supported for reads");
  }

  @Test
  void sinkRejectsScanModeBatch() {
    // Explicit scan.mode=batch on a sink is still a read-side option; reject just like continuous.
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "batch");
    assertThatThrownBy(() -> createSink(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("only supported for reads");
  }

  @Test
  void sinkStillWorksForPlainOptions() {
    assertThat(createSink(baseOptions())).isInstanceOf(LanceDynamicTableSink.class);
  }

  @Test
  void overwritePolicyOptionIsNoLongerAccepted() {
    // scan.overwrite-policy was removed; FactoryUtil's validate() rejects it as unknown.
    Map<String, String> o = baseOptions();
    o.put("scan.overwrite-policy", "fail");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.overwrite-policy");
  }

  // -- Metadata table rejects continuous --
  @Test
  void metadataTableRejectsScanMode() {
    Map<String, String> o = baseOptions();
    o.put("metadata-type", "snapshots");
    o.put("scan.mode", "continuous");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.mode");
  }

  @Test
  void metadataTableRejectsDiscoveryInterval() {
    Map<String, String> o = baseOptions();
    o.put("metadata-type", "snapshots");
    o.put("continuous.discovery-interval", "5s");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("continuous.discovery-interval");
  }

  // -- Lookup options --
  @Test
  void batchSourceCarriesParsedLookupConfig() {
    Map<String, String> o = baseOptions();
    o.put("lookup.allow-full-scan", "true");
    o.put("lookup.cache", "PARTIAL");
    o.put("lookup.partial-cache.max-rows", "100");
    DynamicTableSource source = createSource(o);
    LanceDynamicTableSource lance = (LanceDynamicTableSource) source;
    assertThat(lance.getLookupConfig().allowFullScan()).isTrue();
    assertThat(lance.getLookupConfig().cacheType().name()).isEqualTo("PARTIAL");
    assertThat(lance.getLookupConfig().partialCacheConfig()).isPresent();
  }

  @Test
  void continuousSourceAlsoAcceptsLookupOptions() {
    // Continuous sources still accept lookup options (rejection happens at lookup-time, not
    // catalog-time); a streaming-only table with a lookup option must still create successfully.
    Map<String, String> o = baseOptions();
    o.put("scan.mode", "continuous");
    o.put("lookup.allow-full-scan", "true");
    assertThat(createSource(o)).isInstanceOf(LanceDynamicTableSource.class);
  }

  @Test
  void metadataTableAcceptsLookupOptionsAtCatalogTime() {
    // The spec is explicit: a table valid for normal scan use must not become invalid merely
    // because lookup-only options are unsupported for it. The rejection happens later, when the
    // metadata table source is actually used as a lookup source.
    Map<String, String> o = baseOptions();
    o.put("metadata-type", "snapshots");
    o.put("lookup.allow-full-scan", "true");
    o.put("lookup.cache", "PARTIAL");
    assertThat(createSource(o)).isInstanceOf(LanceMetadataTableSource.class);
  }

  @Test
  void sinkRejectsLookupAllowFullScan() {
    Map<String, String> o = baseOptions();
    o.put("lookup.allow-full-scan", "true");
    assertThatThrownBy(() -> createSink(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("lookup.allow-full-scan")
        .hasMessageContaining("only supported for reads");
  }

  @Test
  void sinkRejectsLookupCache() {
    Map<String, String> o = baseOptions();
    o.put("lookup.cache", "PARTIAL");
    assertThatThrownBy(() -> createSink(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("lookup.cache")
        .hasMessageContaining("only supported for reads");
  }

  @Test
  void unknownLookupOptionStillRejected() {
    Map<String, String> o = baseOptions();
    o.put("lookup.unknown-option", "true");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("lookup.unknown-option");
  }

  @Test
  void maxRetriesIsNotAcceptedSilently() {
    // lookup.max-retries is a standard FLIP-221 key, but the connector does not implement
    // retrying lookups. The factory must surface this rather than appear to honor the option.
    Map<String, String> o = baseOptions();
    o.put("lookup.max-retries", "3");
    assertThatThrownBy(() -> createSource(o))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("lookup.max-retries");
  }

  // -- Helpers --
  private static Map<String, String> baseOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", LanceDynamicTableFactory.IDENTIFIER);
    options.put("path", "/tmp/lance-factory-test");
    return options;
  }

  private static DynamicTableSource createSource(Map<String, String> options) {
    return new LanceDynamicTableFactory().createDynamicTableSource(context(options, false));
  }

  private static DynamicTableSink createSink(Map<String, String> options) {
    return new LanceDynamicTableFactory().createDynamicTableSink(context(options, false));
  }

  private static DynamicTableFactory.Context context(
      Map<String, String> options, boolean withPrimaryKey) {
    ResolvedSchema schema =
        withPrimaryKey
            ? new ResolvedSchema(
                List.of(
                    Column.physical("id", DataTypes.BIGINT().notNull()),
                    Column.physical("name", DataTypes.STRING())),
                List.of(),
                UniqueConstraint.primaryKey("pk", List.of("id")))
            : ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT()),
                Column.physical("name", DataTypes.STRING()));
    CatalogTable catalogTable =
        CatalogTable.of(
            Schema.newBuilder().fromResolvedSchema(schema).build(), null, List.of(), options);
    ResolvedCatalogTable resolved = new ResolvedCatalogTable(catalogTable, schema);
    return new FactoryUtil.DefaultDynamicTableContext(
        ObjectIdentifier.of("default_catalog", "default_database", "lance_test"),
        resolved,
        Map.of(),
        new Configuration(),
        Thread.currentThread().getContextClassLoader(),
        false);
  }
}
