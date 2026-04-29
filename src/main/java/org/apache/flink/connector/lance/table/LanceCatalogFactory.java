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

import org.lance.namespace.DirectoryNamespace;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Factory for {@link LanceNamespaceCatalog}.
 *
 * <pre>{@code
 * CREATE CATALOG my_catalog WITH (
 *     'type' = 'lance',
 *     'warehouse' = 'file:/tmp/lance'
 * );
 * }</pre>
 */
public class LanceCatalogFactory implements CatalogFactory {

  public static final String IDENTIFIER = "lance";

  private static final String WAREHOUSE_KEY = "warehouse";
  private static final String DEFAULT_DATABASE_KEY = "default-database";
  private static final String NAMESPACE_ROOT_KEY = "root";

  private static final Set<String> SUPPORTED_OPTIONS =
      Set.of("type", "property-version", WAREHOUSE_KEY, DEFAULT_DATABASE_KEY);

  public static final ConfigOption<String> WAREHOUSE =
      ConfigOptions.key(WAREHOUSE_KEY)
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Lance warehouse location for the directory namespace, for example 'file:/tmp/lance'.");

  public static final ConfigOption<String> DEFAULT_DATABASE =
      ConfigOptions.key(DEFAULT_DATABASE_KEY)
          .stringType()
          .defaultValue(LanceNamespaceCatalog.DEFAULT_DATABASE)
          .withDescription("Default database name for the Lance catalog.");

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(WAREHOUSE);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(DEFAULT_DATABASE);
    return options;
  }

  @Override
  public Catalog createCatalog(Context context) {
    Map<String, String> rawOptions = context.getOptions();

    for (String option : rawOptions.keySet()) {
      if (!SUPPORTED_OPTIONS.contains(option)) {
        throw new ValidationException(
            String.format(
                "Unsupported option '%s' for Lance catalog. "
                    + "Supported Lance catalog options are: '%s', '%s'.",
                option, WAREHOUSE_KEY, DEFAULT_DATABASE_KEY));
      }
    }

    String warehouse = rawOptions.get(WAREHOUSE_KEY);
    if (warehouse == null || warehouse.isBlank()) {
      throw new ValidationException(
          "Option '" + WAREHOUSE_KEY + "' must not be blank for Lance catalog.");
    }

    String defaultDatabase =
        rawOptions.getOrDefault(DEFAULT_DATABASE_KEY, LanceNamespaceCatalog.DEFAULT_DATABASE);
    if (defaultDatabase.isBlank()) {
      throw new ValidationException(
          "Option '" + DEFAULT_DATABASE_KEY + "' must not be blank for Lance catalog.");
    }

    Map<String, String> options = new HashMap<>();
    options.put(NAMESPACE_ROOT_KEY, warehouse);

    DirectoryNamespace namespace = new DirectoryNamespace();

    return new LanceNamespaceCatalog(context.getName(), defaultDatabase, namespace, options);
  }
}
