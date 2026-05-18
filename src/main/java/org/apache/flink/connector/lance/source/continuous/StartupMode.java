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
package org.apache.flink.connector.lance.source.continuous;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/** Continuous source startup-position semantics. */
public enum StartupMode {
  LATEST("latest", false),
  LATEST_FULL("latest-full", true),
  FROM_SNAPSHOT("from-snapshot", false),
  FROM_SNAPSHOT_FULL("from-snapshot-full", true),
  FROM_TAG("from-tag", false),
  FROM_TAG_FULL("from-tag-full", true),
  FROM_TIMESTAMP("from-timestamp", false),
  FROM_TIMESTAMP_FULL("from-timestamp-full", true),
  FROM_TIMESTAMP_MILLIS("from-timestamp-millis", false),
  FROM_TIMESTAMP_MILLIS_FULL("from-timestamp-millis-full", true);

  private final String configValue;
  private final boolean full;

  StartupMode(String configValue, boolean full) {
    this.configValue = configValue;
    this.full = full;
  }

  public String configValue() {
    return configValue;
  }

  /** Whether this mode emits the baseline snapshot at startup. */
  public boolean isFull() {
    return full;
  }

  public static StartupMode fromString(String value) {
    String normalized = value == null ? "" : value.toLowerCase(Locale.ROOT).trim();
    for (StartupMode mode : values()) {
      if (mode.configValue.equals(normalized)) {
        return mode;
      }
    }
    String supported =
        Arrays.stream(values()).map(StartupMode::configValue).collect(Collectors.joining(", "));
    throw new IllegalArgumentException(
        "Unsupported scan.startup-mode '" + value + "'. Supported values: " + supported);
  }
}
