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
package org.apache.flink.connector.lance.source;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/** Source-level switch between bounded and continuous reads. */
public enum LanceScanMode {
  BATCH("batch"),
  CONTINUOUS("continuous");

  private final String configValue;

  LanceScanMode(String configValue) {
    this.configValue = configValue;
  }

  public String configValue() {
    return configValue;
  }

  public static LanceScanMode fromString(String value) {
    String normalized = value == null ? "" : value.toLowerCase(Locale.ROOT).trim();
    for (LanceScanMode mode : values()) {
      if (mode.configValue.equals(normalized)) {
        return mode;
      }
    }
    String supported =
        Arrays.stream(values()).map(LanceScanMode::configValue).collect(Collectors.joining(", "));
    throw new IllegalArgumentException(
        "Unsupported scan.mode '" + value + "'. Supported values: " + supported);
  }
}
