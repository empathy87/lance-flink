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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Source-level option holder shared across batch and continuous reads. */
public final class LanceSourceOptions {

  private LanceSourceOptions() {}

  public static final ConfigOption<String> SCAN_MODE =
      ConfigOptions.key("scan.mode")
          .stringType()
          .defaultValue(LanceScanMode.BATCH.configValue())
          .withDescription(
              "Source mode: 'batch' (bounded, default) or 'continuous' (FLIP-27 streaming source"
                  + " that polls the Lance dataset for new versions).");
}
