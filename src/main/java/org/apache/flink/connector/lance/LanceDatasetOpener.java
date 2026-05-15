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
package org.apache.flink.connector.lance;

import org.lance.Dataset;
import org.lance.ReadOptions;

import org.apache.arrow.memory.BufferAllocator;

import javax.annotation.Nullable;

/** Opens a Lance {@link Dataset}, pinning to a specific version when one is provided. */
public final class LanceDatasetOpener {

  private LanceDatasetOpener() {}

  public static Dataset open(BufferAllocator allocator, String path, @Nullable Long version) {
    if (version == null) {
      return Dataset.open().allocator(allocator).uri(path).build();
    }
    ReadOptions readOptions = new ReadOptions.Builder().setVersion(version).build();
    return Dataset.open().readOptions(readOptions).allocator(allocator).uri(path).build();
  }
}
