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
package org.apache.flink.connector.lance.sink;

import org.lance.FragmentMetadata;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

final class FragmentMetadataSerialization {

  private FragmentMetadataSerialization() {}

  static void writeFragments(ObjectOutputStream out, List<FragmentMetadata> fragments)
      throws IOException {
    out.writeInt(fragments.size());
    for (FragmentMetadata fragment : fragments) {
      out.writeObject(fragment);
    }
  }

  static List<FragmentMetadata> readFragments(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    int count = in.readInt();
    if (count < 0) {
      throw new IOException("Invalid fragment count: " + count);
    }

    List<FragmentMetadata> fragments = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      fragments.add((FragmentMetadata) in.readObject());
    }
    return fragments;
  }
}
