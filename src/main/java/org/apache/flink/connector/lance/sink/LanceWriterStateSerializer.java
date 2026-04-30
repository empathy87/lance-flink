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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Serializer for append writer state. */
// TODO: Avoid Java serialization if Lance exposes a stable FragmentMetadata encoding.
public class LanceWriterStateSerializer implements SimpleVersionedSerializer<LanceWriterState> {

  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(LanceWriterState state) throws IOException {
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      FragmentMetadataSerialization.writeFragments(out, state.pendingFragments());
      out.flush();
      return bytes.toByteArray();
    }
  }

  @Override
  public LanceWriterState deserialize(int version, byte[] serialized) throws IOException {
    if (version != VERSION) {
      throw new IOException("Unsupported LanceWriterState version: " + version);
    }

    try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
        ObjectInputStream in = new ObjectInputStream(bytes)) {
      return new LanceWriterState(FragmentMetadataSerialization.readFragments(in));
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to deserialize FragmentMetadata", e);
    }
  }
}
