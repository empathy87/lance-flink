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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Versioned binary serializer for {@link LanceSourceEnumState}. */
public class LanceSourceEnumStateSerializer
    implements SimpleVersionedSerializer<LanceSourceEnumState> {

  public static final LanceSourceEnumStateSerializer INSTANCE =
      new LanceSourceEnumStateSerializer();

  private static final int VERSION = 1;

  private final LanceSourceSplitSerializer splitSerializer = LanceSourceSplitSerializer.INSTANCE;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(LanceSourceEnumState state) throws IOException {
    List<LanceSourceSplit> splits = state.remainingSplits();
    DataOutputSerializer out = new DataOutputSerializer(256);
    out.writeInt(splitSerializer.getVersion());
    out.writeInt(splits.size());
    for (LanceSourceSplit split : splits) {
      byte[] bytes = splitSerializer.serialize(split);
      out.writeInt(bytes.length);
      out.write(bytes);
    }
    return out.getCopyOfBuffer();
  }

  @Override
  public LanceSourceEnumState deserialize(int version, byte[] bytes) throws IOException {
    if (version != VERSION) {
      throw new IOException("Unknown LanceSourceEnumState version: " + version);
    }
    DataInputDeserializer in = new DataInputDeserializer(bytes);
    int splitVersion = in.readInt();
    int count = in.readInt();
    if (count < 0) {
      throw new IOException("Invalid LanceSourceEnumState split count: " + count);
    }
    List<LanceSourceSplit> splits = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      int len = in.readInt();
      if (len < 0) {
        throw new IOException("Invalid LanceSourceSplit payload length: " + len);
      }
      byte[] splitBytes = new byte[len];
      in.readFully(splitBytes);
      splits.add(splitSerializer.deserialize(splitVersion, splitBytes));
    }
    return new LanceSourceEnumState(splits);
  }
}
