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

import org.apache.flink.connector.lance.source.LanceSourceSplit;
import org.apache.flink.connector.lance.source.LanceSourceSplitSerializer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Versioned binary serializer for {@link LanceContinuousEnumState}. */
public class LanceContinuousEnumStateSerializer
    implements SimpleVersionedSerializer<LanceContinuousEnumState> {

  public static final LanceContinuousEnumStateSerializer INSTANCE =
      new LanceContinuousEnumStateSerializer();

  private static final int VERSION = 1;
  private final LanceSourceSplitSerializer splitSerializer = LanceSourceSplitSerializer.INSTANCE;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(LanceContinuousEnumState state) throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(256);
    out.writeLong(state.lastEnumeratedVersion());

    String fp = state.schemaFingerprint();
    out.writeBoolean(fp != null);
    if (fp != null) {
      out.writeUTF(fp);
    }

    out.writeInt(state.knownFragmentIds().size());
    for (Integer id : new TreeSet<>(state.knownFragmentIds())) {
      out.writeInt(id);
    }

    out.writeInt(splitSerializer.getVersion());
    List<LanceSourceSplit> splits = state.remainingSplits();
    out.writeInt(splits.size());
    for (LanceSourceSplit s : splits) {
      byte[] bytes = splitSerializer.serialize(s);
      out.writeInt(bytes.length);
      out.write(bytes);
    }
    return out.getCopyOfBuffer();
  }

  @Override
  public LanceContinuousEnumState deserialize(int version, byte[] bytes) throws IOException {
    if (version != VERSION) {
      throw new IOException("Unknown LanceContinuousEnumState version: " + version);
    }
    DataInputDeserializer in = new DataInputDeserializer(bytes);
    long lastVer = in.readLong();

    boolean hasFp = in.readBoolean();
    String fp = hasFp ? in.readUTF() : null;

    int knownCount = in.readInt();
    if (knownCount < 0) {
      throw new IOException("Invalid known-fragment count: " + knownCount);
    }
    Set<Integer> known = new HashSet<>(Math.max(knownCount, 8));
    for (int i = 0; i < knownCount; i++) {
      known.add(in.readInt());
    }

    int splitVersion = in.readInt();
    int splitCount = in.readInt();
    if (splitCount < 0) {
      throw new IOException("Invalid split count: " + splitCount);
    }
    List<LanceSourceSplit> splits = new ArrayList<>(splitCount);
    for (int i = 0; i < splitCount; i++) {
      int len = in.readInt();
      if (len < 0) {
        throw new IOException("Invalid LanceSourceSplit payload length: " + len);
      }
      byte[] splitBytes = new byte[len];
      in.readFully(splitBytes);
      splits.add(splitSerializer.deserialize(splitVersion, splitBytes));
    }
    return new LanceContinuousEnumState(lastVer, known, splits, fp);
  }
}
