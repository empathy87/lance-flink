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

/** Versioned binary serializer for {@link LanceSourceSplit}. */
public class LanceSourceSplitSerializer implements SimpleVersionedSerializer<LanceSourceSplit> {

  public static final LanceSourceSplitSerializer INSTANCE = new LanceSourceSplitSerializer();

  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(LanceSourceSplit split) throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(32);
    out.writeLong(split.datasetVersion());
    out.writeInt(split.fragmentId());
    out.writeLong(split.recordsToSkip());
    return out.getCopyOfBuffer();
  }

  @Override
  public LanceSourceSplit deserialize(int version, byte[] bytes) throws IOException {
    if (version != VERSION) {
      throw new IOException("Unknown LanceSourceSplit version: " + version);
    }
    DataInputDeserializer in = new DataInputDeserializer(bytes);
    long datasetVersion = in.readLong();
    int fragmentId = in.readInt();
    long recordsToSkip = in.readLong();
    return new LanceSourceSplit(datasetVersion, fragmentId, recordsToSkip);
  }
}
