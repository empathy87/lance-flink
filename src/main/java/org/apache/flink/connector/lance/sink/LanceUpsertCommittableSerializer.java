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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer for Lance upsert committables. */
// TODO: Chunk large Arrow IPC payloads instead of relying on int-sized byte arrays.
public class LanceUpsertCommittableSerializer
    implements SimpleVersionedSerializer<LanceUpsertCommittable> {

  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(LanceUpsertCommittable committable) throws IOException {
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeLong(committable.committableId());
      out.writeInt(committable.subtaskId());
      out.writeUTF(committable.mode().name());
      out.writeLong(committable.rowCount());

      byte[] payload = committable.arrowIpcBytes();
      out.writeInt(payload.length);
      out.write(payload);

      return bytes.toByteArray();
    }
  }

  @Override
  public LanceUpsertCommittable deserialize(int version, byte[] serialized) throws IOException {
    if (version != VERSION) {
      throw new IOException("Unsupported LanceUpsertCommittable version: " + version);
    }

    try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(bytes)) {
      long committableId = in.readLong();
      int subtaskId = in.readInt();
      LanceUpsertCommittable.Mode mode = LanceUpsertCommittable.Mode.valueOf(in.readUTF());
      long rowCount = in.readLong();
      if (rowCount < 0) {
        throw new IOException("Invalid row count: " + rowCount);
      }

      int payloadSize = in.readInt();
      if (payloadSize < 0) {
        throw new IOException("Invalid Arrow IPC payload size: " + payloadSize);
      }

      byte[] payload = new byte[payloadSize];
      in.readFully(payload);

      return new LanceUpsertCommittable(committableId, subtaskId, mode, payload, rowCount);
    }
  }
}
