/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.orc.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.apache.orc.storage.common.io.DiskRangeList;

public class InStream$UncompressedStream extends InStream {
  protected ByteBuffer decrypted;
  protected DiskRangeList currentRange;
  protected long currentOffset;

  public InStream$UncompressedStream(Object name, long offset, long length) {
    super(name, offset, length);
  }

  public InStream$UncompressedStream(Object name, DiskRangeList input, long offset, long length) {
    super(name, offset, length);
    reset(input);
  }

  @Override
  public int read() {
    if (decrypted == null || decrypted.remaining() == 0) {
      if (position == length) {
        return -1;
      }
      setCurrent(currentRange.next, false);
    }
    position += 1;
    return 0xff & decrypted.get();
  }

  @Override
  protected void setCurrent(DiskRangeList newRange, boolean isJump) {
    currentRange = newRange;
    if (newRange != null) {
      decrypted = newRange.getData().slice();
      currentOffset = newRange.getOffset();
      int start = (int) (position + offset - currentOffset);
      decrypted.position(start);
      decrypted.limit(start + (int) Math.min(decrypted.remaining(), length - position));
    }
  }

  @Override
  public int read(byte[] data, int offset, int length) {
    if (decrypted == null || decrypted.remaining() == 0) {
      if (position == this.length) {
        return -1;
      }
      setCurrent(currentRange.next, false);
    }
    int actualLength = Math.min(length, decrypted.remaining());
    decrypted.get(data, offset, actualLength);
    position += actualLength;
    return actualLength;
  }

  @Override
  public int available() {
    if (decrypted != null && decrypted.remaining() > 0) {
      return decrypted.remaining();
    }
    return (int) (length - position);
  }

  @Override
  public void close() {
    currentRange = null;
    position = length;
    decrypted = null;
    bytes = null;
  }

  @Override
  public void changeIv(Consumer<byte[]> modifier) {}

  @Override
  public void seek(PositionProvider index) throws IOException {
    seek(index.getNext());
  }

  public void seek(long desired) throws IOException {
    if (desired == 0 && bytes == null) {
      return;
    }

    long positionFile = desired + offset;
    if (currentRange != null
        && positionFile >= currentRange.getOffset()
        && positionFile < currentRange.getEnd()) {
      decrypted.position((int) (positionFile - currentOffset));
      position = desired;
    } else {
      for (DiskRangeList currentRange = bytes;
          currentRange != null;
          currentRange = currentRange.next) {
        boolean isLogicalEnd = desired == length && positionFile == currentRange.getEnd();
        if (currentRange.getOffset() <= positionFile
            && (isLogicalEnd
                || (currentRange.next == null
                    ? positionFile <= currentRange.getEnd()
                    : positionFile < currentRange.getEnd()))) {
          position = desired;
          setCurrent(currentRange, true);
          return;
        }
      }
      throw new IllegalArgumentException(
          "Seek in " + name + " to " + desired + " is outside of the data");
    }
  }

  @Override
  public String toString() {
    return "uncompressed stream "
        + name
        + " position: "
        + position
        + " length: "
        + length
        + " range: "
        + InStream.getRangeNumber(bytes, currentRange)
        + " offset: "
        + currentRange.getOffset()
        + " position: "
        + (decrypted == null ? 0 : decrypted.position())
        + " limit: "
        + (decrypted == null ? 0 : decrypted.limit());
  }
}
