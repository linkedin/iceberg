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
import java.io.InputStream;
import java.util.function.Consumer;
import org.apache.orc.storage.common.io.DiskRangeList;

/**
 * Compile API for the binary-compatible ORC 1.8.2 nested-class overlay.
 *
 * <p>The patch compiler resolves inherited members through this class. The packaged overlay
 * resolves them through ORC's {@link InStream} at runtime.
 */
public abstract class InStream extends InputStream {
  protected Object name;
  protected long offset;
  protected long length;
  protected long position;
  protected DiskRangeList bytes;

  protected InStream(Object name, long offset, long length) {}

  protected void reset(DiskRangeList input) {}

  protected abstract void setCurrent(DiskRangeList newRange, boolean isJump);

  public abstract void changeIv(Consumer<byte[]> modifier);

  public abstract void seek(PositionProvider index) throws IOException;

  static int getRangeNumber(DiskRangeList list, DiskRangeList current) {
    return 0;
  }
}
