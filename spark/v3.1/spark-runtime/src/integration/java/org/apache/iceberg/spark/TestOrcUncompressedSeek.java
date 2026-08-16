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
package org.apache.iceberg.spark;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.shaded.org.apache.orc.CompressionKind;
import org.apache.iceberg.shaded.org.apache.orc.OrcFile;
import org.apache.iceberg.shaded.org.apache.orc.Reader;
import org.apache.iceberg.shaded.org.apache.orc.TypeDescription;
import org.apache.iceberg.shaded.org.apache.orc.Writer;
import org.apache.iceberg.shaded.org.apache.orc.impl.BufferChunk;
import org.apache.iceberg.shaded.org.apache.orc.impl.InStream;
import org.apache.iceberg.shaded.org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.iceberg.shaded.org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Test;

public class TestOrcUncompressedSeek {

  @Test
  public void readOrcFileThroughShadedRuntime() throws Exception {
    File orcFile =
        Files.createTempDirectory("iceberg-orc-uncompressed-seek").resolve("test.orc").toFile();
    orcFile.deleteOnExit();
    Configuration conf = new Configuration();
    Path path = new Path(orcFile.toURI());
    TypeDescription schema = TypeDescription.fromString("struct<id:bigint>");

    try (Writer writer =
        OrcFile.createWriter(
            path, OrcFile.writerOptions(conf).setSchema(schema).compress(CompressionKind.NONE))) {
      VectorizedRowBatch batch = schema.createRowBatch();
      ((LongColumnVector) batch.cols[0]).vector[0] = 34;
      batch.size = 1;
      writer.addRowBatch(batch);
    }

    try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf))) {
      Assert.assertEquals(1, reader.getNumberOfRows());
    }
  }

  @Test
  public void seekToLogicalEndAtNonFinalRangeBoundary() throws Exception {
    BufferChunk streamData = new BufferChunk(ByteBuffer.allocate(24), 0);
    streamData.next = new BufferChunk(ByteBuffer.allocate(8), 32);
    InStream.UncompressedStream stream =
        (InStream.UncompressedStream) InStream.create("test", streamData, 0, 24);

    stream.seek(24);

    Assert.assertEquals(0, stream.available());
    Assert.assertEquals(-1, stream.read());
    Assert.assertThrows(IllegalArgumentException.class, () -> stream.seek(25));
  }
}
