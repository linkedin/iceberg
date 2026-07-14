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
package org.apache.iceberg.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestOrcReplication {

  private static final Schema SCHEMA = new Schema(optional(1, "id", Types.IntegerType.get()));
  private static final short CUSTOM_REPLICATION = 5;

  @TempDir private File tempDir;

  @Test
  public void testReplicationPropagatesThroughFileAppender() throws IOException {
    Configuration conf = captureConf();
    Path path = newPath("data-file.orc");
    OutputFile out = HadoopOutputFile.fromPath(path, conf, CUSTOM_REPLICATION);
    assertThat(((HadoopOutputFile) out).replication()).isEqualTo(CUSTOM_REPLICATION);

    try (FileAppender<Record> appender =
        ORC.write(out).schema(SCHEMA).createWriterFunc(GenericOrcWriter::buildWriter).build()) {
      appender.add(record(1));
    }

    assertThat(CaptureReplicationFileSystem.capturedReplication(path))
        .isEqualTo(CUSTOM_REPLICATION);

    // the file written through the OutputFileSystem path must still be valid ORC
    List<Record> rows = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        ORC.read(out.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .build()) {
      reader.forEach(rows::add);
    }
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getField("id")).isEqualTo(1);
  }

  @Test
  public void testReplicationPropagatesThroughPositionDeleteWriter() throws IOException {
    Configuration conf = captureConf();
    Path path = newPath("position-deletes.orc");
    OutputFile out = HadoopOutputFile.fromPath(path, conf, CUSTOM_REPLICATION);

    PositionDeleteWriter<Record> deleteWriter =
        ORC.writeDeletes(out)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .withSpec(PartitionSpec.unpartitioned())
            .buildPositionWriter();

    PositionDelete<Record> delete = PositionDelete.create();
    try (PositionDeleteWriter<Record> writer = deleteWriter) {
      writer.write(delete.set("file:/data/data-file.orc", 0L, null));
    }

    assertThat(deleteWriter.toDeleteFile().recordCount()).isEqualTo(1L);
    assertThat(CaptureReplicationFileSystem.capturedReplication(path))
        .isEqualTo(CUSTOM_REPLICATION);
  }

  @Test
  public void testFilesystemDefaultReplicationWhenNotConfigured() throws IOException {
    Configuration conf = captureConf();
    Path path = newPath("default-replication.orc");
    OutputFile out = HadoopOutputFile.fromPath(path, conf);

    try (FileAppender<Record> appender =
        ORC.write(out).schema(SCHEMA).createWriterFunc(GenericOrcWriter::buildWriter).build()) {
      appender.add(record(2));
    }

    short fsDefault = path.getFileSystem(conf).getDefaultReplication(path);
    assertThat(CaptureReplicationFileSystem.capturedReplication(path)).isEqualTo(fsDefault);
  }

  private Path newPath(String name) {
    return new Path(new File(tempDir, name).toURI());
  }

  private Configuration captureConf() {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", CaptureReplicationFileSystem.class, FileSystem.class);
    conf.setBoolean("fs.file.impl.disable.cache", true);
    return conf;
  }

  private Record record(int id) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    return record;
  }

  /** Local filesystem that records the replication factor passed to each stream creation. */
  public static class CaptureReplicationFileSystem extends RawLocalFileSystem {
    private static final Map<String, Short> CAPTURED = Maps.newConcurrentMap();

    static short capturedReplication(Path path) {
      Short replication = CAPTURED.get(path.getName());
      assertThat(replication).as("No stream was created for %s", path).isNotNull();
      return replication;
    }

    @Override
    public FSDataOutputStream create(
        Path path,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      CAPTURED.put(path.getName(), replication);
      return super.create(path, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(
        Path path,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      CAPTURED.put(path.getName(), replication);
      return super.create(
          path, permission, overwrite, bufferSize, replication, blockSize, progress);
    }
  }
}
