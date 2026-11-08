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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
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

public class TestParquetReplication {

  private static final Schema SCHEMA = new Schema(optional(1, "id", Types.IntegerType.get()));
  private static final short CUSTOM_REPLICATION = 5;

  @TempDir private File tempDir;

  @Test
  public void testHadoopShortcutUsedOnlyWithoutCustomReplication() {
    Configuration conf = new Configuration();
    Path path = newPath("routing.parquet");

    OutputFile withReplication = HadoopOutputFile.fromPath(path, conf, CUSTOM_REPLICATION);
    assertThat(((HadoopOutputFile) withReplication).replication()).isEqualTo(CUSTOM_REPLICATION);
    assertThat(ParquetIO.file(withReplication))
        .isNotInstanceOf(org.apache.parquet.hadoop.util.HadoopOutputFile.class);
    assertThat(ParquetIO.file(withReplication, conf))
        .isNotInstanceOf(org.apache.parquet.hadoop.util.HadoopOutputFile.class);

    OutputFile withoutReplication = HadoopOutputFile.fromPath(path, conf);
    assertThat(ParquetIO.file(withoutReplication))
        .isInstanceOf(org.apache.parquet.hadoop.util.HadoopOutputFile.class);
    assertThat(ParquetIO.file(withoutReplication, conf))
        .isInstanceOf(org.apache.parquet.hadoop.util.HadoopOutputFile.class);
  }

  @Test
  public void testReplicationPropagatesThroughDefaultAppender() throws IOException {
    Configuration conf = captureConf();
    Path path = newPath("default-appender.parquet");
    OutputFile out = HadoopOutputFile.fromPath(path, conf, CUSTOM_REPLICATION);

    try (FileAppender<GenericData.Record> appender =
        Parquet.write(out).schema(SCHEMA).named("test").build()) {
      appender.add(record(1));
    }

    assertThat(CaptureReplicationFileSystem.capturedReplication(path))
        .isEqualTo(CUSTOM_REPLICATION);

    // the file written through the stream-based path must still be valid parquet
    List<GenericData.Record> rows = Lists.newArrayList();
    try (CloseableIterable<GenericData.Record> reader =
        Parquet.read(out.toInputFile()).project(SCHEMA).build()) {
      reader.forEach(rows::add);
    }
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
  }

  @Test
  public void testReplicationPropagatesThroughIcebergParquetWriter() throws IOException {
    Configuration conf = captureConf();
    Path path = newPath("iceberg-writer.parquet");
    OutputFile out = HadoopOutputFile.fromPath(path, conf, CUSTOM_REPLICATION);

    try (FileAppender<GenericData.Record> appender =
        Parquet.write(out)
            .schema(SCHEMA)
            .named("test")
            .createWriterFunc(ParquetAvroWriter::buildWriter)
            .build()) {
      appender.add(record(2));
    }

    assertThat(CaptureReplicationFileSystem.capturedReplication(path))
        .isEqualTo(CUSTOM_REPLICATION);
  }

  @Test
  public void testReplicationPropagatesThroughPositionDeleteWriter() throws IOException {
    Configuration conf = captureConf();
    Path path = newPath("position-deletes.parquet");
    OutputFile out = HadoopOutputFile.fromPath(path, conf, CUSTOM_REPLICATION);

    PositionDeleteWriter<Record> deleteWriter =
        Parquet.writeDeletes(out)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .withSpec(PartitionSpec.unpartitioned())
            .buildPositionWriter();

    PositionDelete<Record> delete = PositionDelete.create();
    try (PositionDeleteWriter<Record> writer = deleteWriter) {
      writer.write(delete.set("file:/data/data-file.parquet", 0L, null));
    }

    assertThat(deleteWriter.toDeleteFile().recordCount()).isEqualTo(1L);
    assertThat(CaptureReplicationFileSystem.capturedReplication(path))
        .isEqualTo(CUSTOM_REPLICATION);
  }

  @Test
  public void testFilesystemDefaultReplicationWhenNotConfigured() throws IOException {
    Configuration conf = captureConf();
    Path path = newPath("default-replication.parquet");
    OutputFile out = HadoopOutputFile.fromPath(path, conf);

    try (FileAppender<GenericData.Record> appender =
        Parquet.write(out).schema(SCHEMA).named("test").build()) {
      appender.add(record(3));
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

  private GenericData.Record record(int id) {
    return new GenericRecordBuilder(AvroSchemaUtil.convert(SCHEMA, "test")).set("id", id).build();
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
