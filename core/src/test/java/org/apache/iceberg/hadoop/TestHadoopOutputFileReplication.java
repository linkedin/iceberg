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
package org.apache.iceberg.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestHadoopOutputFileReplication {

  @TempDir private File tempDir;
  private Configuration conf;
  private FileSystem fs;

  @BeforeEach
  public void before() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
  }

  @Test
  public void testOutputFileWithDefaultReplication() throws IOException {
    Path testPath = new Path(tempDir.toURI().toString(), "test-default-replication.txt");
    OutputFile outputFile = HadoopOutputFile.fromPath(testPath, fs, conf);

    try (PositionOutputStream stream = outputFile.create()) {
      stream.write("test data".getBytes());
    }

    assertThat(fs.exists(testPath)).isTrue();
    assertThat(fs.getFileStatus(testPath).getLen()).isGreaterThan(0);
  }

  @Test
  public void testOutputFileWithCustomReplication() throws IOException {
    Path testPath = new Path(tempDir.toURI().toString(), "test-custom-replication.txt");
    short replicationFactor = 2;
    OutputFile outputFile = HadoopOutputFile.fromPath(testPath, conf, replicationFactor);

    try (PositionOutputStream stream = outputFile.create()) {
      stream.write("test data with custom replication".getBytes());
    }

    assertThat(fs.exists(testPath)).isTrue();
    assertThat(fs.getFileStatus(testPath).getLen()).isGreaterThan(0);
    // Note: Local filesystem doesn't support replication, but the API should work
  }

  @Test
  public void testOutputFileWithZeroReplication() throws IOException {
    Path testPath = new Path(tempDir.toURI().toString(), "test-zero-replication.txt");
    short replicationFactor = 0;
    OutputFile outputFile = HadoopOutputFile.fromPath(testPath, conf, replicationFactor);

    try (PositionOutputStream stream = outputFile.create()) {
      stream.write("test data with zero replication".getBytes());
    }

    assertThat(fs.exists(testPath)).isTrue();
  }

  @Test
  public void testOutputFileWithNegativeReplication() throws IOException {
    Path testPath = new Path(tempDir.toURI().toString(), "test-negative-replication.txt");
    short replicationFactor = -1;
    OutputFile outputFile = HadoopOutputFile.fromPath(testPath, conf, replicationFactor);

    try (PositionOutputStream stream = outputFile.create()) {
      stream.write("test data with negative replication".getBytes());
    }

    assertThat(fs.exists(testPath)).isTrue();
  }

  @Test
  public void testCreateOrOverwriteWithCustomReplication() throws IOException {
    Path testPath = new Path(tempDir.toURI().toString(), "test-overwrite-replication.txt");
    short replicationFactor = 3;
    OutputFile outputFile = HadoopOutputFile.fromPath(testPath, conf, replicationFactor);

    // Create initial file
    try (PositionOutputStream stream = outputFile.createOrOverwrite()) {
      stream.write("initial data".getBytes());
    }

    assertThat(fs.exists(testPath)).isTrue();
    long firstSize = fs.getFileStatus(testPath).getLen();

    // Overwrite the file
    try (PositionOutputStream stream = outputFile.createOrOverwrite()) {
      stream.write("overwritten data with more content".getBytes());
    }

    assertThat(fs.exists(testPath)).isTrue();
    long secondSize = fs.getFileStatus(testPath).getLen();
    assertThat(secondSize).isGreaterThan(firstSize);
  }

  @Test
  public void testFileIONewOutputFileWithReplication() throws IOException {
    HadoopFileIO fileIO = new HadoopFileIO(conf);
    String location =
        new Path(tempDir.toURI().toString(), "test-fileio-replication.txt").toString();
    short replicationFactor = 2;

    OutputFile outputFile =
        fileIO.newOutputFile(
            location,
            ImmutableMap.of(
                OutputFileFactory.FILE_REPLICATION_FACTOR, String.valueOf(replicationFactor)));

    try (PositionOutputStream stream = outputFile.create()) {
      stream.write("test data from FileIO".getBytes());
    }

    assertThat(fileIO.newInputFile(location).exists()).isTrue();
  }

  @Test
  public void testFileIONewOutputFileWithoutReplication() throws IOException {
    HadoopFileIO fileIO = new HadoopFileIO(conf);
    String location =
        new Path(tempDir.toURI().toString(), "test-fileio-no-replication.txt").toString();

    // Use default method without replication
    OutputFile outputFile = fileIO.newOutputFile(location);

    try (PositionOutputStream stream = outputFile.create()) {
      stream.write("test data without explicit replication".getBytes());
    }

    assertThat(fileIO.newInputFile(location).exists()).isTrue();
  }

  @Test
  public void testOutputFileWithInvalidReplication() throws IOException {
    Path testPath = new Path(tempDir.toURI().toString(), "test-invalid-replication.txt");
    String invalidReplication = "invalid";
    OutputFile outputFile =
        HadoopOutputFile.fromPath(
            testPath,
            conf,
            ImmutableMap.of(OutputFileFactory.FILE_REPLICATION_FACTOR, invalidReplication));

    try (PositionOutputStream stream = outputFile.create()) {
      stream.write("test data".getBytes());
    }

    assertThat(fs.exists(testPath)).isTrue();
  }
}
