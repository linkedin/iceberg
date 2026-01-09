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
package org.apache.iceberg.io;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for OutputFileFactory's replication factor support.
 *
 * <p>Note: These tests verify that the builder pattern accepts replication factor parameters and
 * constructs factories correctly. The actual file creation with replication is tested at the
 * HadoopOutputFile level.
 */
public class TestOutputFileFactoryReplication {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;

  @Test
  public void testBuilderWithReplicationFactor() {
    Configuration conf = new Configuration();
    HadoopFileIO fileIO = new HadoopFileIO(conf);
    PartitionSpec spec = PartitionSpec.unpartitioned();

    short replicationFactor = 3;
    int partitionId = 1;
    long taskId = 100L;

    // Verify that the builder accepts replication factor parameter
    org.apache.iceberg.hadoop.HadoopTables tables =
        new org.apache.iceberg.hadoop.HadoopTables(conf);
    String location = tempDir.getAbsolutePath() + "/test_table";
    org.apache.iceberg.Table table = tables.create(SCHEMA, spec, location);

    OutputFileFactory.Builder builder =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(FileFormat.PARQUET)
            .replicationFactor(replicationFactor);

    assertThat(builder).isNotNull();

    OutputFileFactory factory = builder.build();
    assertThat(factory).isNotNull();
  }

  @Test
  public void testBuilderWithoutReplicationFactor() {
    Configuration conf = new Configuration();
    PartitionSpec spec = PartitionSpec.unpartitioned();
    int partitionId = 1;
    long taskId = 100L;

    // Verify that the builder works without replication factor (default)
    org.apache.iceberg.hadoop.HadoopTables tables =
        new org.apache.iceberg.hadoop.HadoopTables(conf);
    String location = tempDir.getAbsolutePath() + "/test_table2";
    org.apache.iceberg.Table table = tables.create(SCHEMA, spec, location);

    OutputFileFactory.Builder builder =
        OutputFileFactory.builderFor(table, partitionId, taskId).format(FileFormat.PARQUET);

    assertThat(builder).isNotNull();

    OutputFileFactory factory = builder.build();
    assertThat(factory).isNotNull();
  }

  @Test
  public void testBuilderWithDifferentReplicationFactors() {
    Configuration conf = new Configuration();
    PartitionSpec spec = PartitionSpec.unpartitioned();
    int partitionId = 1;
    long taskId = 100L;

    org.apache.iceberg.hadoop.HadoopTables tables =
        new org.apache.iceberg.hadoop.HadoopTables(conf);
    String location = tempDir.getAbsolutePath() + "/test_table3";
    org.apache.iceberg.Table table = tables.create(SCHEMA, spec, location);

    // Test with replication factor 1
    OutputFileFactory factory1 =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(FileFormat.PARQUET)
            .replicationFactor((short) 1)
            .build();
    assertThat(factory1).isNotNull();

    // Test with replication factor 5
    OutputFileFactory factory2 =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(FileFormat.PARQUET)
            .replicationFactor((short) 5)
            .build();
    assertThat(factory2).isNotNull();

    // Test with maximum replication factor
    OutputFileFactory factory3 =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(FileFormat.PARQUET)
            .replicationFactor(Short.MAX_VALUE)
            .build();
    assertThat(factory3).isNotNull();
  }

  @Test
  public void testBuilderWithReplicationFactorAndSuffix() {
    Configuration conf = new Configuration();
    PartitionSpec spec = PartitionSpec.unpartitioned();
    short replicationFactor = 3;
    int partitionId = 1;
    long taskId = 100L;

    org.apache.iceberg.hadoop.HadoopTables tables =
        new org.apache.iceberg.hadoop.HadoopTables(conf);
    String location = tempDir.getAbsolutePath() + "/test_table4";
    org.apache.iceberg.Table table = tables.create(SCHEMA, spec, location);

    // Verify that replication factor works with suffix option
    OutputFileFactory factory =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(FileFormat.PARQUET)
            .replicationFactor(replicationFactor)
            .suffix("deletes")
            .build();

    assertThat(factory).isNotNull();
  }

  @Test
  public void testBuilderWithAllOptions() {
    Configuration conf = new Configuration();
    PartitionSpec spec = PartitionSpec.unpartitioned();
    short replicationFactor = 2;
    int partitionId = 1;
    long taskId = 100L;

    org.apache.iceberg.hadoop.HadoopTables tables =
        new org.apache.iceberg.hadoop.HadoopTables(conf);
    String location = tempDir.getAbsolutePath() + "/test_table5";
    org.apache.iceberg.Table table = tables.create(SCHEMA, spec, location);

    // Test builder with all options including replication factor
    OutputFileFactory factory =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(FileFormat.AVRO)
            .replicationFactor(replicationFactor)
            .operationId("test-operation-id")
            .suffix("test-suffix")
            .build();

    assertThat(factory).isNotNull();
  }
}
