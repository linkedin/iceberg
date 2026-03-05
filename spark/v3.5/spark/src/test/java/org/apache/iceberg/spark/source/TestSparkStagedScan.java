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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkStagedScan extends CatalogTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testTaskSetLoading() throws NoSuchTableException, IOException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).as("Should produce 1 snapshot").hasSize(1);

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
      String setID = UUID.randomUUID().toString();
      taskSetManager.stageTasks(table, setID, ImmutableList.copyOf(fileScanTasks));

      // load the staged file set
      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, setID)
              .load(tableName);

      // write the records back essentially duplicating data
      scanDF.writeTo(tableName).append();
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "a"), row(1, "a"), row(2, "b"), row(2, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testTaskSetPlanning() throws NoSuchTableException, IOException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).as("Should produce 2 snapshot").hasSize(2);

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
      String setID = UUID.randomUUID().toString();
      List<FileScanTask> tasks = ImmutableList.copyOf(fileScanTasks);
      taskSetManager.stageTasks(table, setID, tasks);

      // load the staged file set and make sure each file is in a separate split
      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, setID)
              .option(SparkReadOptions.SPLIT_SIZE, tasks.get(0).file().fileSizeInBytes())
              .load(tableName);
      assertThat(scanDF.javaRDD().getNumPartitions())
          .as("Num partitions should match")
          .isEqualTo(2);

      // load the staged file set and make sure we combine both files into a single split
      scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, setID)
              .option(SparkReadOptions.SPLIT_SIZE, Long.MAX_VALUE)
              .load(tableName);
      assertThat(scanDF.javaRDD().getNumPartitions())
          .as("Num partitions should match")
          .isEqualTo(1);
    }
  }

  @TestTemplate
  public void testDataOnlyWeightTaskGroupPlanning() throws NoSuchTableException, IOException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.coalesce(1).writeTo(tableName).append();
    df.coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);

    // get data files to write position deletes against them
    List<DataFile> dataFiles = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      for (FileScanTask task : fileScanTasks) {
        dataFiles.add(task.file());
      }
    }

    assertThat(dataFiles).as("Should have 2 data files").hasSize(2);

    // write position deletes for each data file to inflate sizeBytes()
    for (DataFile dataFile : dataFiles) {
      List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
      deletes.add(Pair.of(dataFile.path(), 0L));
      Pair<DeleteFile, ?> result =
          FileHelpers.writeDeleteFile(
              table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes);
      table.newRowDelta().addDeletes(result.first()).commit();
    }

    table.refresh();

    // scan with deletes to get inflated sizeBytes
    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
      String setID = UUID.randomUUID().toString();
      List<FileScanTask> tasks = ImmutableList.copyOf(fileScanTasks);
      taskSetManager.stageTasks(table, setID, tasks);

      long dataOnlySize = tasks.stream().mapToLong(FileScanTask::length).sum();
      long totalSizeBytes = tasks.stream().mapToLong(FileScanTask::sizeBytes).sum();
      assertThat(totalSizeBytes)
          .as("sizeBytes should be larger than data-only length due to delete files")
          .isGreaterThan(dataOnlySize);

      // with data-only weight: both files should fit in one partition
      // because their data-only sizes sum to exactly dataOnlySize
      Dataset<Row> dataOnlyDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, setID)
              .option(SparkReadOptions.SPLIT_SIZE, dataOnlySize)
              .option(SparkReadOptions.FILE_OPEN_COST, "0")
              .option(SparkReadOptions.USE_DATA_ONLY_WEIGHT, "true")
              .load(tableName);
      assertThat(dataOnlyDF.javaRDD().getNumPartitions())
          .as("Data-only weight should pack both files into 1 partition")
          .isEqualTo(1);

      // without data-only weight: inflated sizeBytes should cause 2 partitions
      // because the sizeBytes of both files together exceeds dataOnlySize
      Dataset<Row> defaultDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, setID)
              .option(SparkReadOptions.SPLIT_SIZE, dataOnlySize)
              .option(SparkReadOptions.FILE_OPEN_COST, "0")
              .load(tableName);
      assertThat(defaultDF.javaRDD().getNumPartitions())
          .as("Default weight (with deletes) should produce more partitions")
          .isGreaterThan(1);
    }
  }
}
