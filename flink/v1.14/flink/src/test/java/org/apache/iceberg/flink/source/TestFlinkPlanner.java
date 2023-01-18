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
package org.apache.iceberg.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.util.ThreadPools;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkPlanner {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  protected HadoopCatalog catalog;
  protected String warehouse;
  protected String location;
  FileFormat fileFormat = FileFormat.valueOf(FileFormat.ORC.toString());

  @Before
  public void before() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
    Configuration conf = new Configuration();
    catalog = new HadoopCatalog(conf, warehouse);
    location = String.format("%s/%s/%s", warehouse, TestFixtures.DATABASE, TestFixtures.TABLE);
  }

  @After
  public void after() throws IOException {}

  /** Test PLAN_SINGLE_WHOLE_FILE_PER_TASK configuration */
  @Test
  public void testFlinkPlannerSingleFilePerTask() throws Exception {
    Table table =
        catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER)
        .appendToTable(org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), expectedRecords);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER)
        .appendToTable(org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), expectedRecords);

    ScanContext scanContext = ScanContext.builder().planParallelism(2).build();
    ExecutorService workerPool = ThreadPools.newWorkerPool("test", scanContext.planParallelism());
    List<IcebergSourceSplit> splits =
        FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext, workerPool);
    Assert.assertEquals("Expected 1 split but scan returned %d splits", 1, splits.size());
    Assert.assertEquals(
        "Expected 2 FileScanTask in the split but has %d instead",
        2, splits.get(0).task().tasks().size());

    // Enable planning single file per task
    scanContext = ScanContext.builder().planParallelism(2).planSingleWholeFilePerTask(true).build();
    splits = FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext, workerPool);
    // There should be 2 splits with 1 FileScanTask each
    Assert.assertEquals("Expected 2 splits but scan returned %d splits", 2, splits.size());
    splits.forEach(
        split ->
            Assert.assertEquals(
                "Expected a single file task in the split but task has %d tasks",
                1, split.task().tasks().size()));
  }
}
