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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestSparkDataWrite3 extends TestSparkDataWrite {

  public TestSparkDataWrite3(String format) {
    super(format);
  }

  @Parameterized.AfterParam
  public static void clearSourceCache() {
    ManualSource.clearTables();
  }

  @Test
  public void testCommitUnknownException() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "commitunknown");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    AppendFiles append = table.newFastAppend();
    AppendFiles spyAppend = spy(append);
    doAnswer(invocation -> {
      append.commit();
      throw new CommitStateUnknownException(new RuntimeException("Datacenter on Fire"));
    }).when(spyAppend).commit();

    Table spyTable = spy(table);
    when(spyTable.newAppend()).thenReturn(spyAppend);
    SparkTable sparkTable = new SparkTable(spyTable, false);

    String manualTableName = "unknown_exception";
    ManualSource.setTable(manualTableName, sparkTable);

    // Although an exception is thrown here, write and commit have succeeded
    AssertHelpers.assertThrowsWithCause("Should throw a Commit State Unknown Exception",
        SparkException.class,
        "Writing job aborted",
        CommitStateUnknownException.class,
        "Datacenter on Fire",
        () -> df.select("id", "data").sort("data").write()
            .format("org.apache.iceberg.spark.source.ManualSource")
            .option(ManualSource.TABLE_NAME, manualTableName)
            .mode(SaveMode.Append)
            .save(location.toString()));

    // Since write and commit succeeded, the rows should be readable
    Dataset<Row> result = spark.read().format("iceberg").load(location.toString());
    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
    Assert.assertEquals("Number of rows should match", records.size(), actual.size());
    Assert.assertEquals("Result rows should match", records, actual);
  }

}
