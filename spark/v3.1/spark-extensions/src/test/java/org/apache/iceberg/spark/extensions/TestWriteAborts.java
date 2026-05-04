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
package org.apache.iceberg.spark.extensions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

public class TestWriteAborts extends SparkExtensionsTestBase {

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            CatalogProperties.FILE_IO_IMPL,
            CustomFileIO.class.getName(),
            "default-namespace",
            "default")
      },
      {
        "testhivebulk",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            CatalogProperties.FILE_IO_IMPL,
            CustomBulkFileIO.class.getName(),
            "default-namespace",
            "default")
      }
    };
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public TestWriteAborts(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void resetCustomIOState() {
    CustomFileIO.deleteAttempts.set(0);
    CustomFileIO.failDeletes = false;
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    CustomFileIO.failDeletes = false;
  }

  @Test
  public void testBatchAppend() throws Exception {
    String dataLocation = temp.newFolder().toString();

    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data)"
            + "TBLPROPERTIES ('%s' '%s')",
        tableName, TableProperties.WRITE_DATA_LOCATION, dataLocation);

    List<SimpleRecord> records =
        ImmutableList.of(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "a"),
            new SimpleRecord(4, "b"));
    Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);

    AssertHelpers.assertThrows(
        "Write must fail",
        SparkException.class,
        "Writing job aborted",
        () -> {
          try {
            // incoming records are not ordered by partitions so the job must fail
            inputDF.coalesce(1).sortWithinPartitions("id").writeTo(tableName).append();
          } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
          }
        });

    assertEquals("Should be no records", sql("SELECT * FROM %s", tableName), ImmutableList.of());

    assertEquals(
        "Should be no orphan data files",
        ImmutableList.of(),
        sql(
            "CALL %s.system.remove_orphan_files(table => '%s', older_than => %dL, location => '%s')",
            catalogName, tableName, System.currentTimeMillis() + 5000, dataLocation));
  }

  @Test
  public void testAbortRetriesByDefault() throws Exception {
    // Bulk path doesn't retry per file (a single deleteFiles call), so this assertion is only
    // meaningful for the non-bulk FileIO.
    Assume.assumeFalse(catalogName.equals("testhivebulk"));

    String dataLocation = temp.newFolder().toString();
    CustomFileIO.failDeletes = true;

    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data)"
            + "TBLPROPERTIES ('%s' '%s')",
        tableName, TableProperties.WRITE_DATA_LOCATION, dataLocation);

    triggerFailingAppend();

    // With retry enabled (default), each path should be attempted retry(3)+1 = 4 times.
    Assert.assertTrue(
        "Expected at least 4 delete attempts when retry is enabled by default, but got "
            + CustomFileIO.deleteAttempts.get(),
        CustomFileIO.deleteAttempts.get() >= 4);
  }

  @Test
  public void testAbortRetryDisabledByTableProperty() throws Exception {
    // Bulk path is not affected by the retry flag.
    Assume.assumeFalse(catalogName.equals("testhivebulk"));

    String dataLocation = temp.newFolder().toString();
    CustomFileIO.failDeletes = true;

    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data)"
            + "TBLPROPERTIES ('%s' '%s', '%s' '%s')",
        tableName,
        TableProperties.WRITE_DATA_LOCATION,
        dataLocation,
        TableProperties.SPARK_WRITE_ABORT_RETRY_ENABLED,
        "false");

    triggerFailingAppend();

    // With retry disabled, each path should be attempted exactly once. We don't know the exact
    // number of files written before the task failed, but it should be small (coalesce(1) on
    // 4 input rows). The default retry would produce >= 4 attempts per file.
    int attempts = CustomFileIO.deleteAttempts.get();
    Assert.assertTrue(
        "Expected at least one delete attempt, got " + attempts, attempts >= 1);
    Assert.assertTrue(
        "Expected fewer than 4 delete attempts when retry is disabled, but got " + attempts,
        attempts < 4);
  }

  @Test
  public void testAbortSuppressFailureDisabledByTableProperty() throws Exception {
    String dataLocation = temp.newFolder().toString();
    CustomFileIO.failDeletes = true;

    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data)"
            + "TBLPROPERTIES ('%s' '%s', '%s' '%s', '%s' '%s')",
        tableName,
        TableProperties.WRITE_DATA_LOCATION,
        dataLocation,
        TableProperties.SPARK_WRITE_ABORT_SUPPRESS_FAILURE_ENABLED,
        "false",
        // Disable retries to keep the failure surface small; the suppress-vs-throw decision is
        // independent of retry behavior.
        TableProperties.SPARK_WRITE_ABORT_RETRY_ENABLED,
        "false");

    List<SimpleRecord> records =
        ImmutableList.of(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "a"),
            new SimpleRecord(4, "b"));
    Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);

    // With suppress disabled, the simulated FileIO failure should surface in the exception chain
    // rather than being silently swallowed by the cleanup utility.
    Assertions.assertThatThrownBy(
            () -> {
              try {
                inputDF.coalesce(1).sortWithinPartitions("id").writeTo(tableName).append();
              } catch (NoSuchTableException e) {
                throw new RuntimeException(e);
              }
            })
        .hasStackTraceContaining("simulated FileIO delete failure");
  }

  private void triggerFailingAppend() {
    List<SimpleRecord> records =
        ImmutableList.of(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "a"),
            new SimpleRecord(4, "b"));
    Dataset<Row> inputDF = spark.createDataFrame(records, SimpleRecord.class);

    AssertHelpers.assertThrows(
        "Write must fail",
        SparkException.class,
        "Writing job aborted",
        () -> {
          try {
            // incoming records are not ordered by partitions so the job must fail
            inputDF.coalesce(1).sortWithinPartitions("id").writeTo(tableName).append();
          } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
          }
        });
  }

  public static class CustomFileIO implements FileIO {

    static final AtomicInteger deleteAttempts = new AtomicInteger(0);
    static volatile boolean failDeletes = false;

    private final FileIO delegate = new HadoopFileIO(new Configuration());

    public CustomFileIO() {}

    protected FileIO delegate() {
      return delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
      return delegate.newInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      deleteAttempts.incrementAndGet();
      if (failDeletes) {
        throw new RuntimeException("simulated FileIO delete failure");
      }
      delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
      delegate.initialize(properties);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  public static class CustomBulkFileIO extends CustomFileIO implements SupportsBulkOperations {

    public CustomBulkFileIO() {}

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException("Only bulk deletes are supported");
    }

    @Override
    public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
      int count = 0;
      for (String path : paths) {
        count++;
        deleteAttempts.incrementAndGet();
        if (!failDeletes) {
          delegate().deleteFile(path);
        }
      }
      if (failDeletes) {
        BulkDeletionFailureException ex = new BulkDeletionFailureException(count);
        ex.initCause(new RuntimeException("simulated FileIO delete failure"));
        throw ex;
      }
    }
  }
}
