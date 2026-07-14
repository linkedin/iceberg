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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end SQL tests for delete file replication with Parquet.
 *
 * <p>The catalog warehouse lives on a dedicated {@code capturefs:} filesystem that records the
 * replication factor passed to every {@link FileSystem#create} call, which is what HDFS would
 * receive in production. The full write path is exercised through SQL only: {@code CREATE TABLE}
 * with merge-on-read Parquet, {@code INSERT}, {@code DELETE}, and the {@code delete_files} metadata
 * table for locating the produced position delete files.
 */
public class TestParquetDeleteFileReplication extends TestBase {

  private static final String CATALOG = "parqreplcat";
  private static final String TABLE = CATALOG + ".default.parquet_repl_test";

  @BeforeAll
  public static void setUpCatalog() throws IOException {
    // session confs are copied into sessionState().newHadoopConf(), which SparkCatalog uses
    spark.conf().set("fs.capturefs.impl", CaptureReplicationFileSystem.class.getName());
    spark.conf().set("fs.capturefs.impl.disable.cache", "true");

    String warehouse = Files.createTempDirectory("parquet-repl-warehouse").toString();
    spark.conf().set("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + CATALOG + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + CATALOG + ".warehouse", "capturefs:" + warehouse);
  }

  @AfterEach
  public void cleanUp() {
    spark.conf().unset(SparkSQLProperties.DELETE_FILE_REPLICATION);
    sql("DROP TABLE IF EXISTS %s", TABLE);
  }

  @Test
  public void testDeleteFileReplicationFromTableProperty() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='2', "
            + "'write.delete.mode'='merge-on-read', "
            + "'write.format.default'='parquet', "
            + "'write.delete-file-replication'='5')",
        TABLE);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", TABLE);
    sql("DELETE FROM %s WHERE id IN (1, 2)", TABLE);

    // the delete files must reach the filesystem with the configured replication factor
    List<Object[]> deleteFiles = sql("SELECT file_path FROM %s.delete_files", TABLE);
    assertThat(deleteFiles).isNotEmpty();
    for (Object[] row : deleteFiles) {
      String location = (String) row[0];
      assertThat(location).endsWith(".parquet");
      assertThat(CaptureReplicationFileSystem.capturedReplication(location))
          .as("Delete file %s should be created with the configured replication", location)
          .isEqualTo((short) 5);
    }

    // data files are not covered by the delete file replication setting
    List<Object[]> dataFiles = sql("SELECT file_path FROM %s.files WHERE content = 0", TABLE);
    assertThat(dataFiles).isNotEmpty();
    for (Object[] row : dataFiles) {
      String location = (String) row[0];
      assertThat(CaptureReplicationFileSystem.capturedReplication(location))
          .as("Data file %s should keep the filesystem default replication", location)
          .isNotEqualTo((short) 5);
    }

    // merge-on-read must still resolve the deletes on read
    assertThat(sql("SELECT id FROM %s ORDER BY id", TABLE))
        .extracting(row -> row[0])
        .containsExactly(3, 4, 5);
  }

  @Test
  public void testDeleteFileReplicationFromSqlSessionConf() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version'='2', "
            + "'write.delete.mode'='merge-on-read', "
            + "'write.format.default'='parquet')",
        TABLE);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", TABLE);

    // the key contains hyphens, so the Spark SQL parser requires backquotes
    sql("SET `%s`=7", SparkSQLProperties.DELETE_FILE_REPLICATION);
    sql("DELETE FROM %s WHERE id IN (1, 2)", TABLE);

    List<Object[]> deleteFiles = sql("SELECT file_path FROM %s.delete_files", TABLE);
    assertThat(deleteFiles).isNotEmpty();
    for (Object[] row : deleteFiles) {
      String location = (String) row[0];
      assertThat(location).endsWith(".parquet");
      assertThat(CaptureReplicationFileSystem.capturedReplication(location))
          .as("Delete file %s should be created with the session conf replication", location)
          .isEqualTo((short) 7);
    }

    assertThat(sql("SELECT id FROM %s ORDER BY id", TABLE))
        .extracting(row -> row[0])
        .containsExactly(3, 4, 5);
  }

  /**
   * Local filesystem under a dedicated {@code capturefs:} scheme that records the replication
   * factor passed to each stream creation.
   */
  public static class CaptureReplicationFileSystem extends RawLocalFileSystem {
    private static final Map<String, Short> CAPTURED = Maps.newConcurrentMap();

    static short capturedReplication(String location) {
      String name = new Path(location).getName();
      Short replication = CAPTURED.get(name);
      assertThat(replication).as("No stream was created for %s", location).isNotNull();
      return replication;
    }

    @Override
    public String getScheme() {
      return "capturefs";
    }

    @Override
    public URI getUri() {
      return URI.create("capturefs:///");
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
