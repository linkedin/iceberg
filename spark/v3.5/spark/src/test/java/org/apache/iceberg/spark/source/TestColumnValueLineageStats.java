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

import java.util.List;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestColumnValueLineageStats extends TestBaseWithCatalog {

  private static final String SESSION_FLAG = "spark.lineage.columnValues.enabled";

  @BeforeEach
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @AfterEach
  public void cleanup() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    spark.conf().unset(SESSION_FLAG);
  }

  private void createTable() {
    sql("CREATE TABLE %s (id BIGINT, datepartition STRING) USING iceberg", tableName);
    sql(
        "INSERT INTO %s VALUES (1, '2024-01-01-00'), (2, '2024-01-01-01'), (3, '2024-01-02-00')",
        tableName);
  }

  private Table table() {
    return validationCatalog.loadTable(tableIdent);
  }

  private Set<Integer> boundedColumnIds() {
    Table table = table();
    SparkScanBuilder scanBuilder =
        new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    SparkBatchQueryScan scan = (SparkBatchQueryScan) scanBuilder.build();

    Set<Integer> columnIds = Sets.newHashSet();
    List<PartitionScanTask> tasks = scan.tasks();
    for (PartitionScanTask task : tasks) {
      FileScanTask fileTask = (FileScanTask) task;
      if (fileTask.file().lowerBounds() != null) {
        columnIds.addAll(fileTask.file().lowerBounds().keySet());
      }
    }

    return columnIds;
  }

  private int fieldId(String column) {
    return table().schema().findField(column).fieldId();
  }

  @TestTemplate
  public void testTrackedColumnStatsRetainedByDefault() {
    createTable();
    assertThat(boundedColumnIds()).contains(fieldId("datepartition"));
  }

  @TestTemplate
  public void testNonTrackedColumnStatsDropped() {
    createTable();
    assertThat(boundedColumnIds()).doesNotContain(fieldId("id"));
  }

  @TestTemplate
  public void testSessionFlagDisablesRetention() {
    createTable();
    spark.conf().set(SESSION_FLAG, "false");
    assertThat(boundedColumnIds()).doesNotContain(fieldId("datepartition"));
  }

  @TestTemplate
  public void testTableKillSwitchDisablesRetention() {
    createTable();
    sql("ALTER TABLE %s SET TBLPROPERTIES ('lineage.columnValues.enabled' = 'false')", tableName);
    assertThat(boundedColumnIds()).doesNotContain(fieldId("datepartition"));
  }

  @TestTemplate
  public void testColumnsOverrideTracksRequestedColumns() {
    createTable();
    sql("ALTER TABLE %s SET TBLPROPERTIES ('lineage.columnValues.columns' = 'id')", tableName);
    Set<Integer> boundedIds = boundedColumnIds();
    assertThat(boundedIds).contains(fieldId("id"));
    assertThat(boundedIds).doesNotContain(fieldId("datepartition"));
  }

  @TestTemplate
  public void testMissingConfiguredColumnIsIgnored() {
    createTable();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('lineage.columnValues.columns' = 'datepartition,does_not_exist')",
        tableName);
    assertThat(boundedColumnIds()).contains(fieldId("datepartition"));
  }

  @TestTemplate
  public void testConfiguredColumnResolvesCaseInsensitively() {
    createTable();
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('lineage.columnValues.columns' = 'DATEPARTITION')",
        tableName);
    assertThat(boundedColumnIds()).contains(fieldId("datepartition"));
  }
}
