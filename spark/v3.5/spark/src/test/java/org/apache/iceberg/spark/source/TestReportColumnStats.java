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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestReportColumnStats extends TestBaseWithCatalog {

  @BeforeEach
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @AfterEach
  public void cleanup() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    spark.conf().unset("spark.sql.caseSensitive");
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

  private Set<Integer> boundedColumnIds(Collection<String> requestedColumns) {
    Table table = table();
    SparkScanBuilder scanBuilder =
        new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    if (requestedColumns != null) {
      scanBuilder.includeColumnStats(requestedColumns);
    }
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
  public void testNoColumnsRequestedRetainsNoStats() {
    createTable();
    assertThat(boundedColumnIds(null)).doesNotContain(fieldId("datepartition"));
  }

  @TestTemplate
  public void testEmptyRequestRetainsNoStats() {
    createTable();
    assertThat(boundedColumnIds(ImmutableList.of())).doesNotContain(fieldId("datepartition"));
  }

  @TestTemplate
  public void testRequestedColumnStatsRetained() {
    createTable();
    Set<Integer> boundedIds = boundedColumnIds(ImmutableList.of("datepartition"));
    assertThat(boundedIds).contains(fieldId("datepartition"));
    assertThat(boundedIds).doesNotContain(fieldId("id"));
  }

  @TestTemplate
  public void testUnknownColumnIsIgnored() {
    createTable();
    assertThat(boundedColumnIds(ImmutableList.of("datepartition", "does_not_exist")))
        .contains(fieldId("datepartition"));
  }

  @TestTemplate
  public void testColumnResolvesCaseInsensitivelyByDefault() {
    createTable();
    assertThat(boundedColumnIds(ImmutableList.of("Datepartition")))
        .contains(fieldId("datepartition"));
  }

  @TestTemplate
  public void testCaseSensitiveModeSkipsMismatchedCase() {
    createTable();
    spark.conf().set("spark.sql.caseSensitive", "true");
    assertThat(boundedColumnIds(ImmutableList.of("Datepartition")))
        .doesNotContain(fieldId("datepartition"));
  }
}
