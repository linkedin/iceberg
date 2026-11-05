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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.TestTemplate;

public class TestMergeOnReadMerge extends TestMerge {

  private static final int WIDE_TABLE_COLUMN_COUNT = 1010;

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.FORMAT_VERSION,
        "2",
        TableProperties.MERGE_MODE,
        RowLevelOperationMode.MERGE_ON_READ.modeName());
  }

  @TestTemplate
  public void testMergeDeleteFileGranularity() {
    checkMergeDeleteGranularity(DeleteGranularity.FILE);
  }

  @TestTemplate
  public void testMergeDeletePartitionGranularity() {
    checkMergeDeleteGranularity(DeleteGranularity.PARTITION);
  }

  @TestTemplate
  public void testMergeAndRewriteWideOrcTableWithFullUpdates() throws NoSuchTableException {
    checkMergeAndRewriteWideOrcTable(true);
  }

  @TestTemplate
  public void testMergeAndRewriteWideOrcTableWithPartialUpdates() throws NoSuchTableException {
    checkMergeAndRewriteWideOrcTable(false);
  }

  private void checkMergeAndRewriteWideOrcTable(boolean updateAllColumns)
      throws NoSuchTableException {
    assumeThat(fileFormat).isEqualTo(FileFormat.ORC);
    assumeThat(branch).isIn(null, SnapshotRef.MAIN_BRANCH);

    String columns =
        "id BIGINT, "
            + IntStream.range(1, WIDE_TABLE_COLUMN_COUNT)
                .mapToObj(i -> "c" + i + (i == 999 ? " ARRAY<INT>" : " STRING"))
                .collect(Collectors.joining(", "));
    createAndInitTable(columns, "PARTITIONED BY (bucket(2, id))", null /* empty */);

    Table table = validationCatalog.loadTable(tableIdent);
    StructType sparkSchema = spark.table(tableName).schema();
    assertThat(table.schema().columns()).hasSize(WIDE_TABLE_COLUMN_COUNT);
    // The business LIST and partition INTEGER deliberately share field ID 1000.
    assertThat(table.schema().findField("c999").fieldId()).isEqualTo(1000);
    assertThat(table.schema().findField("c999").type().isListType()).isTrue();
    assertThat(table.spec().fields().get(0).fieldId()).isEqualTo(1000);

    wideOrcRows(sparkSchema, wideOrcRow(0, "target"), wideOrcRow(1, "target"))
        .coalesce(1)
        .writeTo(tableName)
        .append();
    wideOrcRows(sparkSchema, wideOrcRow(2, "target"), wideOrcRow(3, "target"))
        .coalesce(1)
        .writeTo(tableName)
        .append();
    wideOrcRows(
            sparkSchema, wideOrcRow(1, "source"), wideOrcRow(2, "source"), wideOrcRow(4, "source"))
        .createOrReplaceTempView("source");
    assertThat(spark.table("source").schema()).isEqualTo(sparkSchema);

    String updateClause =
        updateAllColumns ? "UPDATE SET *" : "UPDATE SET c1 = s.c1, c1000 = s.c1000";
    sql(
        "MERGE INTO %s t USING source s ON t.id = s.id "
            + "WHEN MATCHED AND t.id = 1 THEN DELETE "
            + "WHEN MATCHED THEN %s "
            + "WHEN NOT MATCHED THEN INSERT *",
        commitTarget(), updateClause);
    table.refresh();

    Object[] updated = wideOrcRow(2, updateAllColumns ? "source" : "target");
    if (!updateAllColumns) {
      Object[] source = wideOrcRow(2, "source");
      updated[1] = source[1];
      updated[1000] = source[1000];
    }
    List<Object[]> expected =
        ImmutableList.of(
            wideOrcRow(0, "target"), updated, wideOrcRow(3, "target"), wideOrcRow(4, "source"));
    assertEquals(
        "MERGE must update, delete, insert and preserve every column",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
    assertThat(table.currentSnapshot().summary())
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, "2");
    Set<DeleteFile> deletesBefore = TestHelpers.deleteFiles(table);
    assertThat(deletesBefore).isNotEmpty();
    assertThat(deletesBefore)
        .allSatisfy(file -> assertThat(file.content()).isEqualTo(FileContent.POSITION_DELETES));
    List<Object[]> expectedDeletes =
        sql("SELECT file_path, pos FROM %s.position_deletes ORDER BY file_path, pos", tableName);
    assertThat(expectedDeletes).hasSize(2);

    List<Object[]> deleteOutput =
        sql(
            "CALL %s.system.rewrite_position_delete_files("
                + "table => '%s', options => map('rewrite-all','true'))",
            catalogName, tableIdent);
    table.refresh();
    Set<DeleteFile> deletesAfter = TestHelpers.deleteFiles(table);
    assertThat(deletesAfter).isNotEmpty();
    assertEquals(
        "Position-delete rewrite must replace all input files and report their sizes",
        ImmutableList.of(
            row(
                deletesBefore.size(),
                deletesAfter.size(),
                deletesBefore.stream().mapToLong(DeleteFile::fileSizeInBytes).sum(),
                deletesAfter.stream().mapToLong(DeleteFile::fileSizeInBytes).sum())),
        deleteOutput);
    assertThat(
            deletesAfter.stream().map(file -> file.path().toString()).collect(Collectors.toSet()))
        .doesNotContainAnyElementsOf(
            deletesBefore.stream().map(file -> file.path().toString()).collect(Collectors.toSet()));
    assertEquals(
        "Position-delete rewrite must preserve the delete records",
        expectedDeletes,
        sql("SELECT file_path, pos FROM %s.position_deletes ORDER BY file_path, pos", tableName));
    assertEquals(
        "Position-delete rewrite must preserve every data column",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    List<DataFile> dataBefore = TestHelpers.dataFiles(table);
    assertThat(dataBefore).isNotEmpty();
    List<Object[]> dataOutput =
        sql(
            "CALL %s.system.rewrite_data_files("
                + "table => '%s', options => map('rewrite-all','true'))",
            catalogName, tableIdent);
    table.refresh();
    List<DataFile> dataAfter = TestHelpers.dataFiles(table);
    assertThat(dataAfter).isNotEmpty();
    assertEquals(
        "Data-file rewrite must replace all input files without failures",
        ImmutableList.of(
            row(
                dataBefore.size(),
                dataAfter.size(),
                dataBefore.stream().mapToLong(DataFile::fileSizeInBytes).sum(),
                0)),
        dataOutput);
    assertThat(dataAfter.stream().map(file -> file.path().toString()).collect(Collectors.toSet()))
        .doesNotContainAnyElementsOf(
            dataBefore.stream().map(file -> file.path().toString()).collect(Collectors.toSet()));
    assertEquals(
        "Data-file rewrite must preserve every data column",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  private Dataset<Row> wideOrcRows(StructType schema, Object[]... values) {
    List<Row> rows = Arrays.stream(values).map(RowFactory::create).collect(Collectors.toList());
    return spark.createDataFrame(rows, schema);
  }

  private Object[] wideOrcRow(long id, String prefix) {
    Object[] values = new Object[WIDE_TABLE_COLUMN_COUNT];
    values[0] = id;
    for (int i = 1; i < WIDE_TABLE_COLUMN_COUNT; i++) {
      values[i] = prefix + ":c" + i + ":" + id;
    }
    values[999] = ImmutableList.of(Math.toIntExact(id), prefix.equals("source") ? 1999 : 999);
    return values;
  }

  private void checkMergeDeleteGranularity(DeleteGranularity deleteGranularity) {
    createAndInitTable("id INT, dep STRING", "PARTITIONED BY (dep)", null /* empty */);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
        tableName, TableProperties.DELETE_GRANULARITY, deleteGranularity);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 3, \"dep\": \"hr\" }\n" + "{ \"id\": 4, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 1, \"dep\": \"it\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");
    append(tableName, "{ \"id\": 3, \"dep\": \"it\" }\n" + "{ \"id\": 4, \"dep\": \"it\" }");

    createBranchIfNeeded();

    createOrReplaceView("source", ImmutableList.of(1, 3, 5), Encoders.INT());

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.value "
            + "WHEN MATCHED THEN "
            + " DELETE "
            + "WHEN NOT MATCHED THEN "
            + " INSERT (id, dep) VALUES (-1, 'other')",
        commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).hasSize(5);

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    String expectedDeleteFilesCount = deleteGranularity == DeleteGranularity.FILE ? "4" : "2";
    validateMergeOnRead(currentSnapshot, "3", expectedDeleteFilesCount, "1");

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "other"), row(2, "hr"), row(2, "it"), row(4, "hr"), row(4, "it")),
        sql("SELECT * FROM %s ORDER BY id ASC, dep ASC", selectTarget()));
  }
}
