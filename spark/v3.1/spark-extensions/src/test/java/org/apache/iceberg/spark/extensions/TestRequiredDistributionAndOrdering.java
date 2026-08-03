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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestRequiredDistributionAndOrdering extends SparkExtensionsTestBase {

  public TestRequiredDistributionAndOrdering(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  // Unsorted partitioned table: rule must not synthesize a partition-spec sort. Fanout is
  // enabled so FanoutDataWriter accepts the unclustered bucket transitions.
  @Test
  public void testNoSyntheticPartitionSortWithBucketTransforms() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c1))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(7L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @Test
  public void testPartitionColumnsArePrependedForRangeDistribution() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c1))",
        tableName);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should automatically prepend partition columns to the ordering
    sql("ALTER TABLE %s WRITE ORDERED BY c1, c2", tableName);

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(7L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @Test
  public void testSortOrderIncludesPartitionColumns() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c1))",
        tableName);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should succeed with a correct sort order
    sql("ALTER TABLE %s WRITE ORDERED BY bucket(2, c3), c1, c2", tableName);

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(7L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @Test
  public void testHashDistributionOnBucketedColumn() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c1))",
        tableName);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should automatically prepend partition columns to the local ordering after hash distribution
    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION ORDERED BY c1, c2", tableName);

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(7L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  // INSERT VALUES into an unsorted partitioned table across various transform types. Fanout is
  // enabled because the rule no longer clusters unsorted tables.

  @Test
  public void testInsertValuesOnDecimalBucketedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 DECIMAL(20, 2)) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c2)) "
            + "TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    sql("INSERT INTO %s VALUES (1, 20.2), (2, 40.2), (3, 60.2)", tableName);

    List<Object[]> expected =
        ImmutableList.of(
            row(1, new BigDecimal("20.20")),
            row(2, new BigDecimal("40.20")),
            row(3, new BigDecimal("60.20")));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @Test
  public void testInsertValuesOnStringBucketedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c2)) "
            + "TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

    List<Object[]> expected = ImmutableList.of(row(1, "A"), row(2, "B"));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @Test
  public void testInsertValuesOnDecimalTruncatedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 DECIMAL(20, 2)) "
            + "USING iceberg "
            + "PARTITIONED BY (truncate(2, c2)) "
            + "TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    sql("INSERT INTO %s VALUES (1, 20.2), (2, 40.2)", tableName);

    List<Object[]> expected =
        ImmutableList.of(row(1, new BigDecimal("20.20")), row(2, new BigDecimal("40.20")));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @Test
  public void testInsertValuesOnLongTruncatedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 BIGINT) "
            + "USING iceberg "
            + "PARTITIONED BY (truncate(2, c2)) "
            + "TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    sql("INSERT INTO %s VALUES (1, 22222222222222), (2, 444444444444)", tableName);

    List<Object[]> expected = ImmutableList.of(row(1, 22222222222222L), row(2, 444444444444L));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  // testRangeDistributionWithQuotedColumnNames from the v3.2 suite is intentionally omitted:
  // v3.1 SortOrderToSpark passes raw column names through Expressions.column, which can't parse
  // dotted identifiers. v3.2 fixed this by indexing schema-quoted names; that fix is out of scope
  // for this backport.

  // Unclustered input. Rule attaches a local Sort (with partition prefix) for sorted/RANGE
  // tables; HASH/NONE on unsorted tables is a no-op, so those enable fanout. Distribution is
  // never injected — see ExtendedV2Writes class doc. *FailsWithoutRule tests below pin down
  // baseline writer behavior when the rule is disabled.

  // HASH on an unsorted table: rule is a no-op. Fanout enabled. For rule-injected clustering
  // here, set a sort order — see testHashDistributionWithExplicitSortOrder.
  @Test
  public void testHashDistributionModeViaTableProperty() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='hash', '%s'='true')",
        tableName,
        TableProperties.WRITE_DISTRIBUTION_MODE,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals(
        "Distribution mode must be hash",
        "hash",
        table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE));

    Dataset<Row> inputDF = unclusteredInput();

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @Test
  public void testRangeDistributionModeViaSortOrder() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);

    // WRITE ORDERED BY implicitly sets the distribution mode to range
    sql("ALTER TABLE %s WRITE ORDERED BY category, id", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals(
        "Distribution mode must be range",
        "range",
        table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE));
    SortOrder expectedOrder =
        SortOrder.builderFor(table.schema())
            .withOrderId(1)
            .asc("category", NullOrder.NULLS_FIRST)
            .asc("id", NullOrder.NULLS_FIRST)
            .build();
    Assert.assertEquals("Sort order must match", expectedOrder, table.sortOrder());

    Dataset<Row> inputDF = unclusteredInput();

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @Test
  public void testHashDistributionWithExplicitSortOrder() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);

    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION ORDERED BY category", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals(
        "Distribution mode must be hash",
        "hash",
        table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE));

    Dataset<Row> inputDF = unclusteredInput();

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @Test
  public void testNoneDistributionModeViaTableProperty() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    // `none` distribution: the rule attaches no repartition and no synthesized sort. Fanout is
    // enabled so the FanoutDataWriter accepts the unclustered input directly.
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='none', '%s'='true')",
        tableName,
        TableProperties.WRITE_DISTRIBUTION_MODE,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals(
        "Distribution mode must be none",
        "none",
        table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE));

    Dataset<Row> inputDF = unclusteredInput();

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @Test
  public void testRangeDistributionModeViaTableProperty() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='range')",
        tableName, TableProperties.WRITE_DISTRIBUTION_MODE);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals(
        "Distribution mode must be range",
        "range",
        table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE));

    Dataset<Row> inputDF = unclusteredInput();

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  // Rule disabled — asserts ClusteredDataWriter rejects unclustered input. Pre-rule baseline.

  @Test
  public void testNoneDistributionFailsWithoutRule() {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='none')",
        tableName, TableProperties.WRITE_DISTRIBUTION_MODE);

    assertWriterRejectsUnclusteredInput();
  }

  @Test
  public void testHashDistributionFailsWithoutRule() {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='hash')",
        tableName, TableProperties.WRITE_DISTRIBUTION_MODE);

    assertWriterRejectsUnclusteredInput();
  }

  @Test
  public void testRangeDistributionFailsWithoutRule() {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='range')",
        tableName, TableProperties.WRITE_DISTRIBUTION_MODE);

    assertWriterRejectsUnclusteredInput();
  }

  private void assertWriterRejectsUnclusteredInput() {
    Dataset<Row> inputDF = unclusteredInput();
    spark
        .conf()
        .set(
            "spark.sql.optimizer.excludedRules",
            "org.apache.spark.sql.execution.datasources.v2.ExtendedV2Writes");
    try {
      Assertions.assertThatThrownBy(() -> inputDF.writeTo(tableName).append())
          .as(
              "ClusteredDataWriter should reject unclustered input when ExtendedV2Writes is disabled")
          .isInstanceOf(SparkException.class)
          .hasStackTraceContaining("Incoming records violate the writer assumption");
    } finally {
      spark.conf().unset("spark.sql.optimizer.excludedRules");
    }
  }

  // Empty input on the rule's no-op path (HASH unsorted): must still produce a clean snapshot.
  @Test
  public void testEmptyInputWithHashDistribution() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='hash')",
        tableName, TableProperties.WRITE_DISTRIBUTION_MODE);

    Dataset<Row> emptyDF = unclusteredInput().where("1 = 0");
    emptyDF.writeTo(tableName).append();

    assertEquals(
        "Row count must be zero",
        ImmutableList.of(row(0L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  // Null partition values, one row per task — trivially clustered. Null-handling guard.
  @Test
  public void testNullPartitionValuesWithHashDistribution() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (category)",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='hash')",
        tableName, TableProperties.WRITE_DISTRIBUTION_MODE);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "d1"),
            new ThreeColumnRecord(2, null, "d2"),
            new ThreeColumnRecord(3, null, "d3"),
            new ThreeColumnRecord(4, null, "d4"));
    Dataset<Row> inputDF =
        spark
            .createDataFrame(data, ThreeColumnRecord.class)
            .selectExpr("c1 AS id", "c2 AS category", "c3 AS data")
            .repartition(4);

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(4L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  // High-cardinality bucket transform: rule is a no-op (unsorted), fanout handles unclustering.
  @Test
  public void testHighCardinalityBucketWithHashDistribution() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(64, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='hash', '%s'='true')",
        tableName,
        TableProperties.WRITE_DISTRIBUTION_MODE,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    Dataset<Row> inputDF = unclusteredInput();
    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  // Single shuffle partition → all bucket values in one task, unclustered. Fanout required.
  @Test
  public void testHashDistributionWithSingleShufflePartition() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='hash', '%s'='true')",
        tableName,
        TableProperties.WRITE_DISTRIBUTION_MODE,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    String original = spark.conf().get("spark.sql.shuffle.partitions");
    spark.conf().set("spark.sql.shuffle.partitions", "1");
    try {
      unclusteredInput().writeTo(tableName).append();
    } finally {
      spark.conf().set("spark.sql.shuffle.partitions", original);
    }

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  // AQE coalesces post-shuffle partitions → multiple buckets in one task. Fanout required.
  @Test
  public void testHashDistributionWithAQEEnabled() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='hash', '%s'='true')",
        tableName,
        TableProperties.WRITE_DISTRIBUTION_MODE,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    String original = spark.conf().get("spark.sql.adaptive.enabled");
    spark.conf().set("spark.sql.adaptive.enabled", "true");
    try {
      unclusteredInput().writeTo(tableName).append();
    } finally {
      spark.conf().set("spark.sql.adaptive.enabled", original);
    }

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  // Fanout writer accepts unclustered input directly. Rule is a no-op (unsorted, no distribution).
  @Test
  public void testFanoutWriterWithHashDistribution() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='hash', '%s'='true')",
        tableName,
        TableProperties.WRITE_DISTRIBUTION_MODE,
        TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED);

    unclusteredInput().writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  // saveAsTable("append") on an existing V2 table produces AppendData, the same plan node the
  // rule matches for writeTo(...).append(). RANGE distribution keeps the rule active (sort
  // attached) so this exercises the full path, not the no-op branch.
  @Test
  public void testSaveAsTableAppendWithRangeDistribution() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql("ALTER TABLE %s WRITE ORDERED BY category, id", tableName);

    // format("iceberg") is required for the session catalog. DataFrameWriter#saveAsTable only
    // takes the v2 path for a SessionCatalogAndIdentifier when lookupV2Provider() is defined, and
    // that returns None for the default "parquet" source because ParquetDataSourceV2 is a
    // FileDataSourceV2 (SPARK-28396). Without it the write falls through to the v1 path and
    // PreprocessTableCreation rejects it against the Hive-registered table, so AppendData -- the
    // plan node this rule matches -- is never produced. The non-session catalogs resolve as
    // NonSessionCatalogAndIdentifier and reach v2 regardless.
    unclusteredInput().write().format("iceberg").mode("append").saveAsTable(tableName);

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(20L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  // 20 rows across 4 buckets, randomly spread across 4 Spark partitions. Worst-case
  // unclustered input for ClusteredDataWriter.
  private Dataset<Row> unclusteredInput() {
    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(0, "B", "d0"),
            new ThreeColumnRecord(1, "A", "d1"),
            new ThreeColumnRecord(2, "C", "d2"),
            new ThreeColumnRecord(3, "B", "d3"),
            new ThreeColumnRecord(4, "A", "d4"),
            new ThreeColumnRecord(5, "C", "d5"),
            new ThreeColumnRecord(6, "B", "d6"),
            new ThreeColumnRecord(7, "A", "d7"),
            new ThreeColumnRecord(8, "C", "d8"),
            new ThreeColumnRecord(9, "B", "d9"),
            new ThreeColumnRecord(10, "A", "d10"),
            new ThreeColumnRecord(11, "C", "d11"),
            new ThreeColumnRecord(12, "B", "d12"),
            new ThreeColumnRecord(13, "A", "d13"),
            new ThreeColumnRecord(14, "C", "d14"),
            new ThreeColumnRecord(15, "B", "d15"),
            new ThreeColumnRecord(16, "A", "d16"),
            new ThreeColumnRecord(17, "C", "d17"),
            new ThreeColumnRecord(18, "B", "d18"),
            new ThreeColumnRecord(19, "A", "d19"));
    return spark
        .createDataFrame(data, ThreeColumnRecord.class)
        .selectExpr("c1 AS id", "c2 AS category", "c3 AS data")
        .repartition(4);
  }
}
