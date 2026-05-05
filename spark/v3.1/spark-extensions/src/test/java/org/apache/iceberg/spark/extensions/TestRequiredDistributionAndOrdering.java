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

  @Test
  public void testDefaultLocalSortWithBucketTransforms() throws NoSuchTableException {
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

    // should insert a local sort by partition columns by default
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

  @Test
  public void testDefaultSortOnDecimalBucketedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 DECIMAL(20, 2)) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c2))",
        tableName);

    sql("INSERT INTO %s VALUES (1, 20.2), (2, 40.2), (3, 60.2)", tableName);

    List<Object[]> expected =
        ImmutableList.of(
            row(1, new BigDecimal("20.20")),
            row(2, new BigDecimal("40.20")),
            row(3, new BigDecimal("60.20")));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @Test
  public void testDefaultSortOnStringBucketedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c2))",
        tableName);

    sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

    List<Object[]> expected = ImmutableList.of(row(1, "A"), row(2, "B"));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @Test
  public void testDefaultSortOnDecimalTruncatedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 DECIMAL(20, 2)) "
            + "USING iceberg "
            + "PARTITIONED BY (truncate(2, c2))",
        tableName);

    sql("INSERT INTO %s VALUES (1, 20.2), (2, 40.2)", tableName);

    List<Object[]> expected =
        ImmutableList.of(row(1, new BigDecimal("20.20")), row(2, new BigDecimal("40.20")));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @Test
  public void testDefaultSortOnLongTruncatedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 BIGINT) "
            + "USING iceberg "
            + "PARTITIONED BY (truncate(2, c2))",
        tableName);

    sql("INSERT INTO %s VALUES (1, 22222222222222), (2, 444444444444)", tableName);

    List<Object[]> expected = ImmutableList.of(row(1, 22222222222222L), row(2, 444444444444L));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  // testRangeDistributionWithQuotedColumnNames from the v3.2 suite is intentionally omitted:
  // v3.1 SortOrderToSpark passes raw column names through Expressions.column, which can't parse
  // dotted identifiers. v3.2 fixed this by indexing schema-quoted names; that fix is out of scope
  // for this backport.

  // The tests below feed deliberately unclustered input to the ClusteredDataWriter. Without the
  // ExtendedV2Writes rule the writer would reject the data with
  // "Incoming records violate the writer assumption that records are clustered by spec and by
  // partition" — succeeding here confirms the rule re-clusters using the table's configured
  // distribution and ordering.

  @Test
  public void testHashDistributionModeViaTableProperty() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(4, id))",
        tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='hash')",
        tableName, TableProperties.WRITE_DISTRIBUTION_MODE);

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
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='none')",
        tableName, TableProperties.WRITE_DISTRIBUTION_MODE);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals(
        "Distribution mode must be none",
        "none",
        table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE));

    Dataset<Row> inputDF = unclusteredInput();

    // `none` skips the repartition, but the required ordering is computed independently and
    // SortOrderUtil.buildSortOrder synthesizes one from the partition fields for any partitioned
    // table (it only returns unsorted when the table is also unpartitioned). The rule therefore
    // attaches a local sort by the partition columns, which clusters rows within each Spark task
    // and lets the ClusteredDataWriter succeed without a cross-task shuffle.
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

  // The tests below disable ExtendedV2Writes so the same unclustered input reaches the
  // ClusteredDataWriter without any rule-injected repartition or local sort. Each one asserts
  // the writer rejects it — establishing the pre-rule baseline that the positive tests above
  // are claiming to fix.

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

  // Builds a 20-row dataset spread across all 4 buckets and randomly shuffled across 4 Spark
  // partitions, so each task sees rows for multiple buckets — the worst-case input for a
  // ClusteredDataWriter without re-clustering.
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
