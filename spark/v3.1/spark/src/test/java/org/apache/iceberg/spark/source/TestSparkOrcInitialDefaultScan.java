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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSparkOrcInitialDefaultScan {
  private static final Schema TABLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional("country")
              .withId(2)
              .ofType(Types.StringType.get())
              .withInitialDefault(Expressions.lit("US"))
              .build(),
          Types.NestedField.optional(3, "data", Types.StringType.get()));
  private static final Schema OLD_FILE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));
  private static final Configuration CONF = new Configuration();
  private static SparkSession spark;

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private File tableLocation;
  private Table table;

  @BeforeClass
  public static void startSpark() {
    spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    spark.stop();
    spark = null;
  }

  @Before
  public void createMixedFileTable() throws IOException {
    tableLocation = temp.newFolder("mixed-default-files");
    table =
        new HadoopTables(CONF)
            .create(TABLE_SCHEMA, PartitionSpec.unpartitioned(), tableLocation.toString());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name())
        .set(TableProperties.ORC_VECTORIZATION_ENABLED, "true")
        .commit();

    Record old = GenericRecord.create(OLD_FILE_SCHEMA);
    old.setField("id", 1L);
    old.setField("data", "old");

    Record present = GenericRecord.create(TABLE_SCHEMA);
    present.setField("id", 2L);
    present.setField("country", "CA");
    present.setField("data", "new-value");

    Record presentNull = GenericRecord.create(TABLE_SCHEMA);
    presentNull.setField("id", 3L);
    presentNull.setField("country", null);
    presentNull.setField("data", "new-null");

    table
        .newAppend()
        .appendFile(writeFile(OLD_FILE_SCHEMA, Lists.newArrayList(old), "old.orc"))
        .appendFile(writeFile(TABLE_SCHEMA, Lists.newArrayList(present, presentNull), "new.orc"))
        .commit();
  }

  @Test
  public void testMixedFilesApplyDefaultPerFile() {
    List<Row> rows = read().select("id", "country", "data").orderBy("id").collectAsList();

    Assertions.assertThat(rows)
        .extracting(row -> row.getLong(0), row -> row.getString(1), row -> row.getString(2))
        .containsExactly(
            Lists.newArrayList(1L, "US", "old"),
            Lists.newArrayList(2L, "CA", "new-value"),
            Lists.newArrayList(3L, null, "new-null"));
  }

  @Test
  public void testFilterPushdownOnInitialDefaultColumnAbsentFromFile() {
    assertIds("country = 'US'", 1L);
    assertIds("upper(country) = 'US'", 1L);
    assertIds("country IS NOT NULL", 1L, 2L);
    assertIds("country = 'CA'", 2L);
    assertIds("country IS NULL", 3L);
  }

  @Test
  public void testSetAndRangeFiltersEvaluateMaterializedDefault() {
    assertIds("country IN ('US', 'MX')", 1L);
    assertIds("country NOT IN ('CA', 'MX')", 1L);
    assertIds("country >= 'US'", 1L);
    assertIds("country < 'ZW'", 1L, 2L);
    assertIds("country LIKE 'U%'", 1L);
    assertIds("country NOT LIKE 'X%'", 1L, 2L);
  }

  @Test
  public void testFilterPushdownOnNestedInitialDefaultColumnAbsentFromFile() throws IOException {
    File nestedLocation = temp.newFolder("nested-default-files");
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "loc",
                Types.StructType.of(
                    Types.NestedField.optional(3, "city", Types.StringType.get()))));
    Schema currentSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "loc",
                Types.StructType.of(
                    Types.NestedField.optional(3, "city", Types.StringType.get()),
                    Types.NestedField.optional("country")
                        .withId(4)
                        .ofType(Types.StringType.get())
                        .withInitialDefault(Expressions.lit("US"))
                        .build())));
    Table nestedTable =
        new HadoopTables(CONF)
            .create(currentSchema, PartitionSpec.unpartitioned(), nestedLocation.toString());
    nestedTable
        .updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name())
        .set(TableProperties.ORC_VECTORIZATION_ENABLED, "true")
        .commit();

    Record oldLoc = GenericRecord.create(oldSchema.findField("loc").type().asStructType());
    oldLoc.setField("city", "San Francisco");
    Record oldPresent = GenericRecord.create(oldSchema);
    oldPresent.setField("id", 1L);
    oldPresent.setField("loc", oldLoc);
    Record oldNullParent = GenericRecord.create(oldSchema);
    oldNullParent.setField("id", 2L);
    oldNullParent.setField("loc", null);

    Record newLoc = GenericRecord.create(currentSchema.findField("loc").type().asStructType());
    newLoc.setField("city", "Toronto");
    newLoc.setField("country", "CA");
    Record newPresent = GenericRecord.create(currentSchema);
    newPresent.setField("id", 3L);
    newPresent.setField("loc", newLoc);

    nestedTable
        .newAppend()
        .appendFile(
            writeFile(
                nestedLocation,
                oldSchema,
                Lists.newArrayList(oldPresent, oldNullParent),
                "old-nested.orc"))
        .appendFile(
            writeFile(
                nestedLocation, currentSchema, Lists.newArrayList(newPresent), "new-nested.orc"))
        .commit();

    assertIds(nestedLocation, "loc.country = 'US'", 1L);
    assertIds(nestedLocation, "loc.country = 'CA'", 3L);
    assertIds(nestedLocation, "loc.country IS NULL", 2L);
  }

  @Test
  public void testDefaultProjectionUsesIterativeReader() {
    Assertions.assertThat(supportsColumnarReads(null)).isFalse();
  }

  @Test
  public void testProjectionWithoutDefaultRemainsVectorized() {
    Assertions.assertThat(
            supportsColumnarReads(
                new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(3, "data", Types.StringType.get()))))
        .isTrue();
  }

  private Dataset<Row> read() {
    return spark
        .read()
        .format("iceberg")
        .option(SparkReadOptions.VECTORIZATION_ENABLED, "true")
        .load(tableLocation.toString());
  }

  private void assertIds(String filter, Long... expectedIds) {
    assertIds(tableLocation, filter, expectedIds);
  }

  private void assertIds(File location, String filter, Long... expectedIds) {
    List<Long> ids =
        spark.read().format("iceberg").option(SparkReadOptions.VECTORIZATION_ENABLED, "true")
            .load(location.toString()).filter(filter).select("id").orderBy("id").collectAsList()
            .stream()
            .map(row -> row.getLong(0))
            .collect(java.util.stream.Collectors.toList());
    Assertions.assertThat(ids).containsExactly(expectedIds);
  }

  private boolean supportsColumnarReads(Schema projection) {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                "path", tableLocation.toString(), SparkReadOptions.VECTORIZATION_ENABLED, "true"));
    SparkScanBuilder builder = new SparkScanBuilder(spark, table, options);
    if (projection != null) {
      builder.pruneColumns(SparkSchemaUtil.convert(projection));
    }
    Batch batch = builder.build().toBatch();
    InputPartition partition = batch.planInputPartitions()[0];
    PartitionReaderFactory factory = batch.createReaderFactory();
    return factory.supportColumnarReads(partition);
  }

  private DataFile writeFile(Schema schema, List<Record> records, String name) throws IOException {
    return writeFile(tableLocation, schema, records, name);
  }

  private DataFile writeFile(File location, Schema schema, List<Record> records, String name)
      throws IOException {
    File file = new File(new File(location, "data"), name);
    Assertions.assertThat(file.getParentFile().mkdirs() || file.getParentFile().isDirectory())
        .isTrue();
    FileAppender<Record> appender =
        new GenericAppenderFactory(schema).newAppender(localOutput(file), FileFormat.ORC);
    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withInputFile(localInput(file))
        .withMetrics(appender.metrics())
        .build();
  }
}
