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
import static org.assertj.core.api.Assertions.tuple;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
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

/**
 * Spark SQL coverage for ORC initial-defaults: empty-projection filters, mixed physical+defaulted
 * predicates, and nested defaults when the parent struct is null. Defaulted projections are forced
 * onto the row reader even when ORC vectorization is enabled.
 */
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
  private static final Schema NESTED_TABLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional("loc")
              .withId(2)
              .ofType(
                  Types.StructType.of(
                      Types.NestedField.optional(3, "city", Types.StringType.get()),
                      Types.NestedField.optional("country")
                          .withId(4)
                          .ofType(Types.StringType.get())
                          .withInitialDefault(Expressions.lit("US"))
                          .build()))
              .build());
  private static final Schema NESTED_OLD_FILE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional("loc")
              .withId(2)
              .ofType(
                  Types.StructType.of(
                      Types.NestedField.optional(3, "city", Types.StringType.get())))
              .build());
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
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name())
        .set(TableProperties.ORC_VECTORIZATION_ENABLED, "true")
        .commit();
    Assertions.assertThat(table.schema().findField("country").initialDefault()).isEqualTo("US");

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
  public void testDefaultedScansAreRoutedToTheRowReader() {
    Assertions.assertThat(supportsColumnarReads(TABLE_SCHEMA)).isFalse();
    Schema countryOnly =
        new Schema(
            Types.NestedField.optional("country")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("US"))
                .build());
    Assertions.assertThat(supportsColumnarReads(countryOnly)).isFalse();
  }

  @Test
  public void testMixedFilesApplyDefaultPerFile() {
    List<Row> rows = read().select("id", "country", "data").orderBy("id").collectAsList();

    Assertions.assertThat(rows)
        .extracting(row -> row.getLong(0), row -> row.getString(1), row -> row.getString(2))
        .containsExactly(
            tuple(1L, "US", "old"), tuple(2L, "CA", "new-value"), tuple(3L, null, "new-null"));
  }

  @Test
  public void testFilterWhenOnlyOmittedDefaultColumnIsProjected() {
    // Projection+filter on only the defaulted column yields an empty ORC schema for the old file.
    // SARG must disable pushdown so Spark can evaluate the filled default.
    Assertions.assertThat(
            read().select("country").filter("country = 'US'").collectAsList().stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList()))
        .containsExactly("US");

    Assertions.assertThat(read().select("country").filter("country = 'bar'").collectAsList())
        .isEmpty();

    Assertions.assertThat(
            read().select("country").filter("country IS NULL").collectAsList().stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList()))
        .containsExactly((String) null);
  }

  @Test
  public void testFilterMixedPhysicalAndDefaultedColumn() {
    List<Long> ids =
        read().filter("id = 1 AND country = 'US'").select("id").collectAsList().stream()
            .map(row -> row.getLong(0))
            .collect(Collectors.toList());
    Assertions.assertThat(ids).containsExactly(1L);

    Assertions.assertThat(read().filter("id = 1 AND country = 'CA'").collectAsList()).isEmpty();
  }

  @Test
  public void testFilterDefaultedColumnAcrossMixedFiles() {
    // Old file reads country as 'US'; new file has 'CA' and an explicit null. SARG is disabled for
    // the omitted column so Spark filters the filled values.
    Assertions.assertThat(
            read().filter("country IS NOT NULL").select("id").orderBy("id").collectAsList().stream()
                .map(row -> row.getLong(0))
                .collect(Collectors.toList()))
        .containsExactly(1L, 2L);

    Assertions.assertThat(
            read().filter("upper(country) = 'US'").select("id").orderBy("id").collectAsList()
                .stream()
                .map(row -> row.getLong(0))
                .collect(Collectors.toList()))
        .containsExactly(1L);

    Assertions.assertThat(
            read().filter("country = 'CA'").select("id").collectAsList().stream()
                .map(row -> row.getLong(0))
                .collect(Collectors.toList()))
        .containsExactly(2L);
  }

  @Test
  public void testFilterNestedDefaultWhenParentStructIsNull() throws IOException {
    File nestedLocation = temp.newFolder("nested-default-files");
    Table nestedTable =
        new HadoopTables(CONF)
            .create(NESTED_TABLE_SCHEMA, PartitionSpec.unpartitioned(), nestedLocation.toString());
    nestedTable
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, "2")
        .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.ORC.name())
        .set(TableProperties.ORC_VECTORIZATION_ENABLED, "true")
        .commit();

    Types.StructType oldLocType = NESTED_OLD_FILE_SCHEMA.findType("loc").asStructType();
    Record locSf = GenericRecord.create(oldLocType);
    locSf.setField("city", "San Francisco");
    Record presentLoc = GenericRecord.create(NESTED_OLD_FILE_SCHEMA);
    presentLoc.setField("id", 1L);
    presentLoc.setField("loc", locSf);

    Record nullLoc = GenericRecord.create(NESTED_OLD_FILE_SCHEMA);
    nullLoc.setField("id", 2L);
    nullLoc.setField("loc", null);

    Types.StructType newLocType = NESTED_TABLE_SCHEMA.findType("loc").asStructType();
    Record locCa = GenericRecord.create(newLocType);
    locCa.setField("city", "Toronto");
    locCa.setField("country", "CA");
    Record presentCa = GenericRecord.create(NESTED_TABLE_SCHEMA);
    presentCa.setField("id", 3L);
    presentCa.setField("loc", locCa);

    nestedTable
        .newAppend()
        .appendFile(
            writeFile(
                nestedLocation,
                NESTED_OLD_FILE_SCHEMA,
                Lists.newArrayList(presentLoc, nullLoc),
                "old-nested.orc"))
        .appendFile(
            writeFile(
                nestedLocation,
                NESTED_TABLE_SCHEMA,
                Lists.newArrayList(presentCa),
                "new-nested.orc"))
        .commit();

    Dataset<Row> nestedRead =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VECTORIZATION_ENABLED, "true")
            .load(nestedLocation.toString());

    // Present loc fills country from initial-default; a null parent stays null.
    Assertions.assertThat(
            nestedRead.selectExpr("id", "loc.country as country").orderBy("id").collectAsList()
                .stream()
                .map(row -> tuple(row.getLong(0), row.isNullAt(1) ? null : row.getString(1)))
                .collect(Collectors.toList()))
        .containsExactly(tuple(1L, "US"), tuple(2L, null), tuple(3L, "CA"));

    Assertions.assertThat(
            nestedRead.filter("loc.country = 'US'").select("id").collectAsList().stream()
                .map(row -> row.getLong(0))
                .collect(Collectors.toList()))
        .containsExactly(1L);

    Assertions.assertThat(
            nestedRead.filter("loc.country IS NULL").select("id").collectAsList().stream()
                .map(row -> row.getLong(0))
                .collect(Collectors.toList()))
        .containsExactly(2L);

    Assertions.assertThat(
            nestedRead.filter("loc.country = 'CA'").select("id").collectAsList().stream()
                .map(row -> row.getLong(0))
                .collect(Collectors.toList()))
        .containsExactly(3L);
  }

  private Dataset<Row> read() {
    return spark
        .read()
        .format("iceberg")
        .option(SparkReadOptions.VECTORIZATION_ENABLED, "true")
        .load(tableLocation.toString());
  }

  private boolean supportsColumnarReads(Schema projection) {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                "path", tableLocation.toString(), SparkReadOptions.VECTORIZATION_ENABLED, "true"));
    SparkScanBuilder builder = new SparkScanBuilder(spark, table, options);
    builder.pruneColumns(SparkSchemaUtil.convert(projection));
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
