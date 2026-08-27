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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.spark.data.TestHelpers.assertEquals;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Spark ORC reads of fields with {@code initial-default}. Defaulted projections use the row reader.
 * Nested-typed column defaults are not covered: {@code castDefault} rejects them.
 */
public class TestSparkOrcReaderForFieldsWithDefaultValue {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testOrcScalarDefaultValues() throws IOException {
    final int numRows = 10;

    final InternalRow expectedFirstRow = new GenericInternalRow(2);
    expectedFirstRow.update(0, 0);
    expectedFirstRow.update(1, UTF8String.fromString("foo"));

    // Write with Iceberg-embedded field ids (production ORC path). Bare ORC schemas without ids
    // take the name-mapped path, which does not omit for defaults.
    Schema writeSchema = new Schema(Types.NestedField.required(1, "col1", Types.IntegerType.get()));
    TypeDescription orcSchema = ORCSchemaUtil.convert(writeSchema);

    Schema readSchema =
        new Schema(
            Types.NestedField.required(1, "col1", Types.IntegerType.get()),
            Types.NestedField.optional("col2")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("foo"))
                .build());

    File orcFile = writeOrcWithIntColumn(orcSchema, numRows);

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(orcFile))
            .project(readSchema)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema))
            .supportsInitialDefaults()
            .build()) {
      InternalRow actualFirstRow = reader.iterator().next();
      assertEquals(readSchema, expectedFirstRow, actualFirstRow);
    }
  }

  @Test
  public void testOrcNestedScalarDefaultValues() throws IOException {
    // Parent struct `loc` is present in the file; child `country` is new with an initial-default.
    // If the parent itself is absent, the synthetic null parent short-circuits child constants.
    final int numRows = 10;

    final InternalRow expectedLoc = new GenericInternalRow(1);
    expectedLoc.update(0, UTF8String.fromString("US"));
    final InternalRow expectedFirstRow = new GenericInternalRow(2);
    expectedFirstRow.update(0, 0L);
    expectedFirstRow.update(1, expectedLoc);

    // Empty loc struct in the file: country is absent and will be filled from initial-default.
    // Use convert() so the file carries embedded field ids (required to omit for defaults).
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("loc").withId(2).ofType(Types.StructType.of()).build());
    TypeDescription orcSchema = ORCSchemaUtil.convert(writeSchema);

    Schema readSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("loc")
                .withId(2)
                .ofType(
                    Types.StructType.of(
                        Types.NestedField.optional("country")
                            .withId(3)
                            .ofType(Types.StringType.get())
                            .withInitialDefault(Expressions.lit("US"))
                            .build()))
                .build());

    File orcFile = writeOrcWithIdAndEmptyLoc(orcSchema, numRows);

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(orcFile))
            .project(readSchema)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema))
            .supportsInitialDefaults()
            .build()) {
      InternalRow actualFirstRow = reader.iterator().next();
      assertEquals(readSchema, expectedFirstRow, actualFirstRow);
    }
  }

  @Test
  public void testPartialEmbeddedIdsDoNotFillDefault() throws IOException {
    // A file with some iceberg.id attributes takes the EMBEDDED path, but hasAllIds is false, so
    // an unannotated physical column must not be treated as absent and filled with the default.
    TypeDescription orcSchema = TypeDescription.fromString("struct<col1:int,col2:string>");
    orcSchema.getChildren().get(0).setAttribute("iceberg.id", "1");

    Schema readSchema =
        new Schema(
            Types.NestedField.required(1, "col1", Types.IntegerType.get()),
            Types.NestedField.optional("col2")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("foo"))
                .build());

    File orcFile = writeOrcWithIntAndString(orcSchema, 1, "CA");

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(orcFile))
            .project(readSchema)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema))
            .supportsInitialDefaults()
            .build()) {
      InternalRow row = reader.iterator().next();
      Assert.assertEquals(1, row.getInt(0));
      Assert.assertTrue(
          "unannotated physical column must not be replaced by the default", row.isNullAt(1));
    }
  }

  @Test
  public void testFilterOnOnlyOmittedDefaultDoesNotThrow() throws IOException {
    // Projecting and filtering only a defaulted column yields an empty ORC schema. Convert must
    // not throw; SARG is disabled (YES_NO_NULL). Row-level filtering is Spark's job.
    Schema writeSchema = new Schema(Types.NestedField.required(1, "col1", Types.IntegerType.get()));
    TypeDescription orcSchema = ORCSchemaUtil.convert(writeSchema);
    Schema readSchema =
        new Schema(
            Types.NestedField.optional("col2")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("foo"))
                .build());
    File orcFile = writeOrcWithIntColumn(orcSchema, 10);

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(orcFile))
            .project(readSchema)
            .filter(Expressions.equal("col2", "bar"))
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema))
            .supportsInitialDefaults()
            .build()) {
      Assert.assertEquals(10, Iterators.size(reader.iterator()));
    }
  }

  @Test
  public void testFilterOnOnlyOmittedNestedDefaultDoesNotThrow() throws IOException {
    // Projecting and filtering only loc.country leaves struct<loc:struct<>>. Convert must not
    // throw; SARG is disabled (YES_NO_NULL). Row-level filtering is Spark's job.
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("loc").withId(2).ofType(Types.StructType.of()).build());
    TypeDescription orcSchema = ORCSchemaUtil.convert(writeSchema);
    Schema readSchema =
        new Schema(
            Types.NestedField.optional("loc")
                .withId(2)
                .ofType(
                    Types.StructType.of(
                        Types.NestedField.optional("country")
                            .withId(3)
                            .ofType(Types.StringType.get())
                            .withInitialDefault(Expressions.lit("US"))
                            .build()))
                .build());
    File orcFile = writeOrcWithIdAndEmptyLoc(orcSchema, 10);

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(orcFile))
            .project(readSchema)
            .filter(Expressions.equal("loc.country", "bar"))
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema))
            .supportsInitialDefaults()
            .build()) {
      Assert.assertEquals(10, Iterators.size(reader.iterator()));
    }
  }

  private File writeOrcWithIntColumn(TypeDescription orcSchema, int numRows) throws IOException {
    Configuration conf = new Configuration();
    File orcFile = temp.newFile();
    Path orcFilePath = new Path(orcFile.getPath());

    Writer writer =
        OrcFile.createWriter(
            orcFilePath, OrcFile.writerOptions(conf).setSchema(orcSchema).overwrite(true));

    VectorizedRowBatch batch = orcSchema.createRowBatch();
    LongColumnVector firstCol = (LongColumnVector) batch.cols[0];
    for (int r = 0; r < numRows; ++r) {
      int row = batch.size++;
      firstCol.vector[row] = r;
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();
    return orcFile;
  }

  private File writeOrcWithIntAndString(TypeDescription orcSchema, int intValue, String stringValue)
      throws IOException {
    Configuration conf = new Configuration();
    File orcFile = temp.newFile();
    Path orcFilePath = new Path(orcFile.getPath());

    Writer writer =
        OrcFile.createWriter(
            orcFilePath, OrcFile.writerOptions(conf).setSchema(orcSchema).overwrite(true));

    VectorizedRowBatch batch = orcSchema.createRowBatch();
    LongColumnVector intCol = (LongColumnVector) batch.cols[0];
    org.apache.orc.storage.ql.exec.vector.BytesColumnVector strCol =
        (org.apache.orc.storage.ql.exec.vector.BytesColumnVector) batch.cols[1];
    int row = batch.size++;
    intCol.vector[row] = intValue;
    strCol.setVal(row, stringValue.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    writer.addRowBatch(batch);
    writer.close();
    return orcFile;
  }

  private File writeOrcWithIdAndEmptyLoc(TypeDescription orcSchema, int numRows)
      throws IOException {
    Configuration conf = new Configuration();
    File orcFile = temp.newFile();
    Path orcFilePath = new Path(orcFile.getPath());

    Writer writer =
        OrcFile.createWriter(
            orcFilePath, OrcFile.writerOptions(conf).setSchema(orcSchema).overwrite(true));

    VectorizedRowBatch batch = orcSchema.createRowBatch();
    LongColumnVector idCol = (LongColumnVector) batch.cols[0];
    org.apache.orc.storage.ql.exec.vector.StructColumnVector locCol =
        (org.apache.orc.storage.ql.exec.vector.StructColumnVector) batch.cols[1];
    for (int r = 0; r < numRows; ++r) {
      int row = batch.size++;
      idCol.vector[row] = r;
      // non-null empty loc struct
      locCol.noNulls = true;
      locCol.isNull[row] = false;
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();
    return orcFile;
  }
}
