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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.UnionColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.spark.data.TestHelpers.assertEquals;


public class TestSparkOrcUnions {
  private static final int NUM_OF_ROWS = 50;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testComplexUnion() throws IOException {
    TypeDescription orcSchema =
        TypeDescription.fromString("struct<unionCol:uniontype<int,string>>");

    Schema expectedSchema = new Schema(
        Types.NestedField.optional(0, "unionCol", Types.StructType.of(
            Types.NestedField.optional(1, "tag_0", Types.IntegerType.get()),
            Types.NestedField.optional(2, "tag_1", Types.StringType.get())))
    );

    final InternalRow expectedFirstRow = new GenericInternalRow(1);
    final InternalRow field1 = new GenericInternalRow(2);
    field1.update(0, 0);
    field1.update(1, null);
    expectedFirstRow.update(0, field1);

    final InternalRow expectedSecondRow = new GenericInternalRow(1);
    final InternalRow field2 = new GenericInternalRow(2);
    field2.update(0, null);
    field2.update(1, UTF8String.fromString("stringtype1"));
    expectedSecondRow.update(0, field2);

    Configuration conf = new Configuration();

    File orcFile = temp.newFile();
    Path orcFilePath = new Path(orcFile.getPath());

    Writer writer = OrcFile.createWriter(orcFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(orcSchema).overwrite(true));

    VectorizedRowBatch batch = orcSchema.createRowBatch();
    LongColumnVector longColumnVector = new LongColumnVector(NUM_OF_ROWS);
    BytesColumnVector bytesColumnVector = new BytesColumnVector(NUM_OF_ROWS);
    UnionColumnVector complexUnion = new UnionColumnVector(NUM_OF_ROWS, longColumnVector, bytesColumnVector);

    complexUnion.init();

    for (int i = 0; i < NUM_OF_ROWS; i += 1) {
      complexUnion.tags[i] = i % 2;
      longColumnVector.vector[i] = i;
      String stringValue = "stringtype" + i;
      bytesColumnVector.setVal(i, stringValue.getBytes(StandardCharsets.UTF_8));
    }

    batch.size = NUM_OF_ROWS;
    batch.cols[0] = complexUnion;

    writer.addRowBatch(batch);
    batch.reset();
    writer.close();

    // Test non-vectorized reader
    List<InternalRow> internalRows = Lists.newArrayList();
    try (CloseableIterable<InternalRow> reader = ORC.read(Files.localInput(orcFile))
        .project(expectedSchema)
        .createReaderFunc(readOrcSchema -> new SparkOrcReader(expectedSchema, readOrcSchema))
        .build()) {
      reader.forEach(internalRows::add);

      Assert.assertEquals(internalRows.size(), NUM_OF_ROWS);
      assertEquals(expectedSchema, expectedFirstRow, internalRows.get(0));
      assertEquals(expectedSchema, expectedSecondRow, internalRows.get(1));
    }

    // Test vectorized reader
    List<ColumnarBatch> columnarBatches = Lists.newArrayList();
    try (CloseableIterable<ColumnarBatch> reader = ORC.read(Files.localInput(orcFile))
        .project(expectedSchema)
        .createBatchedReaderFunc(readOrcSchema ->
            VectorizedSparkOrcReaders.buildReader(expectedSchema, readOrcSchema, ImmutableMap.of()))
        .build()) {
      reader.forEach(columnarBatches::add);
      Iterator<InternalRow> rowIterator = columnarBatches.get(0).rowIterator();

      Assert.assertEquals(columnarBatches.get(0).numRows(), NUM_OF_ROWS);
      assertEquals(expectedSchema, expectedFirstRow, rowIterator.next());
      assertEquals(expectedSchema, expectedSecondRow, rowIterator.next());
    }
  }

  @Test
  public void testSingleComponentUnion() throws IOException {
    TypeDescription orcSchema =
        TypeDescription.fromString("struct<unionCol:uniontype<int>>");

    Schema expectedSchema = new Schema(
        Types.NestedField.optional(0, "unionCol", Types.StructType.of(
            Types.NestedField.optional(1, "tag_0", Types.IntegerType.get())))
    );

    final InternalRow expectedFirstRow = new GenericInternalRow(1);
    final InternalRow field1 = new GenericInternalRow(1);
    field1.update(0, 0);
    expectedFirstRow.update(0, field1);

    final InternalRow expectedSecondRow = new GenericInternalRow(1);
    final InternalRow field2 = new GenericInternalRow(1);
    field2.update(0, 3);
    expectedSecondRow.update(0, field2);

    Configuration conf = new Configuration();

    File orcFile = temp.newFile();
    Path orcFilePath = new Path(orcFile.getPath());

    Writer writer = OrcFile.createWriter(orcFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(orcSchema).overwrite(true));

    VectorizedRowBatch batch = orcSchema.createRowBatch();
    LongColumnVector longColumnVector = new LongColumnVector(NUM_OF_ROWS);
    UnionColumnVector complexUnion = new UnionColumnVector(NUM_OF_ROWS, longColumnVector);
    complexUnion.init();

    for (int i = 0; i < NUM_OF_ROWS; i += 1) {
      complexUnion.tags[i] = 0;
      longColumnVector.vector[i] = 3 * i;
    }

    batch.size = NUM_OF_ROWS;
    batch.cols[0] = complexUnion;

    writer.addRowBatch(batch);
    batch.reset();
    writer.close();

    // Test non-vectorized reader
    List<InternalRow> internalRows = Lists.newArrayList();
    try (CloseableIterable<InternalRow> reader = ORC.read(Files.localInput(orcFile))
        .project(expectedSchema)
        .createReaderFunc(readOrcSchema -> new SparkOrcReader(expectedSchema, readOrcSchema))
        .build()) {
      reader.forEach(internalRows::add);

      Assert.assertEquals(internalRows.size(), NUM_OF_ROWS);
      assertEquals(expectedSchema, expectedFirstRow, internalRows.get(0));
      assertEquals(expectedSchema, expectedSecondRow, internalRows.get(1));
    }

    // Test vectorized reader
    List<ColumnarBatch> columnarBatches = Lists.newArrayList();
    try (CloseableIterable<ColumnarBatch> reader = ORC.read(Files.localInput(orcFile))
        .project(expectedSchema)
        .createBatchedReaderFunc(readOrcSchema ->
            VectorizedSparkOrcReaders.buildReader(expectedSchema, readOrcSchema, ImmutableMap.of()))
        .build()) {
      reader.forEach(columnarBatches::add);
      Iterator<InternalRow> rowIterator = columnarBatches.get(0).rowIterator();

      Assert.assertEquals(columnarBatches.get(0).numRows(), NUM_OF_ROWS);
      assertEquals(expectedSchema, expectedFirstRow, rowIterator.next());
      assertEquals(expectedSchema, expectedSecondRow, rowIterator.next());
    }
  }
}
