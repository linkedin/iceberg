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
import java.util.Arrays;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.spark.data.TestHelpers.assertEquals;


public class TestSparkOrcReaderForFieldsWithDefaultValue {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testOrcDefaultValues() throws IOException {
    final int numRows = 10;

    final InternalRow expectedFirstRow = new GenericInternalRow(6);
    expectedFirstRow.update(0, 0);
    expectedFirstRow.update(1, UTF8String.fromString("foo"));
    expectedFirstRow.update(2, new GenericArrayData(ImmutableList.of(1, 2).toArray()));
    expectedFirstRow.update(3, new ArrayBasedMapData(
        new GenericArrayData(Arrays.asList(UTF8String.fromString("foo"))),
        new GenericArrayData(Arrays.asList(1))));

    final InternalRow nestedStructData = new GenericInternalRow(1);
    nestedStructData.update(0, 1);
    expectedFirstRow.update(4, nestedStructData);

    // test deeply nested data: array of structs
    final InternalRow deepNestedStructData1 = new GenericInternalRow(1);
    deepNestedStructData1.update(0, 1);
    final InternalRow deepNestedStructData2 = new GenericInternalRow(1);
    deepNestedStructData2.update(0, 2);
    expectedFirstRow.update(5,
        new GenericArrayData(ImmutableList.of(deepNestedStructData1, deepNestedStructData2).toArray()));

    TypeDescription orcSchema =
            TypeDescription.fromString("struct<col1:int>");

    Schema readSchema = new Schema(
            Types.NestedField.required(1, "col1", Types.IntegerType.get()),
            Types.NestedField.required(2, "col2", Types.StringType.get(), "foo", null),
            Types.NestedField.required(3, "col3", Types.ListType.ofRequired(10, Types.IntegerType.get()),
                ImmutableList.of(1, 2), null),
            Types.NestedField.required(4, "col4", Types.MapType.ofRequired(11, 12, Types.StringType.get(),
                Types.IntegerType.get()), ImmutableMap.of("foo", 1), null),
            Types.NestedField.required(5, "col5", Types.StructType.of(
                Types.NestedField.required(13, "nested_col1", Types.IntegerType.get())),
                ImmutableMap.of("nested_col1", 1), null),
            Types.NestedField.required(6, "col6", Types.ListType.ofRequired(14, Types.StructType.of(
                Types.NestedField.required(15, "nested_col2", Types.IntegerType.get()))),
                ImmutableList.of(ImmutableMap.of("nested_col2", 1), ImmutableMap.of("nested_col2", 2)), null)
    );

    Configuration conf = new Configuration();

    File orcFile = temp.newFile();
    Path orcFilePath = new Path(orcFile.getPath());

    Writer writer = OrcFile.createWriter(orcFilePath,
            OrcFile.writerOptions(conf).setSchema(orcSchema).overwrite(true));

    VectorizedRowBatch batch = orcSchema.createRowBatch();
    LongColumnVector firstCol = (LongColumnVector) batch.cols[0];
    for (int r = 0; r < numRows; ++r) {
      int row = batch.size++;
      firstCol.vector[row] = r;
      // If the batch is full, write it out and start over.
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

    // try to read the data using the readSchema, which is an evolved
    // schema that contains a new column with default value

    // non-vectorized read
    try (CloseableIterable<InternalRow> reader = ORC.read(Files.localInput(orcFile))
            .project(readSchema)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema))
            .build()) {
      final Iterator<InternalRow> actualRows = reader.iterator();
      final InternalRow actualFirstRow = actualRows.next();

      assertEquals(readSchema, expectedFirstRow, actualFirstRow);
    }

    // vectorized-read
    try (CloseableIterable<ColumnarBatch> reader = ORC.read(Files.localInput(orcFile))
        .project(readSchema)
        .createBatchedReaderFunc(readOrcSchema ->
            VectorizedSparkOrcReaders.buildReader(readSchema, readOrcSchema, ImmutableMap.of()))
        .build()) {
      final Iterator<InternalRow> actualRows = batchesToRows(reader.iterator());
      final InternalRow actualFirstRow = actualRows.next();

      assertEquals(readSchema, expectedFirstRow, actualFirstRow);
    }
  }

  @Test
  public void testSelectionAndFilterWithDefaultValueColumnOnly() throws IOException {
    final int numRows = 10;

    final InternalRow expectedFirstRow = new GenericInternalRow(1);
    // expectedFirstRow.update(0, 0);
    expectedFirstRow.update(0, UTF8String.fromString("foo"));

    TypeDescription orcSchema =
        TypeDescription.fromString("struct<col1:int>");

    Schema readSchema = new Schema(
        Types.NestedField.required(2, "col2", Types.StringType.get(), "foo", null)
    );

    Configuration conf = new Configuration();

    File orcFile = temp.newFile();
    Path orcFilePath = new Path(orcFile.getPath());

    Writer writer = OrcFile.createWriter(orcFilePath,
        OrcFile.writerOptions(conf).setSchema(orcSchema).overwrite(true));

    VectorizedRowBatch batch = orcSchema.createRowBatch();
    LongColumnVector firstCol = (LongColumnVector) batch.cols[0];
    for (int r = 0; r < numRows; ++r) {
      int row = batch.size++;
      firstCol.vector[row] = r;
      // If the batch is full, write it out and start over.
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

    // try to read the data using the readSchema, which is an evolved
    // schema that contains a new column with default value

    // non-vectorized read
    try (CloseableIterable<InternalRow> reader = ORC.read(Files.localInput(orcFile))
        .project(readSchema)
        .filter(Expressions.equal("col2", "foo"))
        .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema))
        .build()) {
      final Iterator<InternalRow> actualRows = reader.iterator();
      final InternalRow actualFirstRow = actualRows.next();

      assertEquals(readSchema, expectedFirstRow, actualFirstRow);
    }

    // vectorized-read
    try (CloseableIterable<ColumnarBatch> reader = ORC.read(Files.localInput(orcFile))
        .project(readSchema)
        .filter(Expressions.equal("col2", "foo"))
        .createBatchedReaderFunc(readOrcSchema ->
            VectorizedSparkOrcReaders.buildReader(readSchema, readOrcSchema, ImmutableMap.of()))
        .build()) {
      final Iterator<InternalRow> actualRows = batchesToRows(reader.iterator());
      final InternalRow actualFirstRow = actualRows.next();

      assertEquals(readSchema, expectedFirstRow, actualFirstRow);
    }
  }

  private Iterator<InternalRow> batchesToRows(Iterator<ColumnarBatch> batches) {
    return Iterators.concat(Iterators.transform(batches, ColumnarBatch::rowIterator));
  }
}
