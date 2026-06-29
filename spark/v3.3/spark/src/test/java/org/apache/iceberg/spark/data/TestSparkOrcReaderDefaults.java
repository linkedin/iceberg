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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that a top-level scalar {@code initial-default} is filled on ORC read, for both the row
 * and vectorized Spark readers (the 1.2.x positional readers both inject from {@code
 * idToConstant}).
 */
public class TestSparkOrcReaderDefaults {

  private static final Schema WRITE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  private static final Schema READ_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional("country")
              .withId(3)
              .ofType(Types.StringType.get())
              .withInitialDefault(Expressions.lit("US"))
              .build());

  private static final UTF8String EXPECTED_DEFAULT = UTF8String.fromString("US");

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private File writeFile() throws IOException {
    List<InternalRow> rows =
        Lists.newArrayList(
            new GenericInternalRow(new Object[] {1L, UTF8String.fromString("a")}),
            new GenericInternalRow(new Object[] {2L, UTF8String.fromString("b")}),
            new GenericInternalRow(new Object[] {3L, UTF8String.fromString("c")}));

    File testFile = temp.newFile();
    Assertions.assertThat(testFile.delete()).isTrue();
    try (FileAppender<InternalRow> writer =
        ORC.write(Files.localOutput(testFile))
            .createWriterFunc(SparkOrcWriter::new)
            .schema(WRITE_SCHEMA)
            .build()) {
      writer.addAll(rows);
    }
    return testFile;
  }

  @Test
  public void testRowReadFillsDefault() throws IOException {
    File testFile = writeFile();

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(testFile))
            .project(READ_SCHEMA)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(READ_SCHEMA, readOrcSchema))
            .build()) {
      int count = 0;
      for (InternalRow row : reader) {
        Assertions.assertThat(row.getUTF8String(2)).isEqualTo(EXPECTED_DEFAULT);
        count += 1;
      }
      Assertions.assertThat(count).isEqualTo(3);
    }
  }

  @Test
  public void testVectorizedReadFillsDefault() throws IOException {
    File testFile = writeFile();

    try (CloseableIterable<ColumnarBatch> reader =
        ORC.read(Files.localInput(testFile))
            .project(READ_SCHEMA)
            .createBatchedReaderFunc(
                readOrcSchema ->
                    VectorizedSparkOrcReaders.buildReader(
                        READ_SCHEMA, readOrcSchema, ImmutableMap.of()))
            .build()) {
      Iterator<InternalRow> rows = batchesToRows(reader.iterator());
      int count = 0;
      while (rows.hasNext()) {
        Assertions.assertThat(rows.next().getUTF8String(2)).isEqualTo(EXPECTED_DEFAULT);
        count += 1;
      }
      Assertions.assertThat(count).isEqualTo(3);
    }
  }

  private Iterator<InternalRow> batchesToRows(Iterator<ColumnarBatch> batches) {
    return Iterators.concat(Iterators.transform(batches, ColumnarBatch::rowIterator));
  }
}
