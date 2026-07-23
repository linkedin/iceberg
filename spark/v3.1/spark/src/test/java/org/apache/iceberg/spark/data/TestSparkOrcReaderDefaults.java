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
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that the row {@code SparkOrcReader} fills a scalar {@code initial-default} only when the
 * field declares one and is absent from a file with embedded field IDs. Vectorized ORC defaults are
 * a follow-up.
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
    return writeFile(WRITE_SCHEMA, rows);
  }

  private File writeFile(Schema schema, List<InternalRow> rows) throws IOException {
    File testFile = temp.newFile();
    Assertions.assertThat(testFile.delete()).isTrue();
    try (FileAppender<InternalRow> writer =
        ORC.write(Files.localOutput(testFile))
            .createWriterFunc(SparkOrcWriter::new)
            .schema(schema)
            .build()) {
      writer.addAll(rows);
    }
    return testFile;
  }

  @Test
  public void testFillsDefaultWhenFieldIsAbsent() throws IOException {
    File testFile = writeFile();

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(testFile))
            .project(READ_SCHEMA)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(READ_SCHEMA, readOrcSchema))
            .supportsInitialDefaults()
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
  public void testSynthesizesNullWhenAbsentFieldHasNoDefault() throws IOException {
    File testFile = writeFile();
    Schema schemaWithoutDefault =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "country", Types.StringType.get()));

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(testFile))
            .project(schemaWithoutDefault)
            .createReaderFunc(
                readOrcSchema -> new SparkOrcReader(schemaWithoutDefault, readOrcSchema))
            .build()) {
      int count = 0;
      for (InternalRow row : reader) {
        Assertions.assertThat(row.isNullAt(2)).isTrue();
        count += 1;
      }
      Assertions.assertThat(count).isEqualTo(3);
    }
  }

  @Test
  public void testFillsNestedDefaultWhenFieldIsAbsent() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2, "nested", Types.StructType.of(required(3, "value", Types.StringType.get()))));
    Schema readSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "nested",
                Types.StructType.of(
                    required(3, "value", Types.StringType.get()),
                    Types.NestedField.optional("missing")
                        .withId(4)
                        .ofType(Types.StringType.get())
                        .withInitialDefault(Expressions.lit("filled"))
                        .build())));
    InternalRow nested = new GenericInternalRow(new Object[] {UTF8String.fromString("present")});
    File testFile =
        writeFile(
            writeSchema, Lists.newArrayList(new GenericInternalRow(new Object[] {1L, nested})));

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(testFile))
            .project(readSchema)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema))
            .supportsInitialDefaults()
            .build()) {
      List<InternalRow> rows = Lists.newArrayList(reader);
      Assertions.assertThat(rows).hasSize(1);
      InternalRow readNested = rows.get(0).getStruct(1, 2);
      Assertions.assertThat(readNested.getUTF8String(0))
          .isEqualTo(UTF8String.fromString("present"));
      Assertions.assertThat(readNested.getUTF8String(1)).isEqualTo(UTF8String.fromString("filled"));
    }
  }
}
