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
package org.apache.iceberg.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Verifies that scalar {@code initial-default}s are filled on ORC read. */
public class TestOrcDefaultValues {

  private static final Schema WRITE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  // Evolved: adds a top-level scalar with an initial-default that is absent from the written file.
  private static final Schema READ_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional("country")
              .withId(3)
              .ofType(Types.StringType.get())
              .withInitialDefault(Expressions.lit("US"))
              .build());

  private List<Record> records;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void createRecords() {
    GenericRecord record = GenericRecord.create(WRITE_SCHEMA);
    records =
        Lists.newArrayList(
            record.copy(ImmutableMap.of("id", 1L, "data", "a")),
            record.copy(ImmutableMap.of("id", 2L, "data", "b")),
            record.copy(ImmutableMap.of("id", 3L, "data", "c")));
  }

  private OutputFile writeFile() throws IOException {
    OutputFile file = Files.localOutput(temp.newFile());
    DataWriter<Record> writer =
        ORC.writeData(file)
            .schema(WRITE_SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try {
      for (Record record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
    return file;
  }

  @Test
  public void testReadFillsTopLevelScalarDefault() throws IOException {
    OutputFile file = writeFile();

    List<Record> read;
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(READ_SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(READ_SCHEMA, fileSchema))
            .supportsInitialDefaults()
            .build()) {
      read = Lists.newArrayList(reader);
    }

    Assertions.assertThat(read).hasSize(records.size());
    for (int i = 0; i < read.size(); i += 1) {
      Assertions.assertThat(read.get(i).getField("id")).isEqualTo(records.get(i).getField("id"));
      Assertions.assertThat(read.get(i).getField("data"))
          .isEqualTo(records.get(i).getField("data"));
      Assertions.assertThat(read.get(i).getField("country")).isEqualTo("US");
    }
  }

  @Test
  public void testReadSelectsOnlyDefaultColumn() throws IOException {
    OutputFile file = writeFile();

    Schema onlyDefault =
        new Schema(
            Types.NestedField.optional("country")
                .withId(3)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("US"))
                .build());

    List<Record> read;
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(onlyDefault)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(onlyDefault, fileSchema))
            .supportsInitialDefaults()
            .build()) {
      read = Lists.newArrayList(reader);
    }

    Assertions.assertThat(read).hasSize(records.size());
    for (Record record : read) {
      Assertions.assertThat(record.getField("country")).isEqualTo("US");
    }
  }

  @Test
  public void testReadFillsScalarDefaultsAllTypes() throws IOException {
    OutputFile file = writeFile();

    Schema typed =
        new Schema(
            required(1, "id", Types.LongType.get()),
            defaulted(10, "b", Types.BooleanType.get(), Expressions.lit(true)),
            defaulted(11, "i", Types.IntegerType.get(), Expressions.lit(42)),
            defaulted(12, "l", Types.LongType.get(), Expressions.lit(100L)),
            defaulted(13, "f", Types.FloatType.get(), Expressions.lit(1.5f)),
            defaulted(14, "d", Types.DoubleType.get(), Expressions.lit(2.5d)),
            defaulted(15, "s", Types.StringType.get(), Expressions.lit("x")),
            defaulted(
                16, "dec", Types.DecimalType.of(9, 2), Expressions.lit(new BigDecimal("1.50"))));

    List<Record> read;
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(typed)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(typed, fileSchema))
            .supportsInitialDefaults()
            .build()) {
      read = Lists.newArrayList(reader);
    }

    Assertions.assertThat(read).hasSize(records.size());
    for (Record record : read) {
      Assertions.assertThat(record.getField("b")).isEqualTo(true);
      Assertions.assertThat(record.getField("i")).isEqualTo(42);
      Assertions.assertThat(record.getField("l")).isEqualTo(100L);
      Assertions.assertThat(record.getField("f")).isEqualTo(1.5f);
      Assertions.assertThat(record.getField("d")).isEqualTo(2.5d);
      Assertions.assertThat(record.getField("s")).isEqualTo("x");
      Assertions.assertThat(record.getField("dec")).isEqualTo(new BigDecimal("1.50"));
    }
  }

  @Test
  public void testReadFillsRequiredScalarDefault() throws IOException {
    OutputFile file = writeFile();

    // A required field absent from the file but declaring a default must be filled, not rejected.
    Schema requiredDefault =
        new Schema(
            required(1, "id", Types.LongType.get()),
            Types.NestedField.required("code")
                .withId(20)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(Expressions.lit(7))
                .build());

    List<Record> read;
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(requiredDefault)
            .createReaderFunc(
                fileSchema -> GenericOrcReader.buildReader(requiredDefault, fileSchema))
            .supportsInitialDefaults()
            .build()) {
      read = Lists.newArrayList(reader);
    }

    Assertions.assertThat(read).hasSize(records.size());
    for (Record record : read) {
      Assertions.assertThat(record.getField("code")).isEqualTo(7);
    }
  }

  @Test
  public void testReadDoesNotApplyDefaultToIdLessFile() throws IOException {
    File file = writeIdLessFile();

    List<Record> read;
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(file))
            .project(READ_SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(READ_SCHEMA, fileSchema))
            .supportsInitialDefaults()
            .build()) {
      read = Lists.newArrayList(reader);
    }

    Assertions.assertThat(read).hasSize(records.size());
    for (int i = 0; i < read.size(); i += 1) {
      Assertions.assertThat(read.get(i).getField("id")).isEqualTo(records.get(i).getField("id"));
      Assertions.assertThat(read.get(i).getField("data"))
          .isEqualTo(records.get(i).getField("data"));
      Assertions.assertThat(read.get(i).getField("country")).isNull();
    }
  }

  @Test
  public void testReadDoesNotOverridePresentColumn() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "country", Types.StringType.get()));
    Record present = GenericRecord.create(writeSchema);
    present.setField("id", 1L);
    present.setField("data", "a");
    present.setField("country", "CA");
    Record presentNull = GenericRecord.create(writeSchema);
    presentNull.setField("id", 2L);
    presentNull.setField("data", "b");
    presentNull.setField("country", null);

    OutputFile file = writeRecords(writeSchema, Lists.newArrayList(present, presentNull));
    List<Record> read = read(file, READ_SCHEMA);

    Assertions.assertThat(read).hasSize(2);
    Assertions.assertThat(read.get(0).getField("country")).isEqualTo("CA");
    Assertions.assertThat(read.get(1).getField("country")).isNull();
  }

  @Test
  public void testNestedStructScalarDefault() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                3, "nested", Types.StructType.of(required(4, "inner", Types.StringType.get()))));
    Types.StructType writeNested = writeSchema.findField("nested").type().asStructType();

    List<Record> recs = Lists.newArrayList();
    for (int i = 0; i < 3; i += 1) {
      Record nested = GenericRecord.create(writeNested);
      nested.setField("inner", "v" + i);
      Record rec = GenericRecord.create(writeSchema);
      rec.setField("id", (long) i);
      rec.setField("nested", nested);
      recs.add(rec);
    }
    // Row with a null nested struct: a default must not be fabricated when the parent struct is
    // null (the struct stays null; the absent-only fill applies to present structs).
    Record nullNested = GenericRecord.create(writeSchema);
    nullNested.setField("id", 3L);
    nullNested.setField("nested", null);
    recs.add(nullNested);

    OutputFile file = writeRecords(writeSchema, recs);

    Schema readSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                3,
                "nested",
                Types.StructType.of(
                    required(4, "inner", Types.StringType.get()),
                    defaulted(5, "missing", Types.FloatType.get(), Expressions.lit(-0.0F)))));

    List<Record> read = read(file, readSchema);
    Assertions.assertThat(read).hasSize(recs.size());
    for (int i = 0; i < 3; i += 1) {
      Record nested = (Record) read.get(i).getField("nested");
      Assertions.assertThat(nested.getField("inner")).isEqualTo("v" + i);
      Assertions.assertThat(nested.getField("missing")).isEqualTo(-0.0F);
    }
    Assertions.assertThat(read.get(3).getField("nested")).isNull();
  }

  @Test
  public void testMapNestedScalarDefault() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                3,
                "m",
                Types.MapType.ofOptional(
                    4,
                    5,
                    Types.StringType.get(),
                    Types.StructType.of(required(6, "v_str", Types.StringType.get())))));
    Types.StructType writeValue =
        writeSchema.findField("m").type().asMapType().valueType().asStructType();

    Record value = GenericRecord.create(writeValue);
    value.setField("v_str", "s");
    Record rec = GenericRecord.create(writeSchema);
    rec.setField("id", 1L);
    rec.setField("m", Collections.singletonMap("k", value));
    OutputFile file = writeRecords(writeSchema, Lists.newArrayList(rec));

    Schema readSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                3,
                "m",
                Types.MapType.ofOptional(
                    4,
                    5,
                    Types.StringType.get(),
                    Types.StructType.of(
                        required(6, "v_str", Types.StringType.get()),
                        defaulted(7, "v_int", Types.IntegerType.get(), Expressions.lit(34))))));

    List<Record> read = read(file, readSchema);
    Assertions.assertThat(read).hasSize(1);
    Map<?, ?> map = (Map<?, ?>) read.get(0).getField("m");
    Assertions.assertThat(map).hasSize(1);
    Record readValue = (Record) map.values().iterator().next();
    Assertions.assertThat(readValue.getField("v_str")).isEqualTo("s");
    Assertions.assertThat(readValue.getField("v_int")).isEqualTo(34);
  }

  @Test
  public void testListNestedScalarDefault() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                3,
                "l",
                Types.ListType.ofOptional(
                    4, Types.StructType.of(required(5, "e_str", Types.StringType.get())))));
    Types.StructType writeElement =
        writeSchema.findField("l").type().asListType().elementType().asStructType();

    Record element = GenericRecord.create(writeElement);
    element.setField("e_str", "e");
    Record rec = GenericRecord.create(writeSchema);
    rec.setField("id", 1L);
    rec.setField("l", Collections.singletonList(element));
    OutputFile file = writeRecords(writeSchema, Lists.newArrayList(rec));

    Schema readSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                3,
                "l",
                Types.ListType.ofOptional(
                    4,
                    Types.StructType.of(
                        required(5, "e_str", Types.StringType.get()),
                        defaulted(7, "e_int", Types.IntegerType.get(), Expressions.lit(34))))));

    List<Record> read = read(file, readSchema);
    Assertions.assertThat(read).hasSize(1);
    List<?> list = (List<?>) read.get(0).getField("l");
    Assertions.assertThat(list).hasSize(1);
    Record readElement = (Record) list.get(0);
    Assertions.assertThat(readElement.getField("e_str")).isEqualTo("e");
    Assertions.assertThat(readElement.getField("e_int")).isEqualTo(34);
  }

  @Test
  public void testNestedStructAllSubfieldsDefaulted() throws IOException {
    // File: nested { a }. Read projects only a new defaulted subfield nested { b default 'x' }
    // (a dropped), so the nested read struct is empty. The default must still fill.
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "nested", Types.StructType.of(required(4, "a", Types.LongType.get()))));
    Types.StructType writeNested = writeSchema.findField("nested").type().asStructType();

    Record nested = GenericRecord.create(writeNested);
    nested.setField("a", 9L);
    Record rec = GenericRecord.create(writeSchema);
    rec.setField("id", 1L);
    rec.setField("nested", nested);
    OutputFile file = writeRecords(writeSchema, Lists.newArrayList(rec));

    Schema readSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                3,
                "nested",
                Types.StructType.of(
                    defaulted(5, "b", Types.StringType.get(), Expressions.lit("x")))));

    List<Record> read = read(file, readSchema);
    Assertions.assertThat(read).hasSize(1);
    Record readNested = (Record) read.get(0).getField("nested");
    Assertions.assertThat(readNested.getField("b")).isEqualTo("x");
  }

  private OutputFile writeRecords(Schema schema, List<Record> recs) throws IOException {
    OutputFile file = Files.localOutput(temp.newFile());
    DataWriter<Record> writer =
        ORC.writeData(file)
            .schema(schema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try {
      for (Record rec : recs) {
        writer.write(rec);
      }
    } finally {
      writer.close();
    }
    return file;
  }

  private File writeIdLessFile() throws IOException {
    File file = temp.newFile();
    Assertions.assertThat(file.delete()).isTrue();
    TypeDescription writerSchema = TypeDescription.fromString("struct<id:bigint,data:string>");
    try (org.apache.orc.Writer writer =
        OrcFile.createWriter(
            new Path(file.toString()),
            OrcFile.writerOptions(new Configuration()).setSchema(writerSchema))) {
      VectorizedRowBatch batch = writerSchema.createRowBatch();
      LongColumnVector ids = (LongColumnVector) batch.cols[0];
      BytesColumnVector data = (BytesColumnVector) batch.cols[1];
      for (Record record : records) {
        int row = batch.size++;
        ids.vector[row] = (Long) record.getField("id");
        data.setVal(row, record.getField("data").toString().getBytes(StandardCharsets.UTF_8));
      }
      writer.addRowBatch(batch);
    }
    return file;
  }

  private List<Record> read(OutputFile file, Schema readSchema) throws IOException {
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(readSchema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema))
            .supportsInitialDefaults()
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  private static Types.NestedField defaulted(
      int id,
      String name,
      org.apache.iceberg.types.Type type,
      org.apache.iceberg.expressions.Literal<?> initial) {
    return Types.NestedField.optional(name)
        .withId(id)
        .ofType(type)
        .withInitialDefault(initial)
        .build();
  }
}
