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

package org.apache.iceberg.hivelink.core;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.hivelink.core.utils.MappingUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public class TestReadFileWithCaseMismatch {

  @Parameterized.Parameters(name = "format = {0}")
  public static Object[] parameters() {
    return new Object[] { "avro", "orc" };
  }

  private final FileFormat format;

  public TestReadFileWithCaseMismatch(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void writeAndValidateFileWithLowercaseFields() throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    switch (format) {
      case AVRO:
        org.apache.avro.Schema avroSchema = SchemaBuilder.record("root").fields()
            .name("fieldone").type().optional().record("fieldone").fields()
            .optionalString("innerfield")
            .endRecord()
            .optionalLong("fieldtwo")
            .optionalLong("lowercasefield")
            .endRecord();

        org.apache.avro.Schema fieldoneSchema = avroSchema.getField("fieldone").schema().getTypes().get(1);
        GenericData.Record fieldoneRecord = new GenericData.Record(fieldoneSchema);
        fieldoneRecord.put("innerfield", "1");
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("fieldone", fieldoneRecord);
        record.put("fieldtwo", 2L);
        record.put("lowercasefield", 3L);

        try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
          writer.create(avroSchema, testFile);
          writer.append(record);
        }
        return;
      case ORC:
        TypeDescription writerSchema = TypeDescription.fromString(
            "struct<fieldone:struct<innerfield:string>,fieldtwo:bigint,lowercasefield:bigint>");
        try (CloseableOrcWriterBatch writer = new CloseableOrcWriterBatch(testFile, writerSchema)) {
          VectorizedRowBatch batch = writer.batch;
          ((BytesColumnVector) ((StructColumnVector) batch.cols[0]).fields[0])
              .setVal(0, "1".getBytes(StandardCharsets.UTF_8));
          ((LongColumnVector) batch.cols[1]).vector[0] = 2L;
          ((LongColumnVector) batch.cols[2]).vector[0] = 3L;
        }
        return;
    }

    Schema tableSchema = new Schema(
        optional(1, "fieldOne", Types.StructType.of(
            optional(2, "innerField", Types.StringType.get()),
            optional(3, "extraField", Types.StringType.get())
        )),
        optional(4, "fieldTwo", Types.LongType.get()),
        optional(5, "lowercasefield", Types.LongType.get())
    );
    // Data should be readable using a case insensitive name mapping
    List<Record> rows = readRows(testFile, tableSchema, MappingUtil.create(tableSchema, false));
    Assert.assertEquals("1", ((Record) rows.get(0).getField("fieldOne")).getField("innerField"));
    Assert.assertNull(((Record) rows.get(0).getField("fieldOne")).getField("extraField"));
    Assert.assertEquals(2L, rows.get(0).getField("fieldTwo"));
    Assert.assertEquals(3L, rows.get(0).getField("lowercasefield"));
  }

  @Test
  public void writeAndValidateFileWithMultipleCandidatesInSchema() throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    switch (format) {
      case AVRO:
        org.apache.avro.Schema avroSchema = SchemaBuilder.record("root").fields()
            .name("outer").type().optional().record("outer").fields()
            .optionalString("inner")
            .endRecord()
            .endRecord();

        org.apache.avro.Schema outerSchema = avroSchema.getField("outer").schema().getTypes().get(1);
        GenericData.Record outerRecord = new GenericData.Record(outerSchema);
        outerRecord.put("inner", "1");
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("outer", outerRecord);
        try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
          writer.create(avroSchema, testFile);
          writer.append(record);
        }
        return;
      case ORC:
        TypeDescription writerSchema = TypeDescription.fromString("struct<outer:struct<inner:string>>");
        try (CloseableOrcWriterBatch writer = new CloseableOrcWriterBatch(testFile, writerSchema)) {
          VectorizedRowBatch batch = writer.batch;
          ((BytesColumnVector) ((StructColumnVector) batch.cols[0]).fields[0])
              .setVal(0, "1".getBytes(StandardCharsets.UTF_8));
        }
        return;
    }

    Schema tableSchema = new Schema(
        optional(1, "outer", Types.StructType.of(
            optional(2, "Inner", Types.StringType.get()),
            optional(3, "INNER", Types.StringType.get())
        ))
    );
    // When the file has a field with multiple candidates fields in the table schema, the candidate which exactly
    // matches the case is selected. If no such candidate is available, the selection of candidate is
    // undefined. Based on the current implementation, the first candidate in the table schema by index will be picked
    // e.g. Here "inner" has two candidates in table schema "Inner" and "INNER", it will map to "Inner"
    List<Record> rows = readRows(testFile, tableSchema, MappingUtil.create(tableSchema, false));
    Assert.assertEquals("1", ((Record) rows.get(0).getField("outer")).getField("Inner"));
    Assert.assertNull(((Record) rows.get(0).getField("outer")).getField("INNER"));

    tableSchema = new Schema(
        optional(1, "outer", Types.StructType.of(
            optional(2, "Inner", Types.StringType.get()),
            optional(3, "INNER", Types.StringType.get()),
            optional(4, "inner", Types.StringType.get())
        ))
    );
    // If there is a candidate which exactly matches the case, then it should be selected
    // e.g. Here "inner" has three candidates in table schema "Inner", "INNER", and "inner", it will map to "inner"
    rows = readRows(testFile, tableSchema, MappingUtil.create(tableSchema, false));
    Assert.assertNull(((Record) rows.get(0).getField("outer")).getField("Inner"));
    Assert.assertNull(((Record) rows.get(0).getField("outer")).getField("INNER"));
    Assert.assertEquals("1", ((Record) rows.get(0).getField("outer")).getField("inner"));

    // Even if we our read schema (projection) contains only one of the candidate fields from the table schema
    // the mapping should be done based on the table schema
    // e.g. Here "inner" has three candidates in table schema "Inner", "INNER", and "inner",
    // but the user requested projection is only "INNER", the field will still map to "inner"
    rows = readRows(testFile, tableSchema.select("outer.INNER"), MappingUtil.create(tableSchema, false));
    Assert.assertNull(((Record) rows.get(0).getField("outer")).getField("INNER"));
  }

  @Test
  public void writeAndValidateMultipleCandidatesInFile() throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    switch (format) {
      case AVRO:
        org.apache.avro.Schema avroSchema = SchemaBuilder.record("root").fields()
            .name("outer").type().optional().record("outer").fields()
            .optionalString("Inner")
            .optionalString("INNER")
            .endRecord()
            .endRecord();

        org.apache.avro.Schema outerSchema = avroSchema.getField("outer").schema().getTypes().get(1);
        GenericData.Record outerRecord = new GenericData.Record(outerSchema);
        outerRecord.put("Inner", "1");
        outerRecord.put("INNER", "2");
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("outer", outerRecord);
        try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
          writer.create(avroSchema, testFile);
          writer.append(record);
        }
        return;
      case ORC:
        TypeDescription writerSchema = TypeDescription.fromString("struct<outer:struct<Inner:string,INNER:string>>");
        try (CloseableOrcWriterBatch writer = new CloseableOrcWriterBatch(testFile, writerSchema)) {
          VectorizedRowBatch batch = writer.batch;
          ((BytesColumnVector) ((StructColumnVector) batch.cols[0]).fields[0])
              .setVal(0, "1".getBytes(StandardCharsets.UTF_8));
          ((BytesColumnVector) ((StructColumnVector) batch.cols[0]).fields[1])
              .setVal(0, "2".getBytes(StandardCharsets.UTF_8));
        }
        return;
    }

    Schema tableSchema = new Schema(
        optional(1, "outer", Types.StructType.of(
            optional(2, "Inner", Types.StringType.get())
        ))
    );
    // When the file has two fields with the same name when lowercased, both can get mapped to the same field in the
    // table schema, if there isn't an exact match for both. Which of the candidates eventually gets mapped is
    // undefined. e.g. here two fields "Inner" and "INNER" can be mapped insensitively to "Inner" in table schema
    // Based on the current implementation, the last candidate from the file gets picked
    // so here "INNER" from file maps to "Inner" in table
    List<Record> rows = readRows(testFile, tableSchema, MappingUtil.create(tableSchema, false));
    Assert.assertEquals("2", ((Record) rows.get(0).getField("outer")).getField("Inner"));
  }

  @Test
  public void writeAndValidateDuplicateLowercaseFieldsInFile() throws IOException {
    // Duplicate field names are not possible in Avro
    Assume.assumeTrue(format == FileFormat.ORC);
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    TypeDescription writerSchema = TypeDescription.fromString(
        "struct<outer:struct<inner:string,inner:string>>");
    try (CloseableOrcWriterBatch writer = new CloseableOrcWriterBatch(testFile, writerSchema)) {
      VectorizedRowBatch batch = writer.batch;
      ((BytesColumnVector) ((StructColumnVector) batch.cols[0]).fields[0])
          .setVal(0, "1".getBytes(StandardCharsets.UTF_8));
      ((BytesColumnVector) ((StructColumnVector) batch.cols[0]).fields[1])
          .setVal(0, "2".getBytes(StandardCharsets.UTF_8));
    }

    Schema tableSchema = new Schema(
        optional(1, "outer", Types.StructType.of(
            optional(2, "Inner", Types.StringType.get())
        ))
    );
    // When the file has two fields with the same name and case, both can get mapped to the same field in the table
    // schema. Which of the candidates eventually gets mapped is undefined. Based on the current implementation,
    // the last candidate in the file schema by index will be picked
    List<Record> rows = readRows(testFile, tableSchema, MappingUtil.create(tableSchema, false));
    Assert.assertEquals("2", ((Record) rows.get(0).getField("outer")).getField("Inner"));
  }

  private List<Record> readRows(File inputFile, Schema readSchema, NameMapping nameMapping) throws IOException {
    switch (format) {
      case AVRO:
        try (CloseableIterable<Record> reader = Avro.read(Files.localInput(inputFile))
            .project(readSchema)
            .createReaderFunc(fileSchema -> DataReader.create(readSchema, fileSchema))
            .withNameMapping(nameMapping)
            .build()) {
          return Lists.newArrayList(reader);
        }
      case ORC:
        try (CloseableIterable<Record> reader = ORC.read(Files.localInput(inputFile))
            .project(readSchema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema))
            .withNameMapping(nameMapping)
            .build()) {
          return Lists.newArrayList(reader);
        }
      default:
        throw new UnsupportedOperationException("File format: " + format + " not supported.");
    }
  }

  private static class CloseableOrcWriterBatch implements Closeable {

    private final Writer writer;
    private final VectorizedRowBatch batch;

    CloseableOrcWriterBatch(File outputFile, TypeDescription schema) throws IOException {
      this.writer = OrcFile.createWriter(new Path(outputFile.toString()),
          OrcFile.writerOptions(new Configuration())
              .setSchema(schema));
      this.batch = schema.createRowBatch();
      batch.ensureSize(1);
      batch.size = 1;
    }

    @Override
    public void close() throws IOException {
      writer.addRowBatch(batch);
      writer.close();
    }
  }
}
