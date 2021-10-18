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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class TestSparkAvroUnions {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void writeAndValidateRequiredComplexUnion() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .intType()
        .and()
        .stringType()
        .endUnion()
        .noDefault()
        .endRecord();

    GenericData.Record unionRecord1 = new GenericData.Record(avroSchema);
    unionRecord1.put("unionCol", "foo");
    GenericData.Record unionRecord2 = new GenericData.Record(avroSchema);
    unionRecord2.put("unionCol", 1);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(unionRecord1);
      writer.append(unionRecord2);
    }

    Schema expectedSchema = AvroSchemaUtil.toIceberg(avroSchema);

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
        .createReaderFunc(SparkAvroReader::new)
        .project(expectedSchema)
        .build()) {
      rows = Lists.newArrayList(reader);

      Assert.assertEquals("foo", rows.get(0).getStruct(0, 2).getString(1));
      Assert.assertEquals(1, rows.get(1).getStruct(0, 2).getInt(0));
    }
  }

  @Test
  public void writeAndValidateOptionalComplexUnion() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .nullType()
        .and()
        .intType()
        .and()
        .stringType()
        .endUnion()
        .nullDefault()
        .endRecord();

    GenericData.Record unionRecord1 = new GenericData.Record(avroSchema);
    unionRecord1.put("unionCol", "foo");
    GenericData.Record unionRecord2 = new GenericData.Record(avroSchema);
    unionRecord2.put("unionCol", 1);
    List<GenericData.Record> expectedRows = new ArrayList<>();

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(unionRecord1);
      writer.append(unionRecord2);
    }

    Schema expectedSchema = AvroSchemaUtil.toIceberg(avroSchema);

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
        .createReaderFunc(SparkAvroReader::new)
        .project(expectedSchema)
        .build()) {
      rows = Lists.newArrayList(reader);

      Assert.assertEquals("foo", rows.get(0).getStruct(0, 2).getString(1));
      Assert.assertEquals(1, rows.get(1).getStruct(0, 2).getInt(0));
    }
  }

  @Test
  public void writeAndValidateSingleTypeUnion() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .nullType()
        .and()
        .intType()
        .endUnion()
        .nullDefault()
        .endRecord();

    GenericData.Record unionRecord1 = new GenericData.Record(avroSchema);
    unionRecord1.put("unionCol", 0);
    GenericData.Record unionRecord2 = new GenericData.Record(avroSchema);
    unionRecord2.put("unionCol", 1);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(unionRecord1);
      writer.append(unionRecord2);
    }

    Schema expectedSchema = AvroSchemaUtil.toIceberg(avroSchema);

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
        .createReaderFunc(SparkAvroReader::new)
        .project(expectedSchema)
        .build()) {
      rows = Lists.newArrayList(reader);

      Assert.assertEquals(0, rows.get(0).getInt(0));
      Assert.assertEquals(1, rows.get(1).getInt(0));
    }
  }

  @Test
  public void testNestedComplexSchema() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("col1")
        .type()
        .array()
        .items()
        .unionOf()
        .nullType()
        .and()
        .intType()
        .and()
        .stringType()
        .endUnion()
        .noDefault()
        .endRecord();

    GenericData.Record unionRecord1 = new GenericData.Record(avroSchema);
    unionRecord1.put("col1", Arrays.asList("foo", 1));
    GenericData.Record unionRecord2 = new GenericData.Record(avroSchema);
    unionRecord2.put("col1",  Arrays.asList(2, "bar"));

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(unionRecord1);
      writer.append(unionRecord2);
    }

    Schema expectedSchema = AvroSchemaUtil.toIceberg(avroSchema);

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
        .createReaderFunc(SparkAvroReader::new)
        .project(expectedSchema)
        .build()) {
      rows = Lists.newArrayList(reader);
      System.out.println(rows);

      // making sure it reads the correctly nested structured data, which is array of union-structs
      Assert.assertEquals("foo", rows.get(0).getArray(0).getStruct(0, 2).getString(1));
    }
  }
}
