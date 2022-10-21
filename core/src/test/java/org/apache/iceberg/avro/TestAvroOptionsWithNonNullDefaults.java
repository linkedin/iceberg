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

package org.apache.iceberg.avro;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.Files;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;

public class TestAvroOptionsWithNonNullDefaults {

  private static final String fieldWithDefaultName = "fieldWithDefault";
  private static final String noDefaultFiledName = "noDefaultField";

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void writeAndValidateOptionWithNonNullDefaultsPruning() throws IOException {
    Schema writeSchema = Schema.createRecord("root", null, null, false,
        ImmutableList.of(
            new Schema.Field("field", Schema.createUnion(Schema.createArray(Schema.create(INT)), Schema.create(NULL)),
                null, ImmutableList.of())
        )
    );

    GenericData.Record record1 = new GenericData.Record(writeSchema);
    record1.put("field", ImmutableList.of(1, 2, 3));
    GenericData.Record record2 = new GenericData.Record(writeSchema);
    record2.put("field", null);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(writeSchema, testFile);
      writer.append(record1);
      writer.append(record2);
    }

    List<GenericData.Record> expected = ImmutableList.of(record1, record2);

    org.apache.iceberg.Schema readIcebergSchema = AvroSchemaUtil.toIceberg(writeSchema);
    List<GenericData.Record> rows;
    try (AvroIterable<GenericData.Record> reader = Avro.read(Files.localInput(testFile))
        .project(readIcebergSchema).build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      AvroTestHelpers.assertEquals(readIcebergSchema.asStruct(), expected.get(i), rows.get(i));
    }
  }

  @Test
  public void writeAndValidateOptionWithNonNullDefaultsEvolution() throws IOException {
    Schema writeSchema = Schema.createRecord("root", null, null, false,
        ImmutableList.of(
            new Schema.Field("field", Schema.createUnion(Schema.create(INT), Schema.create(NULL)), null, -1)
        )
    );

    GenericData.Record record1 = new GenericData.Record(writeSchema);
    record1.put("field", 1);
    GenericData.Record record2 = new GenericData.Record(writeSchema);
    record2.put("field", null);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(writeSchema, testFile);
      writer.append(record1);
      writer.append(record2);
    }

    Schema readSchema = Schema.createRecord("root", null, null, false,
        ImmutableList.of(
            new Schema.Field("field", Schema.createUnion(Schema.create(LONG), Schema.create(NULL)), null, -1L)
        )
    );

    GenericData.Record expectedRecord1 = new GenericData.Record(readSchema);
    expectedRecord1.put("field", 1L);
    GenericData.Record expectedRecord2 = new GenericData.Record(readSchema);
    expectedRecord2.put("field", null);
    List<GenericData.Record> expected = ImmutableList.of(expectedRecord1, expectedRecord2);

    org.apache.iceberg.Schema readIcebergSchema = AvroSchemaUtil.toIceberg(readSchema);
    List<GenericData.Record> rows;
    try (AvroIterable<GenericData.Record> reader = Avro.read(Files.localInput(testFile))
        .project(readIcebergSchema).build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      AvroTestHelpers.assertEquals(readIcebergSchema.asStruct(), expected.get(i), rows.get(i));
    }
  }

  @Test
  public void testDefaultValueUsedPrimitiveType() throws IOException {
    Schema writeSchema = Schema.createRecord("root", null, null, false, ImmutableList.of(
        new Schema.Field(noDefaultFiledName, Schema.create(INT), null, null)));
    // evolved schema
    Schema readSchema = Schema.createRecord("root", null, null, false, ImmutableList.of(
        new Schema.Field(noDefaultFiledName, Schema.create(INT), null, null),
        new Schema.Field(fieldWithDefaultName, Schema.create(INT), null, -1)));

    GenericData.Record record1 = new GenericData.Record(writeSchema);
    record1.put(noDefaultFiledName, 1);
    GenericData.Record record2 = new GenericData.Record(writeSchema);
    record2.put(noDefaultFiledName, 2);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(writeSchema, testFile);
      writer.append(record1);
      writer.append(record2);
    }


    List<GenericData.Record> expected = ImmutableList.of(record1, record2);
    org.apache.iceberg.Schema readIcebergSchema = AvroSchemaUtil.toIceberg(readSchema);
    List<GenericData.Record> rows;
    try (AvroIterable<GenericData.Record> reader =
        Avro.read(Files.localInput(testFile)).project(readIcebergSchema).build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      Assert.assertEquals(expected.get(i).get(noDefaultFiledName), rows.get(i).get(noDefaultFiledName));
      // default should be used for records missing the field
      Assert.assertEquals(-1, rows.get(i).get(fieldWithDefaultName));
    }
  }

  @Test
  public void testDefaultValueNotUsedWhenFiledHasValue() throws IOException {
    Schema readSchema = Schema.createRecord("root", null, null, false, ImmutableList.of(
        new Schema.Field(noDefaultFiledName, Schema.create(INT), null, null),
        new Schema.Field(fieldWithDefaultName, Schema.create(INT), null, -1)));

    GenericData.Record record1 = new GenericData.Record(readSchema);
    record1.put(noDefaultFiledName, 3);
    record1.put(fieldWithDefaultName, 3);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(readSchema, testFile);
      writer.append(record1);
    }

    List<GenericData.Record> expected = ImmutableList.of(record1);
    org.apache.iceberg.Schema readIcebergSchema = AvroSchemaUtil.toIceberg(readSchema);
    List<GenericData.Record> rows;
    try (AvroIterable<GenericData.Record> reader =
        Avro.read(Files.localInput(testFile)).project(readIcebergSchema).build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      Assert.assertEquals(expected.get(i).get(noDefaultFiledName), rows.get(i).get(noDefaultFiledName));
      // default value should NOT be used if field is populated
      Assert.assertEquals(expected.get(i).get(fieldWithDefaultName), rows.get(i).get(fieldWithDefaultName));
    }
  }

  @Test
  public void testDefaultValueUsedComplexType() throws IOException {
    Schema writeSchema = Schema.createRecord("root", null, null, false, ImmutableList.of(
        new Schema.Field(noDefaultFiledName, Schema.create(INT), null, null)));
    // evolved schema
    List<Integer> defaultArray = Arrays.asList(-1, -2);
    Schema readSchema = Schema.createRecord("root", null, null, false, ImmutableList.of(
        new Schema.Field(noDefaultFiledName, Schema.create(INT), null, null),
        new Schema.Field(fieldWithDefaultName, Schema.createArray(Schema.create(INT)), null, defaultArray)));

    GenericData.Record record1 = new GenericData.Record(writeSchema);
    record1.put(noDefaultFiledName, 1);
    GenericData.Record record2 = new GenericData.Record(writeSchema);
    record2.put(noDefaultFiledName, 2);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(writeSchema, testFile);
      writer.append(record1);
      writer.append(record2);
    }


    List<GenericData.Record> expected = ImmutableList.of(record1, record2);
    org.apache.iceberg.Schema readIcebergSchema = AvroSchemaUtil.toIceberg(readSchema);
    List<GenericData.Record> rows;
    try (AvroIterable<GenericData.Record> reader =
        Avro.read(Files.localInput(testFile)).project(readIcebergSchema).build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      Assert.assertEquals(expected.get(i).get(noDefaultFiledName), rows.get(i).get(noDefaultFiledName));
      // default should be used for records missing the field
      Assert.assertEquals(defaultArray, rows.get(i).get(fieldWithDefaultName));
    }
  }
}
