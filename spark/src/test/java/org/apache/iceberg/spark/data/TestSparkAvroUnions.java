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
  public void writeAndValidateNonOptionUnionNonNullable() throws IOException {
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
    unionRecord1.put("unionCol", "StringType1");
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
    }
  }

  @Test
  public void writeAndValidateNonOptionUnionNullable() throws IOException {
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
        .noDefault()
        .endRecord();

    GenericData.Record unionRecord1 = new GenericData.Record(avroSchema);
    unionRecord1.put("unionCol", "StringType1");
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
    }
  }

  @Test
  public void writeAndValidateSingleOptionUnion() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .intType()
        .endUnion()
        .noDefault()
        .endRecord();

    GenericData.Record unionRecord1 = new GenericData.Record(avroSchema);
    unionRecord1.put("unionCol", 1);
    GenericData.Record unionRecord2 = new GenericData.Record(avroSchema);
    unionRecord2.put("unionCol", 2);

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
    }
  }
}
