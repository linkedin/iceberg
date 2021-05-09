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
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;

import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;

public class TestSparkAvroReader extends AvroDataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
//    List<Record> expected = RandomData.generateList(schema, 100, 0L);
//
//    File testFile = temp.newFile();
//    Assert.assertTrue("Delete should succeed", testFile.delete());
//
//    try (FileAppender<Record> writer = Avro.write(Files.localOutput(testFile))
//        .schema(schema)
//        .named("test")
//        .build()) {
//      for (Record rec : expected) {
//        writer.add(rec);
//      }
//    }
//
//    List<InternalRow> rows;
//    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
//        .createReaderFunc(SparkAvroReader::new)
//        .project(schema)
//        .build()) {
//      rows = Lists.newArrayList(reader);
//    }
//
//    for (int i = 0; i < expected.size(); i += 1) {
//      assertEqualsUnsafe(schema.asStruct(), expected.get(i), rows.get(i));
//    }
    String indexFiledName = "index";
    String nullableFiledName = "optionalFieldWithDefault";
    String requiredFiledName = "requiredFieldWithDefault";
    int defaultValue = -1;
    org.apache.avro.Schema writeSchema = org.apache.avro.Schema.createRecord("root", null, null, false,
        ImmutableList.of(new org.apache.avro.Schema.Field(indexFiledName, org.apache.avro.Schema.create(INT),
                    null, null), new org.apache.avro.Schema.Field(nullableFiledName,
                                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(INT),
                                org.apache.avro.Schema.create(NULL)), null, defaultValue)));

    // evolve schema by adding a required field with default value
    org.apache.avro.Schema evolvedSchema = org.apache.avro.Schema.createRecord("root", null, null, false,
        ImmutableList.of(new org.apache.avro.Schema.Field(indexFiledName, org.apache.avro.Schema.create(INT),
                    null, null),
        new org.apache.avro.Schema.Field(nullableFiledName,
            org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(INT),
            org.apache.avro.Schema.create(NULL)), null, defaultValue),
        new org.apache.avro.Schema.Field(requiredFiledName, org.apache.avro.Schema.create(INT), null, defaultValue)
         ));

    Schema icebergWriteSchema = AvroSchemaUtil.toIceberg(writeSchema);
    List<Record> expected = RandomData.generateList(icebergWriteSchema, 2, 0L);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = Avro.write(Files.localOutput(testFile))
        .schema(icebergWriteSchema)
        .named("test")
        .build()) {
      for (Record rec : expected) {
        List<InternalRow> rows;
        Schema icebergReadSchema = AvroSchemaUtil.toIceberg(evolvedSchema);
        try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
            .createReaderFunc(SparkAvroReader::new).project(icebergReadSchema)
            .build()) {
          rows = Lists.newArrayList(reader);
        }

        for (int i = 0; i < expected.size(); i += 1) {
          assertEqualsUnsafe(icebergReadSchema.asStruct(), expected.get(i), rows.get(i));
        }
      }
    }
  }
}
