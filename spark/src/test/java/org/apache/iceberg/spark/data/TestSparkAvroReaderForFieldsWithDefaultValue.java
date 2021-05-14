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
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.iceberg.spark.SparkSchemaUtil.convert;

public class TestSparkAvroReaderForFieldsWithDefaultValue {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testAvroDefaultValues() throws IOException {
    String indexFiledName = "index";
    String nullableFiledName = "optionalFieldWithDefault";
    String requiredFiledName = "requiredFieldWithDefault";
    int defaultValue = -1;

    // write records with initial writeSchema
    org.apache.avro.Schema writeSchema = org.apache.avro.Schema.createRecord("root", null, null, false,
        ImmutableList.of(new org.apache.avro.Schema.Field(indexFiledName, org.apache.avro.Schema.create(INT),
            null, null), new org.apache.avro.Schema.Field(nullableFiledName,
            org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(INT),
                org.apache.avro.Schema.create(NULL)), null, defaultValue)));

    Schema icebergWriteSchema = AvroSchemaUtil.toIceberg(writeSchema);
    List<GenericData.Record> expected = RandomData.generateList(icebergWriteSchema, 2, 0L);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(testFile))
        .schema(icebergWriteSchema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : expected) {
        writer.add(rec);
      }
    }

    // evolve schema by adding a required field with default value
    org.apache.avro.Schema evolvedSchema = org.apache.avro.Schema.createRecord("root", null, null, false,
        ImmutableList.of(new org.apache.avro.Schema.Field(indexFiledName, org.apache.avro.Schema.create(INT),
                null, null),
            new org.apache.avro.Schema.Field(nullableFiledName,
                org.apache.avro.Schema.createUnion(org.apache.avro.Schema.create(INT),
                    org.apache.avro.Schema.create(NULL)), null, defaultValue),
            new org.apache.avro.Schema.Field(requiredFiledName, org.apache.avro.Schema.create(INT), null, defaultValue)
        ));

    // read written rows with evolved schema
    List<InternalRow> rows;
    Schema icebergReadSchema = AvroSchemaUtil.toIceberg(evolvedSchema);
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
        .createReaderFunc(SparkAvroReader::new)
        .project(icebergReadSchema)
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    // validate all rows, and all fields are read properly
    Assert.assertNotNull(rows);
    Assert.assertEquals(expected.size(), rows.size());
    for (int row = 0; row < expected.size(); row++) {
      GenericData.Record expectedRow = expected.get(row);
      InternalRow actualRow = rows.get(row);
      List<Types.NestedField> fields = icebergReadSchema.asStruct().fields();

      for (int i = 0; i < fields.size(); i += 1) {
        Object expectedValue = null;
        if (i >= writeSchema.getFields().size() && fields.get(i).hasDefaultValue()) {
          expectedValue = fields.get(i).getDefaultValue();
        } else if (i < writeSchema.getFields().size()) {
          expectedValue = expectedRow.get(i);
        }
        Type fieldType = fields.get(i).type();
        Object actualValue = actualRow.isNullAt(i) ? null : actualRow.get(i, convert(fieldType));
        Assert.assertEquals(expectedValue, actualValue);
      }
    }
  }
}

