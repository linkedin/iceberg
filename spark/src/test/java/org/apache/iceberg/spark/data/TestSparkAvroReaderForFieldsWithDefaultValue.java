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
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.spark.data.TestHelpers.assertEquals;

public class TestSparkAvroReaderForFieldsWithDefaultValue {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testAvroDefaultValues() throws IOException {

    String writeSchemaString = "{\n" +
            "  \"namespace\": \"com.n1\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"n1\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"f0\",\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    org.apache.avro.Schema writeSchema = new org.apache.avro.Schema.Parser().parse(writeSchemaString);
    org.apache.iceberg.Schema icebergWriteSchema = AvroSchemaUtil.toIceberg(writeSchema);

    List<GenericData.Record> expected = RandomData.generateList(icebergWriteSchema, 2, 0L);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    // write records with initial writeSchema
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(testFile))
            .schema(icebergWriteSchema)
            .named("test")
            .build()) {
      for (GenericData.Record rec : expected) {
        writer.add(rec);
      }
    }

    // evolve schema by adding a required field with default value
    String evolvedSchemaString = "{\n" +
            "  \"namespace\": \"com.n1\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"n1\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"f0\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"f1\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"default\": \"foo\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"f2\",\n" +
            "      \"type\": \"int\",\n" +
            "      \"default\": 1\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"f3\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"map\",\n" +
            "        \"values\" : \"int\"\n" +
            "      },\n" +
            "      \"default\": {\"a\": 1}\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"f4\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"items\" : \"int\"\n" +
            "      },\n" +
            "      \"default\": [1, 2, 3]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"f5\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"F5\",\n" +
            "        \"fields\" : [\n" +
            "          {\"name\": \"ff1\", \"type\": \"long\"},\n" +
            "          {\"name\": \"ff2\", \"type\":  \"string\"}\n" +
            "        ]\n" +
            "      },\n" +
            "      \"default\": {\n" +
            "        \"ff1\": 999,\n" +
            "        \"ff2\": \"foo\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"f6\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"map\",\n" +
            "        \"values\": {\n" +
            "          \"type\": \"array\",\n" +
            "          \"items\" : \"int\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"default\": {\"key\": [1, 2, 3]}\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"f7\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"fixed\",\n" +
            "        \"name\": \"md5\",\n" +
            "        \"size\": 2\n" +
            "      },\n" +
            "      \"default\": \"FF\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";
    org.apache.avro.Schema evolvedSchema = new org.apache.avro.Schema.Parser().parse(evolvedSchemaString);

    List<InternalRow> rows;
    Schema icebergReadSchema = AvroSchemaUtil.toIceberg(evolvedSchema);
    // read written rows with evolved schema
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
            .createReaderFunc(SparkAvroReader::new)
            .project(icebergReadSchema)
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    Assert.assertNotNull(rows);
    Assert.assertEquals(expected.size(), rows.size());
    for (int row = 0; row < expected.size(); row++) {
      InternalRow actualRow = rows.get(row);
      final InternalRow expectedRow = new GenericInternalRow(8);
      expectedRow.update(0, UTF8String.fromString((String) expected.get(row).get(0)));
      expectedRow.update(1, UTF8String.fromString("foo"));
      expectedRow.update(2, 1);
      expectedRow.update(3, new ArrayBasedMapData(
              new GenericArrayData(Arrays.asList(UTF8String.fromString("a"))),
              new GenericArrayData(Arrays.asList(1))));
      expectedRow.update(4, new GenericArrayData(ImmutableList.of(1, 2, 3).toArray()));

      final InternalRow nestedStructData = new GenericInternalRow(2);
      nestedStructData.update(0, 999L);
      nestedStructData.update(1, UTF8String.fromString("foo"));
      expectedRow.update(5, nestedStructData);

      List<GenericArrayData> listOfLists = new ArrayList<GenericArrayData>(1);
      listOfLists.add(new GenericArrayData(ImmutableList.of(1, 2, 3).toArray()));
      expectedRow.update(6, new ArrayBasedMapData(
              new GenericArrayData(Arrays.asList(UTF8String.fromString("key"))),
              new GenericArrayData(listOfLists.toArray())));

      byte[] objGUIDByteArr = "FF".getBytes("UTF-8");
      expectedRow.update(7, objGUIDByteArr);
      assertEquals(icebergReadSchema, actualRow, expectedRow);

    }
  }
}

