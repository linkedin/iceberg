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

import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;


public class TestHiveMetadataPreservingTableOperations {

  @Test
  public void testFixMismatchedSchema() {
    // Schema literal with 3 fields (name, id, nested)
    String testSchemaLiteral = "{\"name\":\"testSchema\",\"type\":\"record\",\"namespace\":\"com.linkedin.test\"," +
        "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"nested\"," +
        "\"type\":{\"name\":\"nested\",\"type\":\"record\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}," +
        "{\"name\":\"field2\",\"type\":\"string\"}]}}]}";
    String testSchemaLiteralWithUppercase = "{\"name\":\"testSchema\",\"type\":\"record\",\"namespace\":\"com" +
        ".linkedin.test\", \"fields\":[{\"name\":\"Name\",\"type\":\"string\"},{\"name\":\"ID\",\"type\":\"int\"}" +
        ",{\"name\":\"Nested\", \"type\":{\"name\":\"Nested\",\"type\":\"record\",\"fields\":[{\"name\":\"Field1\"," +
        "\"type\":\"string\"}, {\"name\":\"Field2\",\"type\":\"string\"}]}}]}";

    long currentTimeMillis = System.currentTimeMillis();
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    FieldSchema field1 = new FieldSchema("name", "string", "");
    FieldSchema field2 = new FieldSchema("id", "int", "");
    FieldSchema field3 = new FieldSchema("nested", "struct<field1:string,field2:string>", "");
    // Set cols with incorrect nested type
    storageDescriptor.setCols(ImmutableList.of(field1, field2, new FieldSchema("nested", "struct<field1:int," +
        "field2:string>", "")));
    storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
    Map<String, String> parameters = ImmutableMap.of("avro.schema.literal", testSchemaLiteral);
    Table tbl = new Table("tableName",
        "dbName",
        System.getProperty("user.name"),
        (int) currentTimeMillis / 1000,
        (int) currentTimeMillis / 1000,
        Integer.MAX_VALUE,
        storageDescriptor,
        Collections.emptyList(),
        parameters,
        null,
        null,
        TableType.EXTERNAL_TABLE.toString());

    Assert.assertTrue(HiveMetadataPreservingTableOperations.fixMismatchedSchema(tbl));
    Assert.assertEquals(3, tbl.getSd().getColsSize());
    Assert.assertEquals(field1, tbl.getSd().getCols().get(0));
    Assert.assertEquals(field2, tbl.getSd().getCols().get(1));
    Assert.assertEquals(field3, tbl.getSd().getCols().get(2));
    Assert.assertTrue(storageDescriptor.getSerdeInfo().getParameters()
        .containsKey(HiveMetadataPreservingTableOperations.ORC_COLUMNS));
    Assert.assertTrue(storageDescriptor.getSerdeInfo().getParameters()
        .containsKey(HiveMetadataPreservingTableOperations.ORC_COLUMNS_TYPES));

    // Use same schema literal but containing uppercase and check no mismatch detected
    tbl.setParameters(ImmutableMap.of("avro.schema.literal", testSchemaLiteralWithUppercase));
    Assert.assertFalse(HiveMetadataPreservingTableOperations.fixMismatchedSchema(tbl));
  }
}
