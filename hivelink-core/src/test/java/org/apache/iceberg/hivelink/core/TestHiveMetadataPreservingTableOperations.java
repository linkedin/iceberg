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
    // Schema literal with 2 fields (name and id)
    String testSchemaLiteral = "{\"name\": \"testSchema\", \"type\": \"record\", " +
        "\"namespace\": \"com.linkedin.test\", \"fields\": [{ \"name\": \"name\", \"type\": " +
        "\"string\"},{ \"name\": \"id\", \"type\": \"int\"}]}";
    long currentTimeMillis = System.currentTimeMillis();
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    // Set cols to only id field (missing name)
    storageDescriptor.setCols(ImmutableList.of(new FieldSchema("id", "int", "")));
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

    Assert.assertEquals(1, tbl.getSd().getColsSize());
    HiveMetadataPreservingTableOperations.fixMismatchedSchema(tbl);
    Assert.assertEquals(2, tbl.getSd().getColsSize());
    Assert.assertEquals("name", tbl.getSd().getCols().get(0).getName());
    Assert.assertEquals("id", tbl.getSd().getCols().get(1).getName());
  }
}
