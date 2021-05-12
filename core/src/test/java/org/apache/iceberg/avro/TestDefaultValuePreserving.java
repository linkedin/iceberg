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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.avro.Schema.Type.INT;


/**
 * Testing the preserving of fields; default values in {@link SchemaToType} and {@link TypeToSchema}
 */
public class TestDefaultValuePreserving {

  String noDefaultFiledName = "fieldWithNoDefaultValue";
  String fieldWithDefaultName = "fieldWithDefaultValue";
  Integer defaultValue = -1;

  @Test
  public void testSchemaToTypeRecord() {
    Schema recordSchema = Schema.createRecord("root", null, null, false, ImmutableList.of(
        new Schema.Field(noDefaultFiledName, Schema.create(INT), null, null),
        new Schema.Field(fieldWithDefaultName, Schema.create(INT), null, defaultValue)));
    SchemaToType schemaToType = new SchemaToType(recordSchema);
    List<String> names = recordSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    List<Type> types = ImmutableList.of(Types.IntegerType.get(), Types.IntegerType.get());

    Type record = schemaToType.record(recordSchema, names, types);

    Assert.assertNotNull(record);
    Assert.assertTrue(record.isStructType());
    Assert.assertEquals(names.size(), record.asStructType().fields().size());
    Assert.assertFalse(record.asStructType().field(noDefaultFiledName).hasDefaultValue());
    Assert.assertTrue(record.asStructType().field(fieldWithDefaultName).hasDefaultValue());
    Assert.assertEquals(defaultValue, record.asStructType().field(fieldWithDefaultName).getDefaultValue());
  }

  @Test
  public void testTypeToSchemaStruct() {
    List<Types.NestedField> nestedFields = ImmutableList.of(
        Types.NestedField.required(0, noDefaultFiledName, Types.IntegerType.get()),
        Types.NestedField.required(1, fieldWithDefaultName, Types.IntegerType.get(), defaultValue, null));
    Types.StructType structType = Types.StructType.of(nestedFields);
    Map<Types.StructType, String> names = ImmutableMap.of(structType, "tableName");
    TypeToSchema typeToSchema = new TypeToSchema(names);
    List<Schema> fieldSchemas = ImmutableList.of(Schema.create(INT), Schema.create(INT));

    Schema structSchema = typeToSchema.struct(structType, fieldSchemas);

    Assert.assertNotNull(structSchema);
    Assert.assertEquals(nestedFields.size(), structSchema.getFields().size());
    for (int i = 0; i < nestedFields.size(); i++) {
      if (nestedFields.get(i).hasDefaultValue()) {
        Assert.assertTrue(structSchema.getFields().get(i).hasDefaultValue());
        Assert.assertEquals(nestedFields.get(i).getDefaultValue(), structSchema.getFields().get(i).defaultVal());
      } else {
        Assert.assertFalse(structSchema.getFields().get(i).hasDefaultValue());
      }
    }
  }
}
