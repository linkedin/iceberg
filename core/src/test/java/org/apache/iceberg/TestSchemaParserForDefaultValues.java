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

package org.apache.iceberg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;


public class TestSchemaParserForDefaultValues {

  private void assertEqualStructs(org.apache.iceberg.Schema expected, org.apache.iceberg.Schema actual) {
    if (expected == null) {
      Assert.assertNull(actual);
      return;
    }
    Assert.assertNotNull(actual);
    List<NestedField> expectedFields = expected.asStruct().fields();
    List<NestedField> actualFields = actual.asStruct().fields();

    Assert.assertEquals(expectedFields.size(), actualFields.size());

    for (int i = 0; i < expectedFields.size(); i++) {
      NestedField expectedField = expectedFields.get(i);
      NestedField actualField = actualFields.get(i);
      Assert.assertEquals(expectedField.fieldId(), actualField.fieldId());
      Assert.assertEquals(expectedField.name(), actualField.name());
      Assert.assertEquals(expectedField.type(), actualField.type());
      Assert.assertEquals(expectedField.doc(), actualField.doc());
      if (expectedField.hasDefaultValue()) {
        Assert.assertTrue(actualField.hasDefaultValue());
        switch (expectedField.type().typeId()) {
          case BINARY:
          case FIXED:
            Assert.assertTrue(
                Arrays.equals((byte[]) expectedField.getDefaultValue(), (byte[]) actualField.getDefaultValue()));
            break;
          default:
            Assert.assertEquals(expectedField.getDefaultValue(), actualField.getDefaultValue());
        }
      } else {
        Assert.assertFalse(actualField.hasDefaultValue());
      }
    }
  }

  private void testToFromJsonPreservingDefaultValues(String[] fieldNames, Schema[] fieldsSchemas, Object[] defaults) {
    List<Field> fields = new ArrayList<>();
    IntStream.range(0, defaults.length).forEach(
        i -> fields.add(new Schema.Field(fieldNames[i], fieldsSchemas[i], null, defaults[i])));

    Schema schema = Schema.createRecord("root", null, null, false, fields);
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
    String jsonString = SchemaParser.toJson(icebergSchema);

    Assert.assertTrue(jsonString.contains("default"));

    org.apache.iceberg.Schema icebergSchemaFromJson = SchemaParser.fromJson(jsonString);

    assertEqualStructs(icebergSchema, icebergSchemaFromJson);
  }

  @Test
  public void testPrimitiveTypes() {
    Boolean defaultBoolean = true;
    Integer defaultInt = 1;
    Long defaultLong = -1L;
    Double defaultDouble = 0.1;
    Float defaultFloat = 0.1f;
    String defaultString = "default string";
    String defaultBytes = "1111";
    int fixedSize = defaultBytes.getBytes().length;

    String[] fieldNames = {
        "booleanField",
        "intField",
        "longField",
        "doubleField",
        "floatField",
        "stringField",
        "binaryField",
        "fixedField"};

    Object[] defaults = {
        defaultBoolean,
        defaultInt,
        defaultLong,
        defaultDouble,
        defaultFloat,
        defaultString,
        defaultBytes,
        defaultBytes};

    Schema[] primitives = {
        Schema.create(BOOLEAN),
        Schema.create(INT),
        Schema.create(LONG),
        Schema.create(DOUBLE),
        Schema.create(FLOAT),
        Schema.create(STRING),
        Schema.create(BYTES),
        Schema.createFixed("md5", null, "namespace", fixedSize)};

    testToFromJsonPreservingDefaultValues(fieldNames, primitives, defaults);
  }

  @Test
  public void testLogicalTypes() {
    Long longDefault = Long.valueOf(1234556789);
    String[] fieldNames = {
        "dateField",
        "timeField",
        "timestampField",
        "uuidField",
        "decimalField"};

    Object[] defaults = {
        Integer.valueOf(123446),
        longDefault,
        "randomUUID",
        longDefault};

    Schema dateSchema = Schema.create(INT);
    dateSchema.addProp("logicaltype", "date");
    Schema timestampSchema = Schema.create(LONG);
    timestampSchema.addProp("logicaltype", "timestamp");
    Schema uuidSchema = Schema.create(STRING);
    uuidSchema.addProp("logicaltype", "UUID");
    Schema bigDecimalSchema = Schema.create(LONG);
    bigDecimalSchema.addProp("logicaltype", "decimal");

    Schema[] logicals = {
        dateSchema,
        timestampSchema,
        uuidSchema,
        bigDecimalSchema};

    testToFromJsonPreservingDefaultValues(fieldNames, logicals, defaults);
  }

  @Test
  public void testNestedTypes() {
    String structStringFieldName = "stringFieldOfStruct";
    String structBooleanFieldName = "booleanFieldOfStruct";
    Map<String, Object> defaultStruct = ImmutableMap.of(structStringFieldName, "default string",
        structBooleanFieldName, Boolean.TRUE);
    List<Integer> defaultList = Arrays.asList(1, 2);
    Map<String, Long> defaultMap = ImmutableMap.of("key1", Long.valueOf(1L), "key2", Long.valueOf(2L));
    List<Schema.Field> structFields = ImmutableList.of(
        new Schema.Field(structStringFieldName, Schema.create(STRING), null),
        new Schema.Field(structBooleanFieldName, Schema.create(BOOLEAN), null));

    String[] fieldNames = {"structField", "listField", "mapField"};
    Object[] defaults = {defaultStruct, defaultList, defaultMap};
    Schema[] nested = {
        Schema.createRecord("name", null, "namespace", false, structFields),
        Schema.createArray(Schema.create(INT)),
        Schema.createMap(Schema.create(LONG))};

    testToFromJsonPreservingDefaultValues(fieldNames, nested, defaults);
  }

  @Test
  public void testOptionalWithDefault() {
    Integer defaultInt = 1;
    Map<String, Long> defaultMap = ImmutableMap.of("key1", Long.valueOf(1L), "key2", Long.valueOf(2L));

    String[] fieldNames = {"optionalPrimitive", "optionalNested"};
    Schema[] optionals = {
        Schema.createUnion(Schema.create(INT), Schema.create(NULL)),
        Schema.createUnion(Schema.createMap(Schema.create(LONG)), Schema.create(NULL))};
    Object[] defaults = {defaultInt, defaultMap};

    testToFromJsonPreservingDefaultValues(fieldNames, optionals, defaults);
  }

  @Test
  public void testNestedOfNestedWithDefault() {
    Integer defaultInt = 1;
    Map<String, Long> defaultMap = ImmutableMap.of("key1", Long.valueOf(1L), "key2", Long.valueOf(2L));

    String structIntField = "intFieldOfStruct";
    String structMapFieldName = "mapFieldOfStruct";
    List<Schema.Field> structFields = ImmutableList.of(
        new Schema.Field(structIntField, Schema.create(INT), null, defaultInt),
        new Schema.Field(structMapFieldName, Schema.createMap(Schema.create(LONG)), null, defaultMap));

    String[] fieldNames = {"intFieldNoDefault", "structFieldNoDefault"};
    Schema[] topLevelFields = {
        Schema.create(INT),
        Schema.createRecord("name", null, "namespace", false, structFields)};

    List<Schema.Field> fields = new ArrayList<>();
    IntStream.range(0, fieldNames.length).forEach(
        i -> fields.add(new Schema.Field(fieldNames[i], topLevelFields[i], null)));

    Schema schema = org.apache.avro.Schema.createRecord("root", null, null, false, fields);
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
    String jsonString = SchemaParser.toJson(icebergSchema);

    Assert.assertTrue(jsonString.contains("default"));

    org.apache.iceberg.Schema fromJsonIcebergSchema = SchemaParser.fromJson(jsonString);
    Assert.assertEquals(icebergSchema.toString(), fromJsonIcebergSchema.toString());
  }

  @Test
  public void testDeepNestedWithDefault() {
    Integer defaultInt = 1;
    Map<String, Long> defaultMap = ImmutableMap.of("key1", Long.valueOf(1L), "key2", Long.valueOf(2L));

    String structIntField = "intFieldOfStruct";
    String structMapFieldName = "mapFieldOfStruct";
    List<Schema.Field> structFields = ImmutableList.of(
        new Schema.Field(structIntField, Schema.create(INT), null, defaultInt),
        new Schema.Field(structMapFieldName, Schema.createMap(Schema.create(LONG)), null, defaultMap));

    Schema downLevelStruct = Schema.createRecord("name", null, "namespace0", false, structFields);

    List<Schema.Field> intermediateStructFields = ImmutableList.of(
        new Schema.Field("intermediateIntField", Schema.create(INT), null),
        new Schema.Field("intermediateStructField", downLevelStruct, null));

    Schema intermediateStruct = Schema.createRecord("name", null, "namespace1", false, intermediateStructFields);
    String[] fieldNames = {"topLevelLong", "topLevelString", "topLevelStruct"};
    Schema[] topLevelFields = {
        Schema.create(LONG),
        Schema.create(STRING),
        intermediateStruct};

    List<Schema.Field> fields = new ArrayList<>();
    IntStream.range(0, fieldNames.length).forEach(
        i -> fields.add(new Schema.Field(fieldNames[i], topLevelFields[i], null)));

    Schema schema = org.apache.avro.Schema.createRecord("root", null, null, false, fields);
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
    String jsonString = SchemaParser.toJson(icebergSchema);

    Assert.assertTrue(jsonString.contains("default"));

    org.apache.iceberg.Schema fromJsonIcebergSchema = SchemaParser.fromJson(jsonString);
    Assert.assertEquals(icebergSchema.toString(), fromJsonIcebergSchema.toString());
  }
}
