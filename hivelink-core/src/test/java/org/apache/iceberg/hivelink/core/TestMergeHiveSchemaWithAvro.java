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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.util.internal.JacksonUtils;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.hivelink.core.schema.MergeHiveSchemaWithAvro;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class TestMergeHiveSchemaWithAvro {

  @Test
  public void shouldUseFieldNamesFromAvro() {
    String hive = "struct<fa:int,fb:struct<ga:int>>";
    Schema avro = struct("r1",
        optional("fA", Schema.Type.INT),
        optional("fB", struct("r2",
            optional("gA", Schema.Type.INT)
        ))
    );

    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldUseNullabilityFromAvro() {
    String hive = "struct<fa:int,fb:struct<ga:int>>";
    Schema avro = struct("r1",
        required("fA", Schema.Type.INT),
        required("fB", struct("r2",
            required("gA", Schema.Type.INT)
        ))
    );

    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldUseTypesFromHive() {
    String hive = "struct<fa:struct<ga:int>,fb:array<int>,fc:map<string,int>,fd:string>";
    Schema avro = struct("r1",
        required("fA", Schema.Type.INT),
        required("fB", Schema.Type.INT),
        required("fC", Schema.Type.INT),
        required("fD", Schema.Type.INT)
    );

    Schema expected = struct("r1",
        required("fA", struct("record1", null, "namespace1",
            optional("ga", Schema.Type.INT)
        )),
        required("fB", array(nullable(Schema.Type.INT))),
        required("fC", map(nullable(Schema.Type.INT))),
        required("fD", Schema.Type.STRING)
    );

    assertSchema(expected, merge(hive, avro));
  }

  @Test
  public void shouldIgnoreExtraFieldsFromAvro() {
    String hive = "struct<fa:int,fb:struct<ga:int>>";
    Schema avro = struct("r1",
        required("fA", Schema.Type.INT),
        required("fB", struct("r2",
            required("gA", Schema.Type.INT),
            required("gB", Schema.Type.INT)
        )),
        required("fC", Schema.Type.INT)
    );

    Schema expected = struct("r1",
        required("fA", Schema.Type.INT),
        required("fB", struct("r2",
            required("gA", Schema.Type.INT)
        ))
    );

    assertSchema(expected, merge(hive, avro));
  }

  @Test
  public void shouldRetainExtraFieldsFromHive() {
    String hive = "struct<fa:int,fb:struct<ga:int,gb:int>,fc:int,fd:struct<ha:int>>";
    Schema avro = struct("r1",
        required("fA", Schema.Type.INT),
        required("fB", struct("r2",
            required("gA", Schema.Type.INT)
        ))
    );

    Schema expected = struct("r1",
        required("fA", Schema.Type.INT),
        required("fB", struct("r2",
            required("gA", Schema.Type.INT),
            // Nested field missing in Avro
            optional("gb", Schema.Type.INT)
        )),
        // Top level field missing in Avro
        optional("fc", Schema.Type.INT),
        // Top level struct missing in Avro
        optional("fd", struct("record1", null, "namespace1",
            optional("ha", Schema.Type.INT)
        ))
    );

    assertSchema(expected, merge(hive, avro));
  }

  @Test
  public void shouldRetainDocStringsFromAvro() {
    String hive = "struct<fa:int,fb:struct<ga:int>>";
    Schema avro = struct("r1", "doc-r1", "n1",
        required("fA", Schema.Type.INT, "doc-fA", null, null),
        required("fB", struct("r2", "doc-r2", "n2",
            required("gA", Schema.Type.INT, "doc-gA", null, null)
        ), "doc-fB", null, null)
    );

    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldRetainDefaultValuesFromAvro() {
    String hive = "struct<fa:int,fb:struct<ga:int>>";
    Schema avro = struct("r1",
        required("fA", Schema.Type.INT, null, 1, null),
        required("fB", struct("r2",
            required("gA", Schema.Type.INT, null, 2, null)
        ), null, fromJson("{\"gA\": 3}"), null)
    );

    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldRetainFieldPropsFromAvro() {
    String hive = "struct<fa:int,fb:struct<ga:int>>";
    Schema avro = struct("r1",
        required("fA", Schema.Type.INT, null, null, ImmutableMap.of("pfA", "vfA")),
        required("fB", struct("r2",
            required("gA", Schema.Type.INT, null, null, ImmutableMap.of("pfB", "vfB"))
        ), null, null, ImmutableMap.of("pgA", "vgA"))
    );

    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldHandleLists() {
    String hive = "struct<fa:array<int>,fb:array<int>,fc:array<struct<ga:int>>,fd:array<int>>";
    Schema avro = struct("r1",
        required("fA", array(Schema.Type.INT)),
        optional("fB", array(Schema.Type.INT)),
        required("fC", array(struct("r2",
            required("gA", Schema.Type.INT)
        )))
    );

    Schema expected = struct("r1",
        required("fA", array(Schema.Type.INT)),
        optional("fB", array(Schema.Type.INT)),
        required("fC", array(struct("r2",
            required("gA", Schema.Type.INT)
        ))),
        // Array element type is also nullable because it is generated from Hive
        optional("fd", array(nullable(Schema.Type.INT)))
    );

    assertSchema(expected, merge(hive, avro));
  }

  @Test
  public void shouldHandleMaps() {
    String hive = "struct<fa:map<string,int>,fb:map<string,int>,fc:map<string,struct<ga:int>>,fd:map<string,int>>";
    Schema avro = struct("r1",
        required("fA", map(Schema.Type.INT)),
        optional("fB", map(Schema.Type.INT)),
        required("fC", map(struct("r2",
            required("gA", Schema.Type.INT)
        )))
    );

    Schema expected = struct("r1",
        required("fA", map(Schema.Type.INT)),
        optional("fB", map(Schema.Type.INT)),
        required("fC", map(struct("r2",
            required("gA", Schema.Type.INT)
        ))),
        // Map value type is also nullable because it is generated from Hive
        optional("fd", map(nullable(Schema.Type.INT)))
    );

    assertSchema(expected, merge(hive, avro));
  }

  @Test
  public void shouldHandleUnions() {
    String hive = "struct<fa:uniontype<string,int>,fb:uniontype<string,int>,fc:uniontype<string,int>>";
    Schema avro = struct("r1",
        required("fA", union(Schema.Type.NULL, Schema.Type.STRING, Schema.Type.INT)),
        required("fB", union(Schema.Type.STRING, Schema.Type.INT)),
        required("fC", union(Schema.Type.STRING, Schema.Type.INT, Schema.Type.NULL))
    );

    Schema expected = struct("r1",
        required("fA", union(Schema.Type.NULL, Schema.Type.STRING, Schema.Type.INT)),
        required("fB", union(Schema.Type.STRING, Schema.Type.INT)),
        // our merge logic always put the NULL alternative in the front
        required("fC", union(Schema.Type.NULL, Schema.Type.STRING, Schema.Type.INT))
    );

    assertSchema(expected, merge(hive, avro));
  }

  @Test
  public void shouldSanitizeIncompatibleFieldNames() {
    StructTypeInfo typeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
        Lists.newArrayList("a.b.c", "$#@%!"),
        Lists.newArrayList(TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo)
    );
    Schema avro = struct("r1");

    Schema expected = struct("r1",
        optional("a_x2Eb_x2Ec", Schema.Type.INT),
        optional("_x24_x23_x40_x25_x21", Schema.Type.INT)
    );
    assertSchema(expected, merge(typeInfo, avro));
  }

  @Test
  public void shouldReorderOptionalSchemaToMatchDefaultValue() {
    String hive = "struct<fa:int,fb:struct<ga:int>>";
    Schema avro = struct("r1",
        field("fA", Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL)),
            null, 1, null),
        field("fB", Schema.createUnion(
            struct("r2", required("gA", Schema.Type.INT, null, 2, null)),
            Schema.create(Schema.Type.NULL)
        ), null, fromJson("{\"gA\": 3}"), null)
    );

    assertSchema(avro, merge(hive, avro));
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void shouldFailForMapsWithNonStringKey() {
    String hive = "struct<fa:map<int,int>>";
    Schema avro = struct("r1");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Map keys should always be non-nullable strings");
    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldRetainMapProp() {
    String hive = "struct<fa:map<string,int>>";
    Schema fa = map(Schema.Type.INT);
    fa.addProp("key-id", 1);
    fa.addProp("value-id", 2);
    Schema avro = struct("r1", required("fa", fa));

    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldRetainNullableMapProp() {
    String hive = "struct<fa:map<string,int>>";
    Schema fa = map(Schema.Type.INT);
    fa.addProp("key-id", 1);
    fa.addProp("value-id", 2);
    Schema avro = struct("r1", optional("fa", fa));

    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldRetainListProp() {
    String hive = "struct<fa:array<int>>";
    Schema fa = array(Schema.Type.INT);
    fa.addProp("element-id", 1);
    Schema avro = struct("r1", required("fA", fa));

    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldRetainNullableListProp() {
    String hive = "struct<fa:array<int>>";
    Schema fa = array(Schema.Type.INT);
    fa.addProp("element-id", 1);
    Schema avro = struct("r1", optional("fA", fa));

    assertSchema(avro, merge(hive, avro));
  }

  @Test
  public void shouldRecoverLogicalType() {
    String hive = "struct<fa:date,fb:timestamp,fc:decimal(4,2)>";
    Schema avro = struct("r1",
            optional("fa", Schema.Type.INT),
            optional("fb", Schema.Type.LONG),
            optional("fc", Schema.Type.BYTES));
    Schema merged = merge(hive, avro);

    Schema expectedTimestampSchema = Schema.create(Schema.Type.LONG);
    expectedTimestampSchema.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
    Schema expected = struct("r1",
            optional("fa", LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))),
            optional("fb", LogicalTypes.timestampMillis().addToSchema(expectedTimestampSchema)),
            optional("fc", LogicalTypes.decimal(4, 2).addToSchema(Schema.create(Schema.Type.BYTES))));

    assertSchema(expected, merged);
    Assert.assertEquals("date",
            AvroSchemaUtil.fromOption(merged.getField("fa").schema()).getLogicalType().getName());
    // This last line should not throw any exception.
    AvroSchemaUtil.toIceberg(merged);
  }

  // TODO: tests to retain schema props
  // TODO: tests for explicit type compatibility check between hive and avro primitives, once we implement it
  // TODO: tests for error case => default value in Avro does not match with type from hive

  /** Test Helpers */

  private void assertSchema(Schema expected, Schema actual) {
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expected.toString(true), actual.toString(true));
  }

  private Schema merge(StructTypeInfo typeInfo, Schema avro) {
    return MergeHiveSchemaWithAvro.visit(typeInfo, avro);
  }

  private Schema merge(String hive, Schema avro) {
    StructTypeInfo typeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(hive);
    return merge(typeInfo, avro);
  }

  private Schema struct(String name, String doc, String namespace, Schema.Field... fields) {
    return Schema.createRecord(name, doc, namespace, false, Arrays.asList(fields));
  }

  private Schema struct(String name, Schema.Field... fields) {
    return struct(name, null, "n" + name, fields);
  }

  private Schema array(Schema element) {
    return Schema.createArray(element);
  }

  private Schema array(Schema.Type elementType) {
    return array(Schema.create(elementType));
  }

  private Schema map(Schema value) {
    return Schema.createMap(value);
  }

  private Schema map(Schema.Type valueType) {
    return map(Schema.create(valueType));
  }

  private Schema union(Schema.Type... types) {
    return Schema.createUnion(Arrays.stream(types).map(Schema::create).collect(Collectors.toList()));
  }

  private Schema.Field nullable(Schema.Field field) {
    Preconditions.checkArgument(!AvroSchemaUtil.isOptionSchema(field.schema()));
    return field(field.name(), nullable(field.schema()), field.doc(),
        Schema.Field.NULL_DEFAULT_VALUE, field.getObjectProps());
  }

  private Schema nullable(Schema schema) {
    return AvroSchemaUtil.toOption(schema);
  }

  private Schema nullable(Schema.Type type) {
    return nullable(Schema.create(type));
  }

  private Schema.Field field(String name, Schema schema, String doc, Object defaultValue,
      Map<String, Object> props) {
    Schema.Field field = new Schema.Field(name, schema, doc, defaultValue);
    if (props != null) {
      props.forEach(field::addProp);
    }
    return field;
  }

  private Schema.Field required(String name, Schema schema, String doc, Object defaultValue,
      Map<String, Object> props) {
    return field(name, schema, doc, defaultValue, props);
  }

  private Schema.Field required(String name, Schema schema) {
    return required(name, schema, null, null, null);
  }

  private Schema.Field required(String name, Schema.Type type, String doc, Object defaultValue,
      Map<String, Object> props) {
    return required(name, Schema.create(type), doc, defaultValue, props);
  }

  private Schema.Field required(String name, Schema.Type type) {
    return required(name, type, null, null, null);
  }

  private Schema.Field optional(String name, Schema schema, String doc) {
    return nullable(field(name, schema, doc, null, null));
  }

  private Schema.Field optional(String name, Schema schema) {
    return optional(name, schema, null);
  }

  private Schema.Field optional(String name, Schema.Type type, String doc) {
    return optional(name, Schema.create(type), doc);
  }

  private Schema.Field optional(String name, Schema.Type type) {
    return optional(name, type, null);
  }

  private Object fromJson(String json) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return JacksonUtils.toObject(mapper.readTree(json));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
