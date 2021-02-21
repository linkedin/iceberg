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

package org.apache.iceberg.hive.legacy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;


/**
 * A {@link HiveSchemaWithPartnerVisitor} which augments a Hive schema with extra metadata from a partner Avro schema
 * and generates a resultant "merged" Avro schema
 *
 * 1. Fields are matched between Hive and Avro schemas using a case insensitive search by field name
 * 2. Copies field names, nullability, default value, field props from the Avro schema
 * 3. Copies field type from the Hive schema. We currently perform a check to ensure type compatibility between matched
 *    fields only for container types i.e STRUCT, LIST and MAP. e.g. STRUCT in Hive schema must be matched to
 *    a RECORD in Avro schema. We perform a blind copy of Hive schema for primitive types.
 *    TODO: We should check for type compatibility of primitives instead of a blind copy. We should also handle some
 *          cases of type promotion, e.g. INT -> LONG, BINARY -> FIXED, STRING -> ENUM, etc
 * 4. Retains fields found only in the Hive schema; Ignores fields found only in the Avro schema
 * 5. Fields found only in Hive schema are represented as optional fields in the resultant Avro schema
 * 6. For fields found only in Hive schema, field names are sanitized to make them compatible with Avro identifier spec
 */
class MergeHiveSchemaWithAvro extends HiveSchemaWithPartnerVisitor<Schema, Schema.Field, Schema, Schema.Field> {

  static Schema visit(StructTypeInfo typeInfo, Schema schema) {
    return HiveSchemaWithPartnerVisitor.visit(typeInfo, schema, new MergeHiveSchemaWithAvro(),
        AvroPartnerAccessor.INSTANCE);
  }

  private final AtomicInteger recordCounter = new AtomicInteger(0);
  private final HiveTypeToAvroType hiveToAvro = new HiveTypeToAvroType();

  @Override
  public Schema struct(StructTypeInfo struct, Schema partner, List<Schema.Field> fieldResults) {
    if (partner == null) { // match not found in Avro schema
      int recordNum = recordCounter.incrementAndGet();
      return AvroSchemaUtil.toOption(
          Schema.createRecord("record" + recordNum, null, "namespace" + recordNum, false, fieldResults));
    } else if (AvroSchemaUtil.isOptionSchema(partner)) {
      return AvroSchemaUtil.toOption(
          AvroSchemaUtil.copyRecord(AvroSchemaUtil.fromOption(partner), Lists.newArrayList(fieldResults), null));
    } else {
      return AvroSchemaUtil.copyRecord(partner, Lists.newArrayList(fieldResults), null);
    }
  }

  @Override
  public Schema.Field field(String name, TypeInfo field, Schema.Field partner, Schema fieldResult) {
    if (partner == null) { // match not found in Avro schema
      return new Schema.Field(
          AvroSchemaUtil.makeCompatibleName(name), fieldResult, null, Schema.Field.NULL_DEFAULT_VALUE);
    } else {
      // TODO: How to ensure that field default value is compatible with new field type generated from Hive?
      // Copy field type from the visitor result, copy everything else from the partner
      // For optional fields, reorder the (null, type) options depending on the value of the default since
      // Avro requires the default value to match the first type in the (null, type) union
      Schema reordered = reorderOptionIfRequired(fieldResult, partner.defaultVal());
      return AvroSchemaUtil.copyField(partner, reordered, partner.name());
    }
  }

  private Schema reorderOptionIfRequired(Schema schema, Object defaultValue) {
    if (AvroSchemaUtil.isOptionSchema(schema) && defaultValue != null) {
      boolean isNullFirstOption = schema.getTypes().get(0).getType() == Schema.Type.NULL;
      if (isNullFirstOption && defaultValue.equals(JsonProperties.NULL_VALUE)) {
        return schema;
      } else {
        return Schema.createUnion(schema.getTypes().get(1), schema.getTypes().get(0));
      }
    } else {
      return schema;
    }
  }

  @Override
  public Schema list(ListTypeInfo list, Schema partner, Schema elementResult) {
    if (partner == null || AvroSchemaUtil.isOptionSchema(partner)) {
      return AvroSchemaUtil.toOption(Schema.createArray(elementResult));
    } else {
      return Schema.createArray(elementResult);
    }
  }

  @Override
  public Schema map(MapTypeInfo map, Schema partner, Schema keyResult, Schema valueResult) {
    Preconditions.checkArgument(extractIfOption(keyResult).getType() == Schema.Type.STRING,
        "Map keys should always be non-nullable strings. Found: %s", keyResult);
    if (partner == null || AvroSchemaUtil.isOptionSchema(partner)) {
      return AvroSchemaUtil.toOption(Schema.createMap(valueResult));
    } else {
      return Schema.createMap(valueResult);
    }
  }

  @Override
  public Schema primitive(PrimitiveTypeInfo primitive, Schema partner) {
    Schema schema = HiveTypeUtil.visit(primitive, hiveToAvro);
    if (partner == null) { // match not found in Avro schema
      return AvroSchemaUtil.toOption(schema);
    } else {
      Schema partnerWithoutNull = extractIfOption(partner);
      Schema promoted = checkCompatibilityAndPromote(schema, partnerWithoutNull);
      if (AvroSchemaUtil.isOptionSchema(partner)) {
        return AvroSchemaUtil.toOption(promoted);
      } else {
        return promoted;
      }
    }
  }

  private Schema checkCompatibilityAndPromote(Schema schema, Schema partner) {
    // TODO: Check if schema is compatible with partner
    //       Also do type promotion if required, schema = string & partner = enum, schema = bytes & partner = fixed, etc
    return schema;
  }

  private static class AvroPartnerAccessor implements PartnerAccessors<Schema, Schema.Field> {
    private static final AvroPartnerAccessor INSTANCE = new AvroPartnerAccessor();

    private static final Schema MAP_KEY = Schema.create(Schema.Type.STRING);

    @Override
    public Schema.Field fieldPartner(Schema partner, String name) {
      Schema schema = extractIfOption(partner);
      Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD,
          "Cannot merge Hive type %s with Avro type %s", ObjectInspector.Category.STRUCT, schema.getType());
      // TODO: Optimize? This will be called for every struct field, we will run the for loop for every struct field
      for (Schema.Field field : schema.getFields()) {
        if (field.name().equalsIgnoreCase(name)) {
          return field;
        }
      }
      return null;
    }

    @Override
    public Schema fieldType(Schema.Field partnerField) {
      return partnerField.schema();
    }

    @Override
    public Schema mapKeyPartner(Schema partner) {
      return MAP_KEY;
    }

    @Override
    public Schema mapValuePartner(Schema partner) {
      Schema schema = extractIfOption(partner);
      Preconditions.checkArgument(schema.getType() == Schema.Type.MAP,
          "Cannot merge Hive type %s with Avro type %s", ObjectInspector.Category.MAP, schema.getType());
      return schema.getValueType();
    }

    @Override
    public Schema listElementPartner(Schema partner) {
      Schema schema = extractIfOption(partner);
      Preconditions.checkArgument(schema.getType() == Schema.Type.ARRAY,
          "Cannot merge Hive type %s with Avro type %s", ObjectInspector.Category.LIST, schema.getType());
      return schema.getElementType();
    }
  }

  private static Schema extractIfOption(Schema schema) {
    if (AvroSchemaUtil.isOptionSchema(schema)) {
      return AvroSchemaUtil.fromOption(schema);
    } else {
      return schema;
    }
  }
}
