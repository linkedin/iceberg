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
    if (partner == null) {
      // if there was no matching Avro struct, return an optional struct with new record/namespace
      int recordNum = recordCounter.incrementAndGet();
      return AvroSchemaUtil.toOption(
          Schema.createRecord("record" + recordNum, null, "namespace" + recordNum, false, fieldResults));
    } else if (AvroSchemaUtil.isOptionSchema(partner)) {
      // if the matching Avro struct was an option, return an optional struct by copying the original record
      // but with our new fields
      return AvroSchemaUtil.toOption(
          AvroSchemaUtil.copyRecord(AvroSchemaUtil.fromOption(partner), fieldResults, null));
    } else {
      return AvroSchemaUtil.copyRecord(partner, fieldResults, null);
    }
  }

  @Override
  public Schema.Field field(String name, TypeInfo field, Schema.Field partner, Schema fieldResult) {
    if (partner == null) {
      // if there was no matching Avro field, return an optional field with null default
      // here we expect and assert that other visitor method will always return an optional schema in case their
      // partner is missing
      Preconditions.checkArgument(AvroSchemaUtil.isOptionSchema(fieldResult),
          "Expected an option schema for a missing Avro field. Found: %s", fieldResult);
      return new Schema.Field(
          AvroSchemaUtil.makeCompatibleName(name), fieldResult, null, Schema.Field.NULL_DEFAULT_VALUE);
    } else {
      // TODO: How to ensure that field default value is compatible with new field type generated from Hive?
      // Copy field type from the visitor result, copy everything else from the partner
      // Avro requires the default value to match the first type in the option, reorder option if required
      Schema reordered = reorderOptionIfRequired(fieldResult, partner.defaultVal());
      return AvroSchemaUtil.copyField(partner, reordered, partner.name());
    }
  }

  /**
   * Reorders an option schema so that the type of the provided default value is the first type in the option schema
   *
   * e.g. If the schema is (NULL, INT) and the default value is 1, the returned schema is (INT, NULL)
   * If the schema is not an option schema or if there is no default value, schema is returned as-is
   */
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
      // if there was no matching Avro list, or if matching Avro list was an option, return an optional list
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
      // if there was no matching Avro map, or if matching Avro map was an option, return an optional map
      return AvroSchemaUtil.toOption(Schema.createMap(valueResult));
    } else {
      return Schema.createMap(valueResult);
    }
  }

  @Override
  public Schema primitive(PrimitiveTypeInfo primitive, Schema partner) {
    Schema schema = HiveTypeUtil.visit(primitive, hiveToAvro);
    if (partner == null) {
      // if there was no matching Avro primitive, return an optional primitive
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

  /**
   * A {@link PartnerAccessors} which matches the requested field from a partner Avro struct by case insensitive
   * field name match
   */
  private static class AvroPartnerAccessor implements PartnerAccessors<Schema, Schema.Field> {
    private static final AvroPartnerAccessor INSTANCE = new AvroPartnerAccessor();

    private static final Schema MAP_KEY = Schema.create(Schema.Type.STRING);

    @Override
    public Schema.Field fieldPartner(Schema partnerStruct, String fieldName) {
      Schema struct = extractIfOption(partnerStruct);
      Preconditions.checkArgument(struct.getType() == Schema.Type.RECORD,
          "Cannot merge Hive type %s with Avro type %s", ObjectInspector.Category.STRUCT, struct.getType());
      // TODO: Optimize? This will be called for every struct field, we will run the for loop for every struct field
      for (Schema.Field field : struct.getFields()) {
        if (field.name().equalsIgnoreCase(fieldName)) {
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
    public Schema mapKeyPartner(Schema partnerMap) {
      return MAP_KEY;
    }

    @Override
    public Schema mapValuePartner(Schema partnerMap) {
      Schema map = extractIfOption(partnerMap);
      Preconditions.checkArgument(map.getType() == Schema.Type.MAP,
          "Cannot merge Hive type %s with Avro type %s", ObjectInspector.Category.MAP, map.getType());
      return map.getValueType();
    }

    @Override
    public Schema listElementPartner(Schema partnerList) {
      Schema list = extractIfOption(partnerList);
      Preconditions.checkArgument(list.getType() == Schema.Type.ARRAY,
          "Cannot merge Hive type %s with Avro type %s", ObjectInspector.Category.LIST, list.getType());
      return list.getElementType();
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
