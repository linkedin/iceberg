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

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.codehaus.jackson.node.JsonNodeFactory;


public class HiveTypeToAvroType {
  private int recordCounter;

  private final boolean mkFieldsOptional;

  // Additional numeric type, similar to other logical type names in AvroSerde
  private static final String SHORT_TYPE_NAME = "short";
  private static final String BYTE_TYPE_NAME = "byte";

  public HiveTypeToAvroType(boolean mkFieldsOptional) {
    this.recordCounter = 0;
    this.mkFieldsOptional = mkFieldsOptional;
  }

  Schema convertTypeInfoToAvroSchema(TypeInfo typeInfo, String recordNamespace, String recordName) {
    Schema schema;
    ObjectInspector.Category category = typeInfo.getCategory();

    switch (category) {
      case STRUCT:
        // We don't cache the structType because otherwise it could be possible that a field
        // "lastname" is of type "firstname", where firstname is a compiled class.
        // This will lead to ambiguity.
        schema = parseSchemaFromStruct((StructTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      case LIST:
        schema = parseSchemaFromList((ListTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      case MAP:
        schema = parseSchemaFromMap((MapTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      case PRIMITIVE:
        schema = parseSchemaFromPrimitive((PrimitiveTypeInfo) typeInfo);
        break;
      case UNION:
        schema = parseSchemaFromUnion((UnionTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      default:
        throw new UnsupportedOperationException("Conversion from " + category + " is not supported");
    }

    return schema;
  }

  private Schema parseSchemaFromUnion(UnionTypeInfo unionTypeInfo, final String recordNamespace,
                                      final String recordName) {
    List<TypeInfo> typeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();

    // A union might contain duplicate struct typeinfos because the underlying Avro union has two Record types with
    // different names but the same internal structure.
    // In the case of duplicate typeinfos, we generate a new record type for each struct typeinfo.

    List<Schema> schemas = new ArrayList<>();

    for (TypeInfo typeInfo : typeInfos) {
      Schema candidate;
      if (typeInfo instanceof StructTypeInfo) {
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;

        // In case we have several structType in the same level,
        // we need to add numbers to the record name to distinguish them from each other.
        final String newRecordName = recordName + recordCounter;
        recordCounter += 1;

        candidate = parseSchemaFromStruct(structTypeInfo, recordNamespace, newRecordName);
      } else { // not a struct type
        candidate = convertTypeInfoToAvroSchema(typeInfo, recordNamespace, recordName);
      }
      schemas.add(candidate);
    }

    return Schema.createUnion(schemas);
  }

  // Previously, Hive use recordType[N] as the recordName for each structType,
  // with the change we made in LIHADOOP-36761, the new record name will be in the form of "structNamespace.structName"
  private Schema parseSchemaFromStruct(final StructTypeInfo typeInfo, final String recordNamespace,
      final String recordName) {
    final List<Schema.Field> avroFields = new ArrayList<>();

    List<String> fieldNames =  typeInfo.getAllStructFieldNames();
    for (String fieldName : fieldNames) {
      final TypeInfo fieldTypeInfo = typeInfo.getStructFieldTypeInfo(fieldName);

      // If there's a structType in the schema, we will use "recordNamespace.fieldName" instead of the
      // autogenerated record name. The recordNamespace is composed of its parent's field names recursively.
      // This mimics the logic of spark-avro.
      // We will set the recordName to be capitalized, and the recordNameSpace will be in lower case
      final Schema schema = convertTypeInfoToAvroSchema(fieldTypeInfo, recordNamespace + "." + recordName.toLowerCase(),
              fieldName);
      final Schema.Field avroField = new Schema.Field(fieldName, schema, null, null);
      avroFields.add(avroField);
    }

    final Schema recordSchema = Schema.createRecord(StringUtils.capitalize(recordName), null, recordNamespace, false);
    recordSchema.setFields(avroFields);
    return recordSchema;
  }

  private Schema parseSchemaFromList(final ListTypeInfo typeInfo, final String recordNamespace,
      final String recordName) {
    Schema listSchema = convertTypeInfoToAvroSchema(typeInfo.getListElementTypeInfo(), recordNamespace, recordName);
    return Schema.createArray(listSchema);
  }

  private Schema parseSchemaFromMap(final MapTypeInfo typeInfo, final String recordNamespace, final String recordName) {
    final TypeInfo keyTypeInfo = typeInfo.getMapKeyTypeInfo();
    final PrimitiveObjectInspector.PrimitiveCategory pc = ((PrimitiveTypeInfo) keyTypeInfo).getPrimitiveCategory();
    if (pc != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UnsupportedOperationException("Key of Map can only be a String");
    }

    final TypeInfo valueTypeInfo = typeInfo.getMapValueTypeInfo();
    final Schema valueSchema = convertTypeInfoToAvroSchema(valueTypeInfo, recordNamespace, recordName);

    return Schema.createMap(valueSchema);
  }

  private Schema parseSchemaFromPrimitive(PrimitiveTypeInfo primitiveTypeInfo) {
    Schema schema;
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case LONG:
        schema = Schema.create(Schema.Type.LONG);
        break;

      case DATE:
        schema = Schema.create(Schema.Type.INT);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, AvroSerDe.DATE_TYPE_NAME);
        break;

      case TIMESTAMP:
        schema = Schema.create(Schema.Type.LONG);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, AvroSerDe.TIMESTAMP_TYPE_NAME);
        break;

      case BINARY:
        schema = Schema.create(Schema.Type.BYTES);
        break;
      case BOOLEAN:
        schema = Schema.create(Schema.Type.BOOLEAN);
        break;

      case DOUBLE:
        schema = Schema.create(Schema.Type.DOUBLE);
        break;

      case DECIMAL:
        DecimalTypeInfo dti = (DecimalTypeInfo) primitiveTypeInfo;
        JsonNodeFactory factory = JsonNodeFactory.instance;
        schema = Schema.create(Schema.Type.BYTES);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, AvroSerDe.DECIMAL_TYPE_NAME);
        schema.addProp(AvroSerDe.AVRO_PROP_PRECISION, factory.numberNode(dti.getPrecision()));
        schema.addProp(AvroSerDe.AVRO_PROP_SCALE, factory.numberNode(dti.getScale()));
        break;

      case FLOAT:
        schema = Schema.create(Schema.Type.FLOAT);
        break;

      case BYTE:
        schema = Schema.create(Schema.Type.INT);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, BYTE_TYPE_NAME);
        break;

      case SHORT:
        schema = Schema.create(Schema.Type.INT);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, SHORT_TYPE_NAME);
        break;

      case INT:
        schema = Schema.create(Schema.Type.INT);
        break;

      case CHAR:
      case STRING:
      case VARCHAR:
        schema = Schema.create(Schema.Type.STRING);
        break;

      case VOID:
        schema = Schema.create(Schema.Type.NULL);
        break;

      default:
        throw new UnsupportedOperationException(primitiveTypeInfo + " is not supported.");
    }
    return schema;
  }
}
