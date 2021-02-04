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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LegacyHiveSchemaUtils {
  private LegacyHiveSchemaUtils() {}

  private static final Logger LOG = LoggerFactory.getLogger(LegacyHiveSchemaUtils.class);

  static Schema convertHiveSchemaToAvro(@Nonnull final Table table) {
    Preconditions.checkNotNull(table, "table cannot be null");

    String recordName = table.getTableName();
    String recordNamespace = table.getDbName() + "." + recordName;

    final List<FieldSchema> cols = new ArrayList<>();

    cols.addAll(table.getSd().getCols());
    if (isPartitioned(table)) {
      cols.addAll(getPartitionCols(table));
    }

    return convertFieldSchemaToAvroSchema(recordName, recordNamespace, true, cols);
  }

  private static Schema convertFieldSchemaToAvroSchema(@Nonnull final String recordName,
      @Nonnull final String recordNamespace, @Nonnull final boolean mkFieldsOptional,
      @Nonnull final List<FieldSchema> columns) {
    final List<String> columnNames = new ArrayList<>(columns.size());
    final List<TypeInfo> columnsTypeInfo = new ArrayList<>(columns.size());

    columns.forEach(fs -> {
      columnNames.add(fs.getName());
      columnsTypeInfo.add(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
    });

    return new HiveTypeToAvroType(recordNamespace, mkFieldsOptional).convertFieldsTypeInfoToAvroSchema("",
        getStandardName(recordName), columnNames, columnsTypeInfo);
  }

  private static String getStandardName(@Nonnull String name) {
    String[] sArr = name.split("_");
    StringBuilder sb = new StringBuilder();
    for (String str : sArr) {
      sb.append(StringUtils.capitalize(str));
    }
    return sb.toString();
  }

  static Schema extractActualTypeIfFieldIsNullableTypeRecord(Schema schema) {
    if (schema == null) {
      return null;
    }

    if (!schema.getType().equals(Schema.Type.RECORD)) {
      throw new IllegalArgumentException(
          "Input schemas must be of RECORD type. Actual schema type is: " + schema.getType());
    }

    List<Schema.Field> fields = schema.getFields();

    String recordName = schema.getName();
    String recordNamespace = schema.getNamespace();

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler =
        SchemaBuilder.record(recordName).namespace(recordNamespace).fields();

    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Schema fieldSchema = extractActualTypeIfFieldIsNullableType(field.schema());

      switch (fieldSchema.getType()) {
        case BOOLEAN:
        case BYTES:
        case DOUBLE:
        case ENUM:
        case FIXED:
        case FLOAT:
        case INT:
        case LONG:
        case STRING:
          fieldAssembler.name(fieldName).type(fieldSchema).noDefault();
          break;
        case MAP:
          Schema valueSchema = extractActualTypeIfFieldIsNullableType(fieldSchema.getValueType());
          fieldAssembler.name(fieldName).type().map().values(valueSchema).noDefault();
          break;
        case ARRAY:
          Schema elementSchema = extractActualTypeIfFieldIsNullableType(fieldSchema.getElementType());
          fieldAssembler.name(fieldName).type().array().items(elementSchema).noDefault();
          break;
        case RECORD:
          fieldAssembler.name(fieldName).type(fieldSchema).noDefault();
          break;
        case UNION:
          throw new IllegalArgumentException(
              fieldName + "is of type UNION. We do not support UNION type except nullable field.");
        default:
          throw new IllegalArgumentException(
              "Unsupported Schema type: " + schema.getType().toString() + " for field: " + fieldName);
      }
    }

    return fieldAssembler.endRecord();
  }

  private static Schema extractActualTypeIfFieldIsNullableType(Schema nullableType) {
    if (nullableType == null) {
      return null;
    }

    Schema actualType = nullableType;
    if (AvroSerdeUtils.isNullableType(nullableType)) {
      actualType = AvroSerdeUtils.getOtherTypeFromNullableType(nullableType);
    }

    switch (actualType.getType()) {
      case BOOLEAN:
      case BYTES:
      case DOUBLE:
      case ENUM:
      case FIXED:
      case FLOAT:
      case INT:
      case LONG:
      case STRING:
        return actualType;
      case MAP:
        Schema valueSchema = extractActualTypeIfFieldIsNullableType(actualType.getValueType());
        Schema mapSchema = Schema.createMap(valueSchema);

        return mapSchema;
      case ARRAY:
        Schema elementSchema = extractActualTypeIfFieldIsNullableType(actualType.getElementType());
        Schema arraySchema = Schema.createArray(elementSchema);

        return arraySchema;
      case RECORD:
        Schema recordSchema = extractActualTypeIfFieldIsNullableTypeRecord(actualType);
        return recordSchema;
      case UNION:
        throw new IllegalArgumentException(
            actualType.toString(true) + " is unsupported UNION type. Only nullable field is supported");
      default:
        throw new IllegalArgumentException("Unsupported Schema type: " + actualType.getType().toString());
    }
  }

  /**
   * This method takes schema field and type in sourceSchema as source of truth and applies
   * metadata such as casing and nullability to sourceSchema to generate a merged schema
   *
   * @param metaDataSchema schema get from table property avro.schema.literal
   * @param sourceSchema schema converted from Hive schema
   * @return merged schema
   */
  static Schema mergeRecordSchema(Schema metaDataSchema, Schema sourceSchema) {
    validateInputRecordSchema(metaDataSchema, sourceSchema);

    List<Schema.Field> metaDataFields = metaDataSchema.getFields();
    List<Schema.Field> sourceFields = sourceSchema.getFields();

    Map<String, Queue<Schema.Field>> metaDataFieldsMap = new HashMap<>();
    generateMetaDataFieldsMap(metaDataFields, metaDataFieldsMap);

    String recordName = metaDataSchema.getName();
    String recordNamespace = metaDataSchema.getNamespace();

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler =
        SchemaBuilder.record(recordName).namespace(recordNamespace).fields();

    for (Schema.Field sourceField : sourceFields) {
      String sourceFieldName = sourceField.name();
      Schema sourceFieldSchema = sourceField.schema();

      String sourceFieldNameLowercase = sourceFieldName.toLowerCase();

      boolean isNewlyAddedField = !metaDataFieldsMap.containsKey(sourceFieldNameLowercase);

      switch (sourceFieldSchema.getType()) {
        case BOOLEAN:
          if (isNewlyAddedField) {
            fieldAssembler.name(sourceFieldName).type().optional().booleanType();
          } else {
            mergeAndAppendField(sourceField, metaDataFieldsMap.get(sourceFieldNameLowercase).poll(), fieldAssembler);
          }
          break;
        case BYTES:
          if (isNewlyAddedField) {
            fieldAssembler.name(sourceFieldName).type().optional().bytesType();
          } else {
            mergeAndAppendField(sourceField, metaDataFieldsMap.get(sourceFieldNameLowercase).poll(), fieldAssembler);
          }
          break;
        case DOUBLE:
          if (isNewlyAddedField) {
            fieldAssembler.name(sourceFieldName).type().optional().doubleType();
          } else {
            mergeAndAppendField(sourceField, metaDataFieldsMap.get(sourceFieldNameLowercase).poll(), fieldAssembler);
          }
          break;
        case ENUM:
          if (isNewlyAddedField) {
            fieldAssembler.name(sourceFieldName).type().optional().enumeration(sourceFieldSchema.getName())
                .symbols(sourceFieldSchema.getEnumSymbols().toArray(new String[0]));
          } else {
            mergeAndAppendField(sourceField, metaDataFieldsMap.get(sourceFieldNameLowercase).poll(), fieldAssembler);
          }
          break;
        case FIXED:
          if (isNewlyAddedField) {
            fieldAssembler.name(sourceFieldName).type().optional().fixed(sourceFieldSchema.getName())
                .size(sourceFieldSchema.getFixedSize());
          } else {
            mergeAndAppendField(sourceField, metaDataFieldsMap.get(sourceFieldNameLowercase).poll(), fieldAssembler);
          }
          break;
        case FLOAT:
          if (isNewlyAddedField) {
            fieldAssembler.name(sourceFieldName).type().optional().floatType();
          } else {
            mergeAndAppendField(sourceField, metaDataFieldsMap.get(sourceFieldNameLowercase).poll(), fieldAssembler);
          }
          break;
        case INT:
          if (isNewlyAddedField) {
            fieldAssembler.name(sourceFieldName).type().optional().intType();
          } else {
            mergeAndAppendField(sourceField, metaDataFieldsMap.get(sourceFieldNameLowercase).poll(), fieldAssembler);
          }
          break;
        case LONG:
          if (isNewlyAddedField) {
            fieldAssembler.name(sourceFieldName).type().optional().longType();
          } else {
            mergeAndAppendField(sourceField, metaDataFieldsMap.get(sourceFieldNameLowercase).poll(), fieldAssembler);
          }
          break;
        case STRING:
          if (isNewlyAddedField) {
            fieldAssembler.name(sourceFieldName).type().optional().stringType();
          } else {
            mergeAndAppendField(sourceField, metaDataFieldsMap.get(sourceFieldNameLowercase).poll(), fieldAssembler);
          }
          break;
        case MAP:
          handleMapSchemaMerge(metaDataFieldsMap, fieldAssembler, sourceField, sourceFieldName, sourceFieldSchema,
              sourceFieldNameLowercase, isNewlyAddedField);
          break;
        case ARRAY:
          handleArraySchemaMerge(metaDataFieldsMap, fieldAssembler, sourceField, sourceFieldName, sourceFieldSchema,
              sourceFieldNameLowercase, isNewlyAddedField);
          break;
        case RECORD:
          handleRecordSchemaMerge(metaDataFieldsMap, fieldAssembler, sourceFieldName, sourceFieldSchema,
              sourceFieldNameLowercase, isNewlyAddedField);
          break;
        case UNION:
          throw new IllegalArgumentException(
              "We do not support UNION type except nullable field which is handled in other types");
        default:
          throw new IllegalArgumentException("Unsupported Schema type: " + sourceFieldSchema.getType().toString());
      }
    }

    return fieldAssembler.endRecord();
  }

  private static void generateMetaDataFieldsMap(List<Schema.Field> metaDataFields,
      Map<String, Queue<Schema.Field>> metaDataFieldsMap) {
    for (Schema.Field field : metaDataFields) {
      // The field name of sourceSchema is from hive thus case sensitivity info can be lost in some cases
      // We use lowercase field name to check whether a field is an metaData field or source field
      String fieldLowercase = field.name().toLowerCase();
      if (metaDataFieldsMap.containsKey(fieldLowercase)) {
        metaDataFieldsMap.get(fieldLowercase).offer(field);
      } else {
        Queue<Schema.Field> fieldsQueue = new LinkedList<>();
        fieldsQueue.offer(field);
        metaDataFieldsMap.put(fieldLowercase, fieldsQueue);
      }
    }
  }

  private static void handleMapSchemaMerge(Map<String, Queue<Schema.Field>> metaDataFieldsMap,
      SchemaBuilder.FieldAssembler<Schema> fieldAssembler, Schema.Field sourceField, String sourceFieldName,
      Schema sourceFieldSchema, String sourceFieldNameLowercase, boolean isNewlyAddedField) {
    if (isNewlyAddedField) {
      fieldAssembler.name(sourceFieldName).type().optional().map().values(sourceFieldSchema.getValueType());
    } else {
      Schema.Field oldField = metaDataFieldsMap.get(sourceFieldNameLowercase).poll();
      Schema oldFieldSchema = oldField.schema();

      Schema evolvedMapSchema = mergeArrayAndMapFieldSchema(oldFieldSchema, sourceFieldSchema, Schema.Type.MAP);
      Schema.Field evolvedMapField =
          new Schema.Field(oldField.name(), evolvedMapSchema, oldField.doc(), oldField.defaultVal());

      mergeAndAppendField(sourceField, evolvedMapField, fieldAssembler);
    }
  }

  private static void handleArraySchemaMerge(Map<String, Queue<Schema.Field>> metaDataFieldsMap,
      SchemaBuilder.FieldAssembler<Schema> fieldAssembler, Schema.Field sourceField, String sourceFieldName,
      Schema sourceFieldSchema, String sourceFieldNameLowercase, boolean isNewlyAddedField) {
    if (isNewlyAddedField) {
      fieldAssembler.name(sourceFieldName).type().optional().array().items(sourceFieldSchema.getElementType());
    } else {
      Schema.Field oldField = metaDataFieldsMap.get(sourceFieldNameLowercase).poll();
      Schema oldFieldSchema = oldField.schema();

      Schema evolvedArraySchema =
          mergeArrayAndMapFieldSchema(oldFieldSchema, sourceFieldSchema, Schema.Type.ARRAY);
      Schema.Field evolvedArrayField =
          new Schema.Field(oldField.name(), evolvedArraySchema, oldField.doc(), oldField.defaultVal());

      mergeAndAppendField(sourceField, evolvedArrayField, fieldAssembler);
    }
  }

  private static void handleRecordSchemaMerge(Map<String, Queue<Schema.Field>> metaDataFieldsMap,
      SchemaBuilder.FieldAssembler<Schema> fieldAssembler, String sourceFieldName, Schema sourceFieldSchema,
      String sourceFieldNameLowercase, boolean isNewlyAddedField) {
    if (isNewlyAddedField) {
      fieldAssembler.name(sourceFieldName).type().unionOf().nullType().and().type(sourceFieldSchema).endUnion()
          .nullDefault();
      return;
    } else {
      Schema.Field oldField = metaDataFieldsMap.get(sourceFieldNameLowercase).poll();
      Schema oldFieldSchema = oldField.schema();
      String oldFieldName = oldField.name();

      if (oldFieldSchema.getType().equals(Schema.Type.RECORD)) {
        Schema evolvedSchema = mergeRecordSchema(oldFieldSchema, sourceFieldSchema);
        fieldAssembler.name(oldFieldName).type(evolvedSchema).noDefault();
        return;
      } else if (oldFieldSchema.getType().equals(Schema.Type.UNION)) {
        boolean isRecordFound = false;
        for (Schema field : oldFieldSchema.getTypes()) {
          if (field.getType().equals(Schema.Type.RECORD)) {
            isRecordFound = true;
            Schema evolvedSchema = mergeRecordSchema(field, sourceFieldSchema);
            fieldAssembler.name(oldFieldName).type().unionOf().nullType().and().type(evolvedSchema).endUnion()
                .noDefault();

            break;
          }
        }

        if (isRecordFound) {
          return;
        } else {
          throw new IllegalArgumentException(
              "New Schema is RECORD type, while old Schema is UNION type without RECORD type in it");
        }
      }

      throw new IllegalArgumentException(
          "New Schema is RECORD type, while old Schema is nether RECORD type nor UNION type");
    }
  }

  private static Schema mergeArrayAndMapFieldSchema(Schema oldFieldSchema, Schema newFieldSchema,
      Schema.Type schemaType) {
    Schema evolvedComplexSchema = null;

    if (oldFieldSchema.getType().equals(schemaType)) {
      Schema evolvedInnerSchema = mergeEvolvedArrayAndMapInnerSchema(oldFieldSchema, newFieldSchema);
      evolvedComplexSchema = schemaType.equals(Schema.Type.MAP) ? Schema.createMap(evolvedInnerSchema)
          : Schema.createArray(evolvedInnerSchema);
    } else if (oldFieldSchema.getType().equals(Schema.Type.UNION)) {
      Schema complexNonNullableSchema = extractTargetTypeFromNullableUnionType(oldFieldSchema, schemaType);
      if (complexNonNullableSchema == null) {
        throw new IllegalArgumentException("New Schema is " + schemaType.toString() + " type, " +
            "while old Schema is UNION type without " + schemaType.toString() + " type in it");
      }

      Schema evolvedInnerSchema = mergeEvolvedArrayAndMapInnerSchema(complexNonNullableSchema, newFieldSchema);
      Schema evolvedComplexNonNullableSchema = schemaType.equals(Schema.Type.MAP) ? Schema.createMap(evolvedInnerSchema)
          : Schema.createArray(evolvedInnerSchema);

      evolvedComplexSchema = createNullableUnionSchema(evolvedComplexNonNullableSchema);
    } else {
      throw new IllegalArgumentException("New Schema is " + schemaType.toString() + " type, " +
          "while old Schema is neither " + schemaType.toString() + " type nor UNION type");
    }

    if (evolvedComplexSchema == null) {
      throw new RuntimeException("Fail to generate evolved " + schemaType.toString() + " schema");
    }

    return evolvedComplexSchema;
  }

  private static Schema mergeEvolvedArrayAndMapInnerSchema(Schema oldFieldSchema, Schema newFieldSchema) {
    if (oldFieldSchema == null || newFieldSchema == null) {
      throw new IllegalArgumentException("The input schemas cannot be null");
    }

    if (!((oldFieldSchema.getType().equals(Schema.Type.ARRAY) && newFieldSchema.getType().equals(Schema.Type.ARRAY)) ||
        (oldFieldSchema.getType().equals(Schema.Type.MAP) && newFieldSchema.getType().equals(Schema.Type.MAP)))) {
      throw new IllegalArgumentException("Input schemas must be of both ARRAY type or both MAP type. " +
          "Old schema type: " + oldFieldSchema.getType() + ", " + "new schema type: " + newFieldSchema.getType());
    }

    Schema oldInnerSchema = null;
    Schema newInnerSchema = null;
    Schema evolvedInnerSchema = null;

    if (newFieldSchema.getType().equals(Schema.Type.ARRAY)) {
      oldInnerSchema = oldFieldSchema.getElementType();
      newInnerSchema = newFieldSchema.getElementType();
    } else if (newFieldSchema.getType().equals(Schema.Type.MAP)) {
      oldInnerSchema = oldFieldSchema.getValueType();
      newInnerSchema = newFieldSchema.getValueType();
    } else {
      throw new IllegalArgumentException("Expect ARRAY or MAP type");
    }

    switch (newInnerSchema.getType()) {
      case BOOLEAN:
      case BYTES:
      case DOUBLE:
      case ENUM:
      case FIXED:
      case FLOAT:
      case INT:
      case LONG:
      case STRING:
        // we do not allow type widening at this moment
        evolvedInnerSchema = oldInnerSchema;

        break;
      case MAP:
        evolvedInnerSchema = mergeArrayAndMapFieldSchema(oldInnerSchema, newInnerSchema, Schema.Type.MAP);

        break;
      case ARRAY:
        evolvedInnerSchema = mergeArrayAndMapFieldSchema(oldInnerSchema, newInnerSchema, Schema.Type.ARRAY);

        break;
      case RECORD:
        evolvedInnerSchema = handleArrayAndMapInnerRecordSchema(oldInnerSchema, newInnerSchema);

        break;
      case UNION:
        throw new IllegalArgumentException(
            "We do not support UNION type " + "except nullable field which is handled in other types");
      default:
        throw new IllegalArgumentException("Unsupported Schema type: " + newFieldSchema.getType().toString());
    }

    if (evolvedInnerSchema == null) {
      throw new RuntimeException("Fail to generate evolved schema");
    }

    return evolvedInnerSchema;
  }

  private static Schema handleArrayAndMapInnerRecordSchema(Schema oldInnerSchema, Schema newInnerSchema) {
    Schema evolvedInnerSchema;
    if (oldInnerSchema.getType().equals(Schema.Type.RECORD)) {
      evolvedInnerSchema = mergeRecordSchema(oldInnerSchema, newInnerSchema);
    } else if (oldInnerSchema.getType().equals(Schema.Type.UNION)) {
      Schema recordTypeSchema = extractTargetTypeFromNullableUnionType(oldInnerSchema, Schema.Type.RECORD);
      if (recordTypeSchema == null) {
        throw new IllegalArgumentException("New element Schema is RECORD type, " +
            "while old element Schema is UNION type without RECORD type in it");
      }

      Schema evolvedElementSchemaNonnullable = mergeRecordSchema(recordTypeSchema, newInnerSchema);
      evolvedInnerSchema = createNullableUnionSchema(evolvedElementSchemaNonnullable);
    } else {
      throw new IllegalArgumentException(
          "New element Schema is RECORD type, " + "while old element Schema is neither RECORD type nor UNION type");
    }
    return evolvedInnerSchema;
  }

  private static void validateInputRecordSchema(Schema oldSchema, Schema newSchema) {
    if (oldSchema == null || newSchema == null) {
      throw new IllegalArgumentException("The input schemas cannot be null");
    }

    if (!oldSchema.getType().equals(Schema.Type.RECORD) || !newSchema.getType().equals(Schema.Type.RECORD)) {
      throw new IllegalArgumentException("Input schemas must be of RECORD type. " + "Old schema type: " +
          oldSchema.getType() + ", " + "new schema type: " + newSchema.getType());
    }
  }

  private static Schema extractTargetTypeFromNullableUnionType(Schema unionSchema, Schema.Type targetType) {
    if (unionSchema == null ||
        !unionSchema.getType().equals(Schema.Type.UNION) ||
        !AvroSerdeUtils.isNullableType(unionSchema)) {
      throw new IllegalArgumentException("Input schemas must be of nullable UNION type.");
    }

    for (Schema field : unionSchema.getTypes()) {
      if (field.getType().equals(targetType)) {
        return field;
      }
    }

    return null;
  }

  private static Schema createNullableUnionSchema(Schema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("The input schema cannot be null");
    }

    List<Schema> unionSchemas = new ArrayList<>();
    unionSchemas.add(Schema.create(Schema.Type.NULL));
    unionSchemas.add(schema);

    return Schema.createUnion(unionSchemas);
  }

  static boolean isRecordSchemaEvolved(Schema oldSchema, Schema newSchema) {
    validateInputRecordSchema(oldSchema, newSchema);

    if (oldSchema.toString(true).equals(newSchema.toString(true))) {
      return false;
    }

    List<Schema.Field> oldSchemaFields = oldSchema.getFields();
    List<Schema.Field> newSchemaFields = newSchema.getFields();

    Map<String, Schema.Field> oldSchemaFieldsMap = new HashMap<>();
    for (Schema.Field field : oldSchemaFields) {
      oldSchemaFieldsMap.put(field.name().toLowerCase(), field);
    }

    Map<String, Schema.Field> newSchemaFieldsMap = new HashMap<>();
    for (Schema.Field field : newSchemaFields) {
      newSchemaFieldsMap.put(field.name().toLowerCase(), field);
    }

    for (Schema.Field oldField : oldSchemaFields) {
      if (!newSchemaFieldsMap.containsKey(oldField.name().toLowerCase())) {
        // oldField is deleted in new schema
        LOG.info("Schema is evolved. The field {} is deleted in new schema", oldField.name());
        return true;
      }
    }

    for (Schema.Field newField : newSchemaFields) {
      if (!oldSchemaFieldsMap.containsKey(newField.name().toLowerCase())) {
        // oldField is added in new schema
        LOG.info("Schema is evolved. The field {} is added in new schema", newField.name());
        return true;
      }
    }

    for (Schema.Field oldField : oldSchemaFields) {
      Schema.Field newField = newSchemaFieldsMap.get(oldField.name().toLowerCase());

      if (isSchemaEvolved(oldField.schema(), newField.schema())) {
        return true;
      }
    }

    return false;
  }

  private static boolean isSchemaEvolved(Schema oldSchema, Schema newSchema) {
    if (oldSchema == null || newSchema == null) {
      throw new IllegalArgumentException("The input schemas cannot be null");
    }

    if (!oldSchema.getType().equals(newSchema.getType())) {
      if (oldSchema.getType().equals(Schema.Type.ENUM) && newSchema.getType().equals(Schema.Type.STRING)) {
        return false;
      }

      LOG.info("Schema is evolved. The old schema type is: {}, the new schema type is: {}",
          oldSchema.getType(), newSchema.getType());
      return true;
    }

    switch (newSchema.getType()) {
      case BOOLEAN:
      case BYTES:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case STRING:
      case FIXED:
        return false;
      case ENUM:
        return oldSchema.getEnumSymbols().size() != newSchema.getEnumSymbols().size();

      case RECORD:
        return isRecordSchemaEvolved(oldSchema, newSchema);

      case MAP:
        return isSchemaEvolved(oldSchema.getValueType(), newSchema.getValueType());

      case ARRAY:
        return isSchemaEvolved(oldSchema.getElementType(), newSchema.getElementType());

      case UNION:
        boolean isBothNullableType =
            AvroSerdeUtils.isNullableType(oldSchema) && AvroSerdeUtils.isNullableType(newSchema);

        if (!isBothNullableType) {
          throw new IllegalArgumentException(
              "We do not support UNION type " + "except nullable field in schema: " + newSchema.toString(true));
        }

        Schema oldNonNullType = AvroSerdeUtils.getOtherTypeFromNullableType(oldSchema);
        Schema newNonNullType = AvroSerdeUtils.getOtherTypeFromNullableType(newSchema);

        return isSchemaEvolved(oldNonNullType, newNonNullType);
      default:
        throw new IllegalArgumentException(
            "Unsupported Avro type " + newSchema.getType() + " in new schema: " + newSchema.toString(true));
    }
  }

  private static boolean isPartitioned(@Nonnull Table tableOrView) {
    Preconditions.checkNotNull(tableOrView, "tableOrView cannot be null");

    List<FieldSchema> partitionColumns = getPartitionCols(tableOrView);

    if (partitionColumns == null) {
      return false;
    }

    return partitionColumns.size() != 0;
  }

  private static List<FieldSchema> getPartitionCols(@Nonnull Table tableOrView) {
    Preconditions.checkNotNull(tableOrView, "tableOrView cannot be null");

    List<FieldSchema> partKeys = tableOrView.getPartitionKeys();
    if (partKeys == null) {
      partKeys = new ArrayList<>();
      tableOrView.setPartitionKeys(partKeys);
    }

    return partKeys;
  }

  private static void mergeAndAppendField(Schema.Field sourceField, Schema.Field metaDataField,
      SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    Schema.Type actualMetaDataFieldType = extractActualTypeIfFieldIsNullableType(metaDataField.schema()).getType();
    boolean isSameType = sourceField.schema().getType().equals(actualMetaDataFieldType);
    boolean isMetaDataFieldEnumType = metaDataField.schema().getType().equals(Schema.Type.ENUM);
    boolean useMetaDataType = isSameType || isMetaDataFieldEnumType;

    Object defaultValue = metaDataField.defaultVal();
    if (defaultValue != null) {
      fieldAssembler.name(metaDataField.name()).doc(metaDataField.doc())
          .type(useMetaDataType ? metaDataField.schema() : sourceField.schema()).withDefault(defaultValue);
    } else {
      fieldAssembler.name(metaDataField.name()).doc(metaDataField.doc())
          .type(useMetaDataType ? metaDataField.schema() : sourceField.schema()).noDefault();
    }
  }
}
