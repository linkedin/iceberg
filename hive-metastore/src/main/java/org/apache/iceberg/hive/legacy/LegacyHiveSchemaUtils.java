package org.apache.iceberg.hive.legacy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LegacyHiveSchemaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LegacyHiveSchemaUtils.class);

  public static Schema mergeSchemas(Schema metastoreSchema, Schema inferredSchema) {
    // metastoreSchema does not have nullable unions, all the default values are null
    // inferredSchema has the information from the avro.schema.literal, it may have default values,
    // and it may have nullable unions as the column types
    List<Schema.Field> metastoreFields = metastoreSchema.getFields();
    List<Schema.Field> inferredFields = inferredSchema.getFields();

    Map<String, Schema.Field> metastoreFieldsMap = new HashMap<>();
    for (Schema.Field field : metastoreFields) {
      // We use lowercase field name to check whether a field is an old field or newly added field
      metastoreFieldsMap.put(field.name().toLowerCase(), field);
    }

    String recordName = metastoreSchema.getName();
    String recordNamespace = metastoreSchema.getNamespace();

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(recordName).namespace(recordNamespace).fields();

    for (Schema.Field inferredField : inferredFields) {
      String inferredFieldName = inferredField.name();
      Schema inferredFieldSchema = inferredField.schema();

      String inferredFieldNameLowercase = inferredFieldName.toLowerCase();
      Schema.Type inferredFieldSchemaActualType = extractActualTypeIfFieldIsNullableType(inferredFieldSchema).getType();

      boolean isNewlyAddedField = !metastoreFieldsMap.containsKey(inferredFieldNameLowercase);

      switch (inferredFieldSchemaActualType) {
        case BOOLEAN:
          if (isNewlyAddedField) {
            //TODO: default value?
            fieldAssembler.name(inferredFieldName).type().optional().booleanType();
          } else {
            appendMergedField(metastoreFieldsMap.get(inferredFieldNameLowercase), inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case BYTES:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName).type().optional().bytesType();
          } else {
            appendMergedField(metastoreFieldsMap.get(inferredFieldNameLowercase), inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case DOUBLE:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName).type().optional().doubleType();
          } else {
            appendMergedField(metastoreFieldsMap.get(inferredFieldNameLowercase), inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case ENUM:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName)
                .type()
                .optional()
                .enumeration(inferredFieldSchema.getName())
                .symbols(inferredFieldSchema.getEnumSymbols().toArray(new String[0]));
          } else {
            appendMergedField(metastoreFieldsMap.get(inferredFieldNameLowercase), inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case FIXED:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName)
                .type()
                .optional()
                .fixed(inferredFieldSchema.getName())
                .size(inferredFieldSchema.getFixedSize());
          } else {
            appendMergedField(metastoreFieldsMap.get(inferredFieldNameLowercase), inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case FLOAT:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName).type().optional().floatType();
          } else {
            appendMergedField(metastoreFieldsMap.get(inferredFieldNameLowercase), inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case INT:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName).type().optional().intType();
          } else {
            appendMergedField(metastoreFieldsMap.get(inferredFieldNameLowercase), inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case LONG:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName).type().optional().longType();
          } else {
            appendMergedField(metastoreFieldsMap.get(inferredFieldNameLowercase), inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case STRING:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName).type().optional().stringType();
          } else {
            appendMergedField(metastoreFieldsMap.get(inferredFieldNameLowercase), inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case MAP:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName).type().optional().map().values(extractActualTypeIfFieldIsNullableType(inferredFieldSchema.getValueType()));
          } else {
            Schema.Field metastoreField = metastoreFieldsMap.get(inferredFieldNameLowercase);
            Schema metastoreFieldSchema = metastoreField.schema();

            Schema evolvedMapSchema = evolveArrayAndMapFieldSchema(metastoreFieldSchema, inferredFieldSchema, Schema.Type.MAP);
            Schema.Field evolvedMapField = new Schema.Field(metastoreField.name(),
                evolvedMapSchema,
                metastoreField.doc(),
                metastoreField.defaultVal());


            appendMergedField(evolvedMapField, inferredField, inferredFieldSchemaActualType, fieldAssembler);
          }
          break;
        case ARRAY:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName).type().optional().array().items(extractActualTypeIfFieldIsNullableType(inferredFieldSchema.getElementType()));
          } else {
            Schema.Field metastoreField = metastoreFieldsMap.get(inferredFieldNameLowercase);
            Schema metastoreFieldSchema = metastoreField.schema();

            Schema evolvedArraySchema = evolveArrayAndMapFieldSchema(metastoreFieldSchema, inferredFieldSchema, Schema.Type.ARRAY);
            Schema.Field evolvedArrayField = new Schema.Field(metastoreField.name(),
                evolvedArraySchema,
                metastoreField.doc(),
                metastoreField.defaultVal());

            appendMergedField(evolvedArrayField, inferredField, inferredFieldSchemaActualType, fieldAssembler);

          }
          break;
        case RECORD:
          if (isNewlyAddedField) {
            fieldAssembler.name(inferredFieldName).type().unionOf().nullType().and().type(extractActualTypeIfFieldIsNullableType(inferredFieldSchema)).endUnion().nullDefault();
            break;
          } else {
            Schema metastoreField = metastoreFieldsMap.get(inferredFieldNameLowercase).schema();
            if (metastoreField.getType().equals(Schema.Type.RECORD)) {
              Schema evolvedSchema = mergeSchemas(metastoreField, inferredFieldSchema);
              fieldAssembler.name(inferredFieldName).type(evolvedSchema).noDefault();
              break;
            } else if (inferredFieldSchema.getType().equals(Schema.Type.UNION)) {
              boolean isRecordFound = false;
              for (Schema field : inferredFieldSchema.getTypes()) {
                if (field.getType().equals(Schema.Type.RECORD)) {
                  isRecordFound = true;
                  Schema evolvedSchema = mergeSchemas(metastoreSchema, field);
                  fieldAssembler.name(inferredFieldName)
                      .type().unionOf().nullType().and().type(evolvedSchema).endUnion().nullDefault();

                  break;
                }
              }

              if (isRecordFound) {
                break;
              } else {
                throw new IllegalArgumentException("New Schema is RECORD type, "
                    + "while old Schema is UNION type without RECORD type in it");
              }
            }

            throw new IllegalArgumentException("New Schema is RECORD type, "
                + "while old Schema is nether RECORD type nor UNION type");
          }
        case UNION:
          throw new IllegalArgumentException("We do not support UNION type "
              + "except nullable field which is handled in other types");
        default:
          throw new IllegalArgumentException("Unsupported Schema type: " + inferredFieldSchema.getType().toString());
      }
    }

    return fieldAssembler.endRecord();
  }

  private static Schema evolveArrayAndMapFieldSchema(Schema metastoreFieldSchema,
      Schema inferredFieldSchema,
      Schema.Type schemaType) {
    Schema evolvedComplexSchema = null;

    if (inferredFieldSchema.getType().equals(schemaType)) {
      Schema evolvedInnerSchema = generateEvolvedArrayAndMapInnerSchema(metastoreFieldSchema, inferredFieldSchema);
      evolvedComplexSchema = schemaType.equals(Schema.Type.MAP) ? Schema.createMap(evolvedInnerSchema)
          : Schema.createArray(evolvedInnerSchema);
    } else if (inferredFieldSchema.getType().equals(Schema.Type.UNION)) {
      Schema complexNonNullableSchema = extractTargetTypeFromNullableUnionType(inferredFieldSchema, schemaType);
      if (complexNonNullableSchema == null) {
        throw new IllegalArgumentException("New Schema is " + schemaType.toString() + " type, "
            + "while old Schema is UNION type without " + schemaType.toString() + " type in it");
      }

      Schema evolvedInnerSchema = generateEvolvedArrayAndMapInnerSchema(metastoreFieldSchema, complexNonNullableSchema);
      Schema evolvedComplexNonNullableSchema = schemaType.equals(Schema.Type.MAP) ? Schema.createMap(evolvedInnerSchema)
          : Schema.createArray(evolvedInnerSchema);

      evolvedComplexSchema = createNullableUnionSchema(evolvedComplexNonNullableSchema);
    } else {
      throw new IllegalArgumentException("New Schema is " + schemaType.toString() + " type, "
          + "while old Schema is neither " + schemaType.toString() + " type nor UNION type");
    }

    if (evolvedComplexSchema == null) {
      throw new RuntimeException("Fail to generate evolved " + schemaType.toString() + " schema");
    }

    return evolvedComplexSchema;
  }

  private static Schema generateEvolvedArrayAndMapInnerSchema(Schema metastoreFieldSchema, Schema inferredFieldSchema) {
    if (metastoreFieldSchema == null || inferredFieldSchema == null) {
      throw new IllegalArgumentException("The input schemas cannot be null");
    }

    if (!((metastoreFieldSchema.getType().equals(Schema.Type.ARRAY) && inferredFieldSchema.getType().equals(Schema.Type.ARRAY))
        || (metastoreFieldSchema.getType().equals(Schema.Type.MAP) && inferredFieldSchema.getType().equals(Schema.Type.MAP)))) {
      throw new IllegalArgumentException("Input schemas must be of both ARRAY type or both MAP type. "
          + "Metastore schema type: " + metastoreFieldSchema.getType() + ", "
          + "inferred schema type: " + inferredFieldSchema.getType());
    }

    Schema metastoreInnerSchema = null;
    Schema inferredInnerSchema = null;
    Schema mergedInnerSchema = null;

    if (metastoreFieldSchema.getType().equals(Schema.Type.ARRAY)) {
      metastoreInnerSchema = metastoreFieldSchema.getElementType();
      inferredInnerSchema = inferredFieldSchema.getElementType();
    } else if (metastoreFieldSchema.getType().equals(Schema.Type.MAP)) {
      metastoreInnerSchema = metastoreFieldSchema.getValueType();
      inferredInnerSchema = inferredFieldSchema.getValueType();
    } else {
      throw new IllegalArgumentException("Expect ARRAY or MAP type");
    }

    switch (inferredInnerSchema.getType()) {
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
        mergedInnerSchema = metastoreInnerSchema;

        break;
      case MAP:
        mergedInnerSchema = evolveArrayAndMapFieldSchema(metastoreInnerSchema, inferredInnerSchema, Schema.Type.MAP);

        break;
      case ARRAY:
        mergedInnerSchema = evolveArrayAndMapFieldSchema(metastoreInnerSchema, inferredInnerSchema, Schema.Type.ARRAY);

        break;
      case RECORD:
        if (inferredInnerSchema.getType().equals(Schema.Type.RECORD)) {
          mergedInnerSchema = mergeSchemas(metastoreInnerSchema, inferredInnerSchema);
        } else if (inferredInnerSchema.getType().equals(Schema.Type.UNION)) {
          Schema recordTypeSchema = extractTargetTypeFromNullableUnionType(inferredInnerSchema, Schema.Type.RECORD);
          if (recordTypeSchema == null) {
            throw new IllegalArgumentException("Inferred element Schema is RECORD type, "
                + "while metastore element Schema is UNION type without RECORD type in it");
          }

          Schema evolvedElementSchemaNonnullable = mergeSchemas(metastoreFieldSchema, recordTypeSchema);
          mergedInnerSchema = createNullableUnionSchema(evolvedElementSchemaNonnullable);
        } else {
          throw new IllegalArgumentException("New element Schema is RECORD type, "
              + "while old element Schema is neither RECORD type nor UNION type");
        }

        break;
      case UNION:
        throw new IllegalArgumentException("We do not support UNION type "
            + "except nullable field which is handled in other types");
      default:
        throw new IllegalArgumentException("Unsupported Schema type: " + inferredFieldSchema.getType().toString());
    }

    if (mergedInnerSchema == null) {
      throw new RuntimeException("Fail to generate evolved schema");
    }

    return mergedInnerSchema;
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

  private static Schema extractTargetTypeFromNullableUnionType(Schema unionSchema, Schema.Type targetType) {
    if (unionSchema == null
        || !unionSchema.getType().equals(Schema.Type.UNION)
        || !AvroSerdeUtils.isNullableType(unionSchema)) {
      throw new IllegalArgumentException("Input schemas must be of nullable UNION type.");
    }

    for (Schema field : unionSchema.getTypes()) {
      if (field.getType().equals(targetType)) {
        return field;
      }
    }

    return null;
  }


  public static Schema extractActualTypeIfFieldIsNullableTypeRecord(Schema schema) {
    if (schema == null) {
      return null;
    }

    if (!schema.getType().equals(Schema.Type.RECORD)) {
      throw new IllegalArgumentException("Input schemas must be of RECORD type. "
          + "Actual schema type is: " + schema.getType());
    }

    List<Schema.Field> fields = schema.getFields();

    String recordName = schema.getName();
    String recordNamespace = schema.getNamespace();

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(recordName)
        .namespace(recordNamespace)
        .fields();

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
          throw new IllegalArgumentException(fieldName + "is of type UNION. We do not support UNION type "
              + "except nullable field.");
        default:
          throw new IllegalArgumentException("Unsupported Schema type: " + schema.getType().toString()
              + " for field: " + fieldName);
      }
    }

    return fieldAssembler.endRecord();
  }

  private static Schema extractActualTypeIfFieldIsNullableType(Schema schema) {
    if (schema == null) {
      return null;
    }

    if (AvroSerdeUtils.isNullableType(schema)) {
      schema = AvroSerdeUtils.getOtherTypeFromNullableType(schema);
    }

    switch (schema.getType()) {
      case BOOLEAN:
      case BYTES:
      case DOUBLE:
      case ENUM:
      case FIXED:
      case FLOAT:
      case INT:
      case LONG:
      case STRING:
      case MAP:
      case ARRAY:
        return schema;
      case RECORD:
        Schema recordSchema = extractActualTypeIfFieldIsNullableTypeRecord(schema);
        return recordSchema;
      case UNION:
        throw new IllegalArgumentException(schema.toString(true) + " is unsupported UNION type. "
            + "Only nullable field is supported");
      default:
        throw new IllegalArgumentException("Unsupported Schema type: " + schema.getType().toString());
    }
  }

  /**
   * This method add field into fieldAssembler
   * It handles default value of field properly
   *
   * @param metastoreField Schema.Field from the hive metastore
   * @param fieldAssembler SchemaBuilder.FieldAssembler<Schema> to build the schema
   */
  private static void appendMergedField(Schema.Field metastoreField,
      Schema.Field inferredField,
      Schema.Type inferredFieldActualType,
      SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
    Object defaultValue = metastoreField.schema().getType().equals(inferredFieldActualType) ? inferredField.defaultVal() : null;
    if (defaultValue != null) {
      fieldAssembler.name(inferredField.name()).type(metastoreField.schema()).withDefault(defaultValue);
    } else {
      fieldAssembler.name(inferredField.name()).type(metastoreField.schema()).noDefault();
    }
  }
}