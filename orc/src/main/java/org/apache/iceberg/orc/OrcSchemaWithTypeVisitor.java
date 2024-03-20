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

package org.apache.iceberg.orc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.orc.TypeDescription;

public abstract class OrcSchemaWithTypeVisitor<T> {
  private static final String PSEUDO_ICEBERG_FIELD_ID = "-1";

  public static <T> T visit(
      org.apache.iceberg.Schema iSchema, TypeDescription schema, OrcSchemaWithTypeVisitor<T> visitor) {
    return visit(iSchema.asStruct(), schema, visitor);
  }

  public static <T> T visit(Type iType, TypeDescription schema, OrcSchemaWithTypeVisitor<T> visitor) {
    switch (schema.getCategory()) {
      case STRUCT:
        return visitor.visitRecord(iType != null ? iType.asStructType() : null, schema, visitor);

      case UNION:
        return visitor.visitUnion(iType, schema, visitor);

      case LIST:
        Types.ListType list = iType != null ? iType.asListType() : null;
        return visitor.list(
            list, schema,
            visit(list != null ? list.elementType() : null, schema.getChildren().get(0), visitor));

      case MAP:
        Types.MapType map = iType != null ? iType.asMapType() : null;
        return visitor.map(
            map, schema,
            visit(map != null ? map.keyType() : null, schema.getChildren().get(0), visitor),
            visit(map != null ? map.valueType() : null, schema.getChildren().get(1), visitor));

      default:
        return visitor.primitive(iType != null ? iType.asPrimitiveType() : null, schema);
    }
  }

  protected T visitRecord(
      Types.StructType struct, TypeDescription record, OrcSchemaWithTypeVisitor<T> visitor) {
    List<TypeDescription> fields = record.getChildren();
    List<String> names = record.getFieldNames();
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
    for (TypeDescription field : fields) {
      int fieldId = ORCSchemaUtil.fieldId(field);
      Types.NestedField iField = struct != null ? struct.field(fieldId) : null;
      results.add(visit(iField != null ? iField.type() : null, field, visitor));
    }
    return visitor.record(struct, record, names, results);
  }

  protected T visitUnion(Type type, TypeDescription union, OrcSchemaWithTypeVisitor<T> visitor) {
    List<TypeDescription> types = union.getChildren();
    List<T> options = Lists.newArrayListWithCapacity(types.size());

    if (types.size() == 1) { // single type union
      options.add(visit(type, types.get(0), visitor));
    } else { // complex union
      visitComplexUnion(type, union, visitor, options);
    }

    return visitor.union(type, union, options);
  }

  /*
  A complex union with multiple types of Orc schema is converted into a struct with multiple fields of Iceberg schema.
  Also an extra tag field is added into the struct of Iceberg schema during the conversion.
  Given an example of complex union in both Orc and Iceberg:
  Orc schema: {"name":"unionCol","type":["int","string"]}
  Iceberg schema:  struct<0: tag: required int, 1: field0: optional int, 2: field1: optional string>
  The fields in the struct of Iceberg schema are expected to be stored in the same order
  as the corresponding types in the union of Orc schema.
  Except the tag field, the fields in the struct of Iceberg schema are the same as the types in the union of Orc schema
  in the general case. In case of field projection, the fields in the struct of Iceberg schema only contains
  a subset of the types in the union of ORC schema, but all the readers for the union branch types must be constructed,
  it's up to the reader code logic to determine to return what value for the given projection schema.
  Therefore, this function visits the complex union with the consideration of either whole projection or partially projected schema.
  Noted that null value and default value for complex union is not a consideration in case of ORC
   */
  private <T> void visitComplexUnion(Type type, TypeDescription union, OrcSchemaWithTypeVisitor<T> visitor,
                                     List<T> options) {

    StructType structType = type.asStructType();
    List<TypeDescription> unionTypes = union.getChildren();
    Map<Integer, Integer> idxInOrcUnionToIdxInType = new HashMap<>();
    // Construct idxInOrcUnionToIdxInType
    for (int i = 0; i < structType.fields().size(); i += 1) {
      String fieldName = structType.fields().get(i).name();
      if (!fieldName.equals(ORCSchemaUtil.ICEBERG_UNION_TAG_FIELD_NAME)) {
        int idxInOrcUnion = Integer.parseInt(fieldName
            .substring(ORCSchemaUtil.ICEBERG_UNION_TYPE_FIELD_NAME_PREFIX_LENGTH));
        idxInOrcUnionToIdxInType.put(idxInOrcUnion, i);
      }
    }

    for (int i = 0; i < union.getChildren().size(); i += 1) {
      if (idxInOrcUnionToIdxInType.containsKey(i)) {
        options.add(visit(structType.fields().get(idxInOrcUnionToIdxInType.get(i)).type(), unionTypes.get(i), visitor));
      } else {
        // even if the type is not projected in the iceberg schema, a reader for the underlying orc type branch still needs to be created,
        // we use a OrcToIcebergVisitorWithPseudoId to re-construct the iceberg type from the orc union branch type and add it to the options,
        // with a pseudo iceberg-id "-1" to avoid failures with the remaining iceberg code infra
        visitNotProjectedTypeInComplexUnion(unionTypes.get(i), visitor, options, i);
      }
    }
  }

  // If a field is not projected, a corresponding field in the struct of Iceberg schema cannot be found
  // for current type of union in Orc schema, a reader for current type still needs to be created and
  // used to make the reading of Orc file successfully. In this case, a pseudo Iceberg type is converted from
  // the Orc schema and is used to create the option for the reader of the current type which still can
  // read the corresponding content in Orc file successfully.
  private static <T> void visitNotProjectedTypeInComplexUnion(TypeDescription orcType,
                                                              OrcSchemaWithTypeVisitor<T> visitor,
                                                              List<T> options,
                                                              int typeIndex) {
    OrcToIcebergVisitor schemaConverter = new OrcToIcebergVisitorWithPseudoId();
    schemaConverter.beforeField("field" + typeIndex, orcType);
    Optional<Types.NestedField> icebergType = OrcToIcebergVisitor.visit(orcType, schemaConverter);
    schemaConverter.afterField("field" + typeIndex, orcType);
    options.add(visit(icebergType.get().type(), orcType, visitor));
  }

  public T record(Types.StructType iStruct, TypeDescription record, List<String> names, List<T> fields) {
    return null;
  }

  public T union(Type iUnion, TypeDescription union, List<T> options) {
    return null;
  }

  public T list(Types.ListType iList, TypeDescription array, T element) {
    return null;
  }

  public T map(Types.MapType iMap, TypeDescription map, T key, T value) {
    return null;
  }

  public T primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
    return null;
  }
}
