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
import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.codehaus.jackson.node.JsonNodeFactory;


class HiveTypeToAvroType extends HiveTypeUtil.HiveSchemaVisitor<Schema> {

  static Schema visit(TypeInfo typeInfo) {
    return HiveTypeUtil.visit(typeInfo, new HiveTypeToAvroType());
  }

  // Additional numeric type, similar to other logical type names in AvroSerde
  private static final String SHORT_TYPE_NAME = "short";
  private static final String BYTE_TYPE_NAME = "byte";

  private final AtomicInteger recordCounter = new AtomicInteger(0);

  @Override
  public Schema struct(StructTypeInfo struct, List<String> names, List<Schema> fieldResults) {
    List<Schema.Field> fields = Lists.newArrayListWithExpectedSize(fieldResults.size());
    for (int i = 0; i < fields.size(); i++) {
      fields.add(new Schema.Field(
          AvroSchemaUtil.makeCompatibleName(names.get(i)),
          AvroSchemaUtil.toOption(fieldResults.get(i)), null, Schema.Field.NULL_DEFAULT_VALUE));
    }
    int recordNum = recordCounter.incrementAndGet();
    return Schema.createRecord("record" + recordNum, null, "namespace" + recordNum, false, fields);
  }

  @Override
  public Schema list(ListTypeInfo list, Schema elementResult) {
    return Schema.createArray(AvroSchemaUtil.toOption(elementResult));
  }

  @Override
  public Schema map(MapTypeInfo map, Schema keyResult, Schema valueResult) {
    // TODO: Assert that key is string
    return Schema.createMap(AvroSchemaUtil.toOption(valueResult));
  }

  @Override
  public Schema primitive(PrimitiveTypeInfo primitive) {
    Schema schema;
    switch (primitive.getPrimitiveCategory()) {
      case INT:
        return Schema.create(Schema.Type.INT);

      case BYTE:
        schema = Schema.create(Schema.Type.INT);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, BYTE_TYPE_NAME);
        return schema;

      case SHORT:
        schema = Schema.create(Schema.Type.INT);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, SHORT_TYPE_NAME);
        return schema;

      case LONG:
        return Schema.create(Schema.Type.LONG);

      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);

      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);

      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);

      case CHAR:
      case STRING:
      case VARCHAR:
        return Schema.create(Schema.Type.STRING);

      case BINARY:
        return Schema.create(Schema.Type.BYTES);

      case VOID:
        return Schema.create(Schema.Type.NULL);

      case DATE:
        schema = Schema.create(Schema.Type.INT);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, AvroSerDe.DATE_TYPE_NAME);
        return schema;

      case TIMESTAMP:
        schema = Schema.create(Schema.Type.LONG);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, AvroSerDe.TIMESTAMP_TYPE_NAME);
        return schema;

      case DECIMAL:
        DecimalTypeInfo dti = (DecimalTypeInfo) primitive;
        JsonNodeFactory factory = JsonNodeFactory.instance;
        schema = Schema.create(Schema.Type.BYTES);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, AvroSerDe.DECIMAL_TYPE_NAME);
        schema.addProp(AvroSerDe.AVRO_PROP_PRECISION, factory.numberNode(dti.getPrecision()));
        schema.addProp(AvroSerDe.AVRO_PROP_SCALE, factory.numberNode(dti.getScale()));
        return schema;

      default:
        throw new UnsupportedOperationException(primitive + " is not supported.");
    }
  }
}
