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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.AvroSchemaVisitor;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hivelink.core.schema.MergeHiveSchemaWithAvro;
import org.apache.iceberg.hivelink.core.utils.HiveTypeUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class LegacyHiveTableUtils {

  private LegacyHiveTableUtils() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(LegacyHiveTableUtils.class);

  static Schema getSchema(org.apache.hadoop.hive.metastore.api.Table table) {
    Map<String, String> props = getTableProperties(table);
    String schemaStr = props.get("avro.schema.literal");
    // Disable default value validation for backward compatibility with Avro 1.7
    org.apache.avro.Schema avroSchema =
        schemaStr != null ? new org.apache.avro.Schema.Parser().setValidateDefaults(false).parse(schemaStr) : null;
    Schema schema;
    if (avroSchema != null) {
      String serde = table.getSd().getSerdeInfo().getSerializationLib();
      org.apache.avro.Schema finalAvroSchema;
      if (serde.equals("org.apache.hadoop.hive.serde2.avro.AvroSerDe") ||
          HasDuplicateLowercaseColumnNames.visit(avroSchema)) {
        // Case 1: If serde == AVRO, early escape; Hive column info is not reliable and can be empty for these tables
        //         Hive itself uses avro.schema.literal as source of truth for these tables, so this should be fine
        // Case 2: If avro.schema.literal has duplicate column names when lowercased, that means we cannot do reliable
        //         matching with Hive schema as multiple Avro fields can map to the same Hive field
        finalAvroSchema = avroSchema;
      } else {
        finalAvroSchema = MergeHiveSchemaWithAvro.visit(structTypeInfoFromCols(table.getSd().getCols()), avroSchema);
      }
      schema = AvroSchemaUtil.toIceberg(finalAvroSchema);
    } else {
      // TODO: Do we need to support column and column.types properties for ORC tables?
      LOG.info("Table {}.{} does not have an avro.schema.literal set; using Hive schema instead. " +
                   "The schema will not have case sensitivity and nullability information",
               table.getDbName(), table.getTableName());
      Type icebergType = HiveTypeUtil.convert(structTypeInfoFromCols(table.getSd().getCols()));
      schema = new Schema(icebergType.asNestedType().asStructType().fields());
    }
    Types.StructType dataStructType = schema.asStruct();
    List<Types.NestedField> fields = Lists.newArrayList(dataStructType.fields());

    String partitionColumnIdMappingString = props.get("partition.column.ids");
    Schema partitionSchema = partitionSchema(table.getPartitionKeys(), schema, partitionColumnIdMappingString);
    Types.StructType partitionStructType = partitionSchema.asStruct();
    fields.addAll(partitionStructType.fields());
    return new Schema(fields);
  }

  static StructTypeInfo structTypeInfoFromCols(List<FieldSchema> cols) {
    Preconditions.checkArgument(cols != null && cols.size() > 0, "No Hive schema present");
    List<String> fieldNames = cols
        .stream()
        .map(FieldSchema::getName)
        .collect(Collectors.toList());
    List<TypeInfo> fieldTypeInfos = cols
        .stream()
        .map(f -> TypeInfoUtils.getTypeInfoFromTypeString(f.getType()))
        .collect(Collectors.toList());
    return (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
  }

  private static Schema partitionSchema(List<FieldSchema> partitionKeys, Schema dataSchema, String idMapping) {
    Map<String, Integer> nameToId = parsePartitionColId(idMapping);
    AtomicInteger fieldId = new AtomicInteger(10000);
    List<Types.NestedField> partitionFields = Lists.newArrayList();
    partitionKeys.forEach(f -> {
      Types.NestedField field = dataSchema.findField(f.getName());
      if (field != null) {
        throw new IllegalStateException(String.format("Partition field %s also present in data", field.name()));
      }
      partitionFields.add(
          Types.NestedField.optional(
              nameToId.containsKey(f.getName()) ? nameToId.get(f.getName()) : fieldId.incrementAndGet(),
              f.getName(), primitiveIcebergType(f.getType()), f.getComment()));
    });
    return new Schema(partitionFields);
  }

  /**
   *
   * @param idMapping A comma separated string representation of column name
   *                  and its id, e.g. partitionCol1:10,partitionCol2:11, no
   *                  whitespace is allowed in the middle
   * @return          The parsed in-mem Map representation of the name to
   *                  id mapping
   */
  private static Map<String, Integer> parsePartitionColId(String idMapping) {
    Map<String, Integer> nameToId = Maps.newHashMap();
    if (idMapping != null) {
      // parse idMapping string
      Arrays.stream(idMapping.split(",")).forEach(kv -> {
        String[] split = kv.split(":");
        if (split.length != 2) {
          throw new IllegalStateException(String.format(
              "partition.column.ids property is invalid format: %s",
              idMapping));
        }
        String name = split[0];
        Integer id = Integer.parseInt(split[1]);
        nameToId.put(name, id);
      });
    }
    return nameToId;
  }

  private static Type primitiveIcebergType(String hiveTypeString) {
    PrimitiveTypeInfo primitiveTypeInfo = TypeInfoFactory.getPrimitiveTypeInfo(hiveTypeString);
    return HiveTypeUtil.convert(primitiveTypeInfo);
  }

  static Map<String, String> getTableProperties(org.apache.hadoop.hive.metastore.api.Table table) {
    Map<String, String> props = new HashMap<>();
    props.putAll(table.getSd().getParameters());
    props.putAll(table.getParameters());
    props.putAll(table.getSd().getSerdeInfo().getParameters());
    return props;
  }

  static PartitionSpec getPartitionSpec(org.apache.hadoop.hive.metastore.api.Table table, Schema schema) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    table.getPartitionKeys().forEach(fieldSchema -> builder.identity(fieldSchema.getName()));
    return builder.build();
  }

  static DirectoryInfo toDirectoryInfo(org.apache.hadoop.hive.metastore.api.Table table) {
    return new DirectoryInfo(table.getSd().getLocation(),
                             serdeToFileFormat(table.getSd().getSerdeInfo().getSerializationLib()), null);
  }

  static List<DirectoryInfo> toDirectoryInfos(List<Partition> partitions, PartitionSpec spec) {
    return partitions.stream().map(
        p -> new DirectoryInfo(
            p.getSd().getLocation(),
            serdeToFileFormat(
                p.getSd().getSerdeInfo().getSerializationLib()),
            buildPartitionStructLike(p.getValues(), spec))
    ).collect(Collectors.toList());
  }

  private static StructLike buildPartitionStructLike(List<String> partitionValues, PartitionSpec spec) {
    List<Types.NestedField> fields = spec.partitionType().fields();
    return new StructLike() {
      @Override
      public int size() {
        return partitionValues.size();
      }

      @Override
      public <T> T get(int pos, Class<T> javaClass) {
        Type type = fields.get(pos).type();
        String partitionString = partitionValues.get(pos);
        final Object partitionValue;
        // Special handling of TIMESTAMP type since Iceberg Conversions.fromPartitionString
        // does not support it
        if (type.typeId() == Type.TypeID.TIMESTAMP) {
          String isoFormatTs = partitionString.replaceFirst(" ", "T");
          partitionValue =  Literal.of(isoFormatTs).to(Types.TimestampType.withoutZone()).value();
        } else {
          partitionValue = Conversions.fromPartitionString(type, partitionString);
        }
        return javaClass.cast(partitionValue);
      }

      @Override
      public <T> void set(int pos, T value) {
        throw new IllegalStateException("Read-only");
      }
    };
  }

  private static FileFormat serdeToFileFormat(String serde) {
    switch (serde) {
      case "org.apache.hadoop.hive.serde2.avro.AvroSerDe":
        return FileFormat.AVRO;
      case "org.apache.hadoop.hive.ql.io.orc.OrcSerde":
        return FileFormat.ORC;
      default:
        throw new IllegalArgumentException("Unrecognized serde: " + serde);
    }
  }

  private static class HasDuplicateLowercaseColumnNames extends AvroSchemaVisitor<Boolean> {

    private static boolean visit(org.apache.avro.Schema schema) {
      return AvroSchemaVisitor.visit(schema, new HasDuplicateLowercaseColumnNames());
    }

    @Override
    public Boolean record(org.apache.avro.Schema record, List<String> names, List<Boolean> fieldResults) {
      return fieldResults.stream().anyMatch(x -> x) ||
          names.stream().collect(Collectors.groupingBy(String::toLowerCase))
              .values().stream().anyMatch(x -> x.size() > 1);
    }

    @Override
    public Boolean union(org.apache.avro.Schema union, List<Boolean> optionResults) {
      return optionResults.stream().anyMatch(x -> x);
    }

    @Override
    public Boolean array(org.apache.avro.Schema array, Boolean elementResult) {
      return elementResult;
    }

    @Override
    public Boolean map(org.apache.avro.Schema map, Boolean valueResult) {
      return valueResult;
    }

    @Override
    public Boolean primitive(org.apache.avro.Schema primitive) {
      return false;
    }
  }
}
