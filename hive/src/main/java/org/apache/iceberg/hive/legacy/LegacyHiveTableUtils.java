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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class LegacyHiveTableUtils {

  private LegacyHiveTableUtils() {}

  private static final Logger LOG = LoggerFactory.getLogger(LegacyHiveTableUtils.class);

  static Schema getSchema(org.apache.hadoop.hive.metastore.api.Table table) {
    Map<String, String> props = getTableProperties(table);
    String schemaStr = props.get("avro.schema.literal");
    Schema schema;
    if (schemaStr == null) {
      LOG.warn("Table {}.{} does not have an avro.schema.literal set; using Hive schema instead. The schema will not" +
          " have case sensitivity and nullability information", table.getDbName(), table.getTableName());
      // TODO: Add support for tables without avro.schema.literal
      throw new UnsupportedOperationException("Reading tables without avro.schema.literal not implemented yet");
    } else {
      schema = AvroSchemaUtil.toIceberg(new org.apache.avro.Schema.Parser().parse(schemaStr));
    }

    List<String> partCols = table.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
    return addPartitionColumnsIfRequired(schema, partCols);
  }

  private static Schema addPartitionColumnsIfRequired(Schema schema, List<String> partitionColumns) {
    List<Types.NestedField> fields = new ArrayList<>(schema.columns());
    AtomicInteger fieldId = new AtomicInteger(10000);
    partitionColumns.stream().forEachOrdered(column -> {
      Types.NestedField field = schema.findField(column);
      if (field == null) {
        // TODO: Support partition fields with non-string types
        fields.add(Types.NestedField.required(fieldId.incrementAndGet(), column, Types.StringType.get()));
      } else {
        Preconditions.checkArgument(field.type().equals(Types.StringType.get()),
            "Tables with non-string partition columns not supported yet");
      }
    });
    return new Schema(fields);
  }

  static Map<String, String> getTableProperties(org.apache.hadoop.hive.metastore.api.Table table) {
    Map<String, String> props = new HashMap<>();
    props.putAll(table.getSd().getSerdeInfo().getParameters());
    props.putAll(table.getSd().getParameters());
    props.putAll(table.getParameters());
    return props;
  }

  static PartitionSpec getPartitionSpec(org.apache.hadoop.hive.metastore.api.Table table, Schema schema) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

    table.getPartitionKeys().forEach(fieldSchema -> {
      // TODO: Support partition fields with non-string types
      Preconditions.checkArgument(fieldSchema.getType().equals("string"),
          "Tables with non-string partition columns not supported yet");
      builder.identity(fieldSchema.getName());
    });

    return builder.build();
  }

  static DirectoryInfo toDirectoryInfo(org.apache.hadoop.hive.metastore.api.Table table) {
    return new DirectoryInfo(table.getSd().getLocation(),
        serdeToFileFormat(table.getSd().getSerdeInfo().getSerializationLib()), null);
  }

  static List<DirectoryInfo> toDirectoryInfos(List<Partition> partitions) {
    return partitions.stream().map(p -> {
      return new DirectoryInfo(p.getSd().getLocation(),
          serdeToFileFormat(p.getSd().getSerdeInfo().getSerializationLib()), buildPartitionStructLike(p.getValues()));
    }).collect(Collectors.toList());
  }

  private static StructLike buildPartitionStructLike(List<String> partitionValues) {
    return new StructLike() {

      @Override
      public int size() {
        return partitionValues.size();
      }

      @Override
      public <T> T get(int pos, Class<T> javaClass) {
        return javaClass.cast(partitionValues.get(pos));
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
}
