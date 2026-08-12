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
package org.apache.iceberg.spark;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.BaseDataReader;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;

/**
 * Spark ORC schema visitor that injects {@code initial-default} values into {@code idToConstant}
 * for fields omitted from the per-file ORC projection.
 *
 * <p>{@link org.apache.iceberg.spark.data.SparkOrcReader} uses this inject. The batched ORC reader
 * does not opt into omission, so defaulted Spark scans stay on the row reader.
 */
public abstract class OrcSchemaWithTypeVisitorSpark<T> extends OrcSchemaWithTypeVisitor<T> {

  private final Map<Integer, Object> idToConstant;

  public Map<Integer, Object> getIdToConstant() {
    return idToConstant;
  }

  protected OrcSchemaWithTypeVisitorSpark(Map<Integer, ?> idToConstant) {
    this.idToConstant = Maps.newHashMap();
    this.idToConstant.putAll(idToConstant);
  }

  @Override
  protected T visitRecord(
      Types.StructType struct, TypeDescription record, OrcSchemaWithTypeVisitor<T> visitor) {
    Preconditions.checkState(
        icebergFieldIdsContainOrcFieldIdsInOrder(struct, record),
        "Iceberg schema and ORC schema doesn't align, please call ORCSchemaUtil.buildOrcProjection"
            + " to get an aligned ORC schema first!");
    List<Types.NestedField> iFields = struct.fields();
    List<TypeDescription> fields = record.getChildren();
    List<String> names = record.getFieldNames();
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());

    for (int i = 0, j = 0; i < iFields.size(); i++) {
      Types.NestedField iField = iFields.get(i);
      TypeDescription field = j < fields.size() ? fields.get(j) : null;
      if (field == null || (iField.fieldId() != ORCSchemaUtil.fieldId(field))) {
        // Cases that use idToConstant for an iField:
        // 1. MetadataColumns.ROW_POSITION → RowPositionReader
        // 2. Partition column → ConstantReader (already in idToConstant from PartitionUtil)
        // 3. Field omitted because it declares initial-default → ConstantReader (inject here)
        if (MetadataColumns.nonMetadataColumn(iField.name())
            && !idToConstant.containsKey(iField.fieldId())
            && iField.initialDefault() != null) {
          idToConstant.put(
              iField.fieldId(),
              BaseDataReader.convertConstant(iField.type(), iField.initialDefault()));
        }
      } else {
        results.add(visit(iField.type(), field, visitor));
        j++;
      }
    }
    return visitor.record(struct, record, names, results);
  }

  private static boolean icebergFieldIdsContainOrcFieldIdsInOrder(
      Types.StructType struct, TypeDescription record) {
    List<Integer> icebergIDList =
        struct.fields().stream().map(Types.NestedField::fieldId).collect(Collectors.toList());
    List<Integer> orcIDList =
        record.getChildren().stream().map(ORCSchemaUtil::fieldId).collect(Collectors.toList());

    return containsInOrder(icebergIDList, orcIDList);
  }

  /**
   * Checks whether {@code list1} contains all integers from {@code list2} in the same relative
   * order. {@code list1} may contain extra integers that {@code list2} does not.
   */
  private static boolean containsInOrder(List<Integer> list1, List<Integer> list2) {
    if (list1.size() < list2.size()) {
      return false;
    }

    for (int i = 0, j = 0; j < list2.size(); j++) {
      if (i >= list1.size()) {
        return false;
      }
      while (!list1.get(i).equals(list2.get(j))) {
        i++;
        if (i >= list1.size()) {
          return false;
        }
      }
      i++;
    }
    return true;
  }
}
