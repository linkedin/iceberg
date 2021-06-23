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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;

public abstract class OrcSchemaWithTypeVisitorSpark<T> extends OrcSchemaWithTypeVisitor<T> {

  private final Map<Integer, Object> idToConstant;

  public Map<Integer, Object> getIdToConstant() {
    return idToConstant;
  }

  protected OrcSchemaWithTypeVisitorSpark(Map<Integer, ?> idToConstant) {
    this.idToConstant = new HashMap<>();
    this.idToConstant.putAll(idToConstant);
  }

  @Override
  protected T visitRecord(
          Types.StructType struct, TypeDescription record, OrcSchemaWithTypeVisitor<T> visitor) {
    Preconditions.checkState(
            checkIcebergAndOrcSchemaAlignment(struct, record),
            "Iceberg schema and ORC schema doesn't align, please call ORCSchemaUtil.buildOrcProjection" +
                    "to get an aligned ORC schema first!"
    );
    List<Types.NestedField> iFields = struct.fields();
    List<TypeDescription> fields = record.getChildren();
    List<String> names = record.getFieldNames();
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());

    for (int i = 0, j = 0; i < iFields.size(); i++) {
      Types.NestedField iField = iFields.get(i);
      TypeDescription field = j < fields.size() ? fields.get(j) : null;
      if (field == null || (iField.fieldId() != ORCSchemaUtil.fieldId(field))) {
        if (!iField.equals(MetadataColumns.ROW_POSITION)) {
          idToConstant.put(iField.fieldId(), iField.getDefaultValue());
        }
      } else {
        results.add(visit(iField.type(), field, visitor));
        j++;
      }
    }
    return visitor.record(struct, record, names, results);
  }

  private static boolean checkIcebergAndOrcSchemaAlignment(Types.StructType struct, TypeDescription record) {
    List<Integer> icebergIDList = struct.fields().stream().map(Types.NestedField::fieldId).collect(Collectors.toList());
    List<Integer> orcIDList = record.getChildren().stream().map(ORCSchemaUtil::fieldId).collect(Collectors.toList());

    // icebergIDList should be a superset of orcIDList, and the overlapping ids should appear
    // in the same order in these 2 lists
    return checkTwoListAlignmentHelper(icebergIDList, orcIDList);
  }

  private static boolean checkTwoListAlignmentHelper(List<Integer> list1, List<Integer> list2) {
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
