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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.orc.TypeDescription;

public class OrcToIcebergVisitorWithPseudoId extends OrcToIcebergVisitor {

  private static final String PSEUDO_ICEBERG_FIELD_ID = "-1";

  @Override
  public Optional<NestedField> union(TypeDescription union, List<Optional<NestedField>> options) {
    union.setAttribute(org.apache.iceberg.orc.ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, PSEUDO_ICEBERG_FIELD_ID);
    List<Optional<NestedField>> optionsCopy = new ArrayList<>(options);
    optionsCopy.add(0, Optional.of(
        Types.NestedField.of(Integer.parseInt(PSEUDO_ICEBERG_FIELD_ID), true,
            ORCSchemaUtil.ICEBERG_UNION_TAG_FIELD_NAME, Types.IntegerType.get())));
    return Optional.of(
        Types.NestedField.of(Integer.parseInt(PSEUDO_ICEBERG_FIELD_ID), true, currentFieldName(), Types.StructType.of(
            optionsCopy.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList()))));
  }

  @Override
  public Optional<NestedField> record(TypeDescription record, List<String> names,
      List<Optional<NestedField>> fields) {
    record.setAttribute(org.apache.iceberg.orc.ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, PSEUDO_ICEBERG_FIELD_ID);
    return super.record(record, names, fields);
  }

  @Override
  public Optional<NestedField> list(TypeDescription array, Optional<NestedField> element) {
    array.setAttribute(org.apache.iceberg.orc.ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, PSEUDO_ICEBERG_FIELD_ID);
    return super.list(array, element);
  }

  @Override
  public Optional<NestedField> map(TypeDescription map, Optional<NestedField> key,
      Optional<NestedField> value) {
    map.setAttribute(org.apache.iceberg.orc.ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, PSEUDO_ICEBERG_FIELD_ID);
    return super.map(map, key, value);
  }

  @Override
  public Optional<NestedField> primitive(TypeDescription primitive) {
    primitive.setAttribute(org.apache.iceberg.orc.ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, PSEUDO_ICEBERG_FIELD_ID);
    return super.primitive(primitive);
  }
}
