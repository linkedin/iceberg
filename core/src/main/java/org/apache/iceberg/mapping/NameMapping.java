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

package org.apache.iceberg.mapping;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Represents a mapping from external schema names to Iceberg type IDs.
 */
public class NameMapping implements Serializable {
  private static final Joiner DOT = Joiner.on('.');

  public static NameMapping of(MappedField... fields) {
    return new NameMapping(MappedFields.of(ImmutableList.copyOf(fields)));
  }

  public static NameMapping of(List<MappedField> fields) {
    return new NameMapping(MappedFields.of(fields));
  }

  public static NameMapping of(MappedFields fields) {
    return new NameMapping(fields);
  }

  private final MappedFields mapping;
  private final boolean caseSensitive;
  private transient Map<Integer, MappedField> fieldsById;
  private transient Map<String, MappedField> fieldsByName;
  private transient Map<String, MappedField> fieldsByNameLowercase;

  NameMapping(MappedFields mapping) {
    this(mapping, true);
  }

  public NameMapping(MappedFields mapping, boolean caseSensitive) {
    this.mapping = mapping;
    this.caseSensitive = caseSensitive;
    lazyFieldsById();
    lazyFieldsByName();
    if (!caseSensitive) {
      lazyFieldsByNameLowercase();
    }
  }

  public MappedField find(int id) {
    return lazyFieldsById().get(id);
  }

  public MappedField find(String... names) {
    return find(DOT.join(names));
  }

  public MappedField find(List<String> names) {
    return find(DOT.join(names));
  }

  private MappedField find(String qualifiedName) {
    MappedField field = lazyFieldsByName().get(qualifiedName);
    if (field == null && !caseSensitive) {
      field = lazyFieldsByNameLowercase().get(qualifiedName.toLowerCase());
    }
    return field;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public MappedFields asMappedFields() {
    return mapping;
  }

  private Map<Integer, MappedField> lazyFieldsById() {
    if (fieldsById == null) {
      this.fieldsById = MappingUtil.indexById(mapping);
    }
    return fieldsById;
  }

  private Map<String, MappedField> lazyFieldsByName() {
    if (fieldsByName == null) {
      this.fieldsByName = MappingUtil.indexByName(mapping);
    }
    return fieldsByName;
  }

  private Map<String, MappedField> lazyFieldsByNameLowercase() {
    if (fieldsByNameLowercase == null) {
      this.fieldsByNameLowercase = lazyFieldsByName().entrySet().stream()
          .collect(Collectors.toMap(x -> x.getKey().toLowerCase(), Map.Entry::getValue, (u, v) -> u));
    }
    return fieldsByNameLowercase;
  }

  @Override
  public String toString() {
    if (mapping.fields().isEmpty()) {
      return "[]";
    } else {
      return "[\n  " + Joiner.on("\n  ").join(mapping.fields()) + "\n]";
    }
  }
}
