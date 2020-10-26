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

package org.apache.iceberg.mr.hive;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hive.legacy.HiveTypeUtil;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.mapred.Container;

public class HiveIcebergSerDe extends AbstractSerDe {

  private ObjectInspector inspector;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties serDeProperties) throws SerDeException {
    Schema readSchema;
    if (serDeProperties.getProperty(serdeConstants.LIST_COLUMNS) != null) {
      String tableColumns = serDeProperties.getProperty(serdeConstants.LIST_COLUMNS);
      String tableColumnTypes = serDeProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
      if (configuration.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS) != null &&
          configuration.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES) != null) {
        tableColumns = configuration.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS);
        tableColumnTypes = configuration.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES);
      }
      readSchema = getSchemaFromTypeString(tableColumns, tableColumnTypes);
    } else {
      Table table = Catalogs.loadTable(configuration, serDeProperties);
      readSchema = table.schema();
    }
    try {
      this.inspector = IcebergObjectInspector.create(readSchema);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Container.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) {
    throw new UnsupportedOperationException("Serialization is not supported.");
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) {
    return ((Container<?>) writable).get();
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  public static Schema getSchemaFromTypeString(String tableColumns, String hiveTypeProperty) {
    List<TypeInfo> typeInfoList = TypeInfoUtils.getTypeInfosFromTypeString(hiveTypeProperty);
    List<String> colNames = Arrays.asList(tableColumns.split(","));
    TypeInfo typeInfo = TypeInfoFactory.getStructTypeInfo(colNames, typeInfoList);
    return new Schema(HiveTypeUtil.convert(typeInfo).asNestedType().asStructType().fields());
  }

}
