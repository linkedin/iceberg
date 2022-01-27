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

package org.apache.iceberg.hiveberg.spark2;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;


public class IcebergSource extends org.apache.iceberg.spark.source.IcebergSource {

  @Override
  public DataSourceReader createReader(StructType readSchema, DataSourceOptions options) {
    Configuration conf = new Configuration(lazyBaseConf());
    Table table = getTableAndResolveHadoopConfiguration(options, conf);
    String caseSensitive = lazySparkSession().conf().get("spark.sql.caseSensitive");

    Broadcast<FileIO> io = lazySparkContext().broadcast(SparkUtil.serializableFileIO(table));
    Broadcast<EncryptionManager> encryptionManager = lazySparkContext().broadcast(table.encryption());

    Reader reader = new Reader(table, io, encryptionManager, Boolean.parseBoolean(caseSensitive), options);
    if (readSchema != null) {
      // convert() will fail if readSchema contains fields not in table.schema()
      SparkSchemaUtil.convert(table.schema(), readSchema);
      reader.pruneColumns(readSchema);
    }

    return reader;
  }
}
