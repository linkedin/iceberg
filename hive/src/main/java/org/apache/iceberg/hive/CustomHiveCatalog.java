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

package org.apache.iceberg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;


/**
 * A {@link HiveCatalog} which uses {@link CustomHiveTableOperations} underneath.
 *
 * This catalog is especially useful if the Iceberg operations are being performed in response to Hive operations
 */
public class CustomHiveCatalog extends HiveCatalog {

  private final HiveClientPool clients;
  private final Configuration conf;

  public CustomHiveCatalog(Configuration conf) {
    super(conf);
    this.clients = new HiveClientPool(2, conf);
    this.conf = conf;
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new CustomHiveTableOperations(conf, clients, dbName, tableName);
  }
}
