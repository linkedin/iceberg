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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link HiveCatalog} which uses falls back to using Hive metadata to read tables when Iceberg metadata is not
 * available. If the table is read through Hive metadata, features like time travel, snapshot isolation and incremental
 * computation are not supported along with any WRITE operations to either the data or metadata.
 */
public class HiveCatalogWithLegacyReadFallback extends HiveCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogWithLegacyReadFallback.class);

  public HiveCatalogWithLegacyReadFallback(Configuration conf) {
    super(conf);
  }

  @Override
  @SuppressWarnings("CatchBlockLogException")
  public Table loadTable(TableIdentifier identifier) {
    // Try to load the table using Iceberg metadata first. If it fails, use Hive metadata
    try {
      return super.loadTable(identifier);
    } catch (NoSuchTableException e) {
      TableOperations ops = legacyTableOps(identifier);
      if (ops.current() == null) {
        if (isValidMetadataIdentifier(identifier)) {
          throw new UnsupportedOperationException(
              "Metadata tables not supported for Hive tables without Iceberg metadata. Table: " + identifier);
        }
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      } else {
        LOG.info(
            "Iceberg metadata does not exist for {}; Falling back to Hive metadata. Time travel, snapshot isolation," +
                " incremental computation features will not be available", identifier);
        return new LegacyHiveTable(ops, fullTableName(name(), identifier));
      }
    }
  }

  private TableOperations legacyTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new LegacyHiveTableOperations(conf(), clientPool(), dbName, tableName);
  }
}
