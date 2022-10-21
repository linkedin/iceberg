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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * A {@link HiveCatalog} which uses {@link HiveMetadataPreservingTableOperations} underneath.
 *
 * This catalog should be only be used by metadata publishers wanting to publish/update Iceberg metadata to an existing
 * Hive table while preserving the current Hive metadata
 */
public class HiveMetadataPreservingCatalog extends HiveCatalog {

  private static final String DEFAULT_NAME = "hive_meta_preserving";

  public HiveMetadataPreservingCatalog() {
  }

  private static final Cache<String, HiveMetadataPreservingCatalog> HIVE_METADATA_PRESERVING_CATALOG_CACHE =
          Caffeine.newBuilder().build();

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new HiveMetadataPreservingTableOperations(conf(), clientPool(), new HadoopFileIO(conf()), name(), dbName,
        tableName);
  }

  public static Catalog loadHiveMetadataPreservingCatalog(Configuration conf) {
    return loadHiveMetadataPreservingCatalog(DEFAULT_NAME, conf);
  }

  public static Catalog loadHiveMetadataPreservingCatalog(String name, Configuration conf) {
    return CatalogUtil.loadCatalog(HiveMetadataPreservingCatalog.class.getName(), name,
        ImmutableMap.of(), conf);
  }
}
