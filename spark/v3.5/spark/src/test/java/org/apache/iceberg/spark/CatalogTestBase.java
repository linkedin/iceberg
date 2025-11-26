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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;

public abstract class CatalogTestBase extends TestBaseWithCatalog {

  @TempDir protected Path temp;

  // these parameters are broken out to avoid changes that need to modify lots of test suites
  protected static Object[][] baseCatalogParameters() {
    List<Object[]> params = new ArrayList<>();

    // Check if we should skip default catalogs
    boolean skipDefaults = Boolean.getBoolean("iceberg.test.catalog.skip.defaults");

    // Load external catalog provider if specified
    String providerClassName = System.getProperty("iceberg.test.catalog.provider");
    if (providerClassName != null) {
      try {
        TestCatalogProvider provider =
            (TestCatalogProvider)
                Class.forName(providerClassName).getDeclaredConstructor().newInstance();
        // Must call beforeAll() to initialize the provider (e.g., start servers)
        provider.beforeAll();
        params.addAll(Arrays.asList(provider.getCatalogConfigurations()));
      } catch (Exception e) {
        throw new RuntimeException("Failed to load catalog provider: " + providerClassName, e);
      }
    }

    // Add default catalogs unless skipped
    if (!skipDefaults) {
      params.add(
          new Object[] {
            SparkCatalogConfig.HIVE.catalogName(),
            SparkCatalogConfig.HIVE.implementation(),
            SparkCatalogConfig.HIVE.properties()
          });
      params.add(
          new Object[] {
            SparkCatalogConfig.HADOOP.catalogName(),
            SparkCatalogConfig.HADOOP.implementation(),
            SparkCatalogConfig.HADOOP.properties()
          });
      params.add(
          new Object[] {
            SparkCatalogConfig.SPARK.catalogName(),
            SparkCatalogConfig.SPARK.implementation(),
            SparkCatalogConfig.SPARK.properties()
          });
    }

    return params.toArray(new Object[0][]);
  }
}
