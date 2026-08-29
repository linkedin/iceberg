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

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public enum SparkCatalogConfig {
  HIVE(
      "testhive",
      SparkCatalog.class.getName(),
      ImmutableMap.of(
          "type", "hive",
          "default-namespace", "default")),
  HADOOP(
      "testhadoop",
      SparkCatalog.class.getName(),
      ImmutableMap.of("type", "hadoop", "cache-enabled", "false")),
  SPARK(
      "spark_catalog",
      SparkSessionCatalog.class.getName(),
      ImmutableMap.of(
          "type", "hive",
          "default-namespace", "default",
          "parquet-enabled", "true",
          "cache-enabled",
              "false" // Spark will delete tables using v1, leaving the cache out of sync
          )),
  SPARK_WITH_VIEWS(
      "spark_with_views",
      SparkCatalog.class.getName(),
      ImmutableMap.of(
          CatalogProperties.CATALOG_IMPL,
          InMemoryCatalog.class.getName(),
          "default-namespace",
          "default",
          "cache-enabled",
          "false"));

  private final String catalogName;
  private final String implementation;
  private final Map<String, String> properties;

  private static final Object[] DYNAMIC_CONFIG = loadDynamicConfig();

  private static Object[] loadDynamicConfig() {
    String providerClass = System.getProperty("iceberg.test.catalog.provider");
    if (providerClass != null && !providerClass.isEmpty()) {
      try {
        TestCatalogProvider provider =
            (TestCatalogProvider)
                Class.forName(providerClass).getDeclaredConstructor().newInstance();
        Object[][] configs = provider.getCatalogConfigurations();
        if (configs != null && configs.length > 0) {
          for (Object[] config : configs) {
            if ("spark_catalog".equals(config[0])) {
              return config;
            }
          }
          return configs[0];
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to load catalog provider: " + providerClass, e);
      }
    }
    return null;
  }

  SparkCatalogConfig(String catalogName, String implementation, Map<String, String> properties) {
    this.catalogName = catalogName;
    this.implementation = implementation;
    this.properties = properties;
  }

  public String catalogName() {
    return catalogName;
  }

  public String implementation() {
    if (this == SPARK && DYNAMIC_CONFIG != null) {
      return (String) DYNAMIC_CONFIG[1];
    }
    return implementation;
  }

  public Map<String, String> properties() {
    if (this == SPARK && DYNAMIC_CONFIG != null) {
      return (Map<String, String>) DYNAMIC_CONFIG[2];
    }
    return properties;
  }
}
