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

/**
 * SPI interface for external catalog providers to inject catalog configurations
 * into Iceberg's parameterized tests.
 * 
 * <p>Implementations can be registered via:
 * <ul>
 *   <li>System property: -Diceberg.test.catalog.provider=com.example.Provider</li>
 *   <li>ServiceLoader: META-INF/services/org.apache.iceberg.spark.TestCatalogProvider</li>
 * </ul>
 */
public interface TestCatalogProvider {
  
  /**
   * Returns catalog configurations to test against.
   * Each array contains: [catalogName, implementation, configMap]
   * 
   * @return array of catalog configurations
   */
  Object[][] getCatalogConfigurations();
  
  /**
   * Called before any tests run. Use for setup (e.g., starting servers).
   * 
   * @throws Exception if setup fails
   */
  default void beforeAll() throws Exception {}
  
  /**
   * Called after all tests complete. Use for cleanup (e.g., stopping servers).
   * 
   * @throws Exception if cleanup fails
   */
  default void afterAll() throws Exception {}
}

