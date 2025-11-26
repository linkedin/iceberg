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
package org.apache.iceberg;

import java.io.File;

/**
 * SPI interface for external table providers to inject custom table creation
 * into Iceberg's core tests.
 * 
 * <p>Implementations can be registered via:
 * <ul>
 *   <li>System property: -Diceberg.test.table.provider=com.example.Provider</li>
 *   <li>ServiceLoader: META-INF/services/org.apache.iceberg.TestTableProvider</li>
 * </ul>
 */
public interface TestTableProvider {
  
  /**
   * Creates a test table with custom operations.
   * 
   * @param dir the directory for the table
   * @param name the table name
   * @param schema the table schema
   * @param spec the partition spec
   * @param formatVersion the format version
   * @return a test table instance
   */
  TestTables.TestTable createTable(
    File dir, String name, Schema schema, PartitionSpec spec, int formatVersion);
  
  /**
   * Called before any tests run.
   * 
   * @throws Exception if setup fails
   */
  default void beforeAll() throws Exception {}
  
  /**
   * Called after all tests complete.
   * 
   * @throws Exception if cleanup fails
   */
  default void afterAll() throws Exception {}
}
