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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.sql.SparkSession;

/**
 * Service Provider Interface that allows downstream integrations (e.g. catalog servers)
 * to supply custom {@link SparkSession}s for Iceberg's Spark-based test suites.
 *
 * <p>Implementations can be wired using either the {@code iceberg.test.spark.session.provider}
 * system property or via {@link java.util.ServiceLoader} registration at
 * {@code META-INF/services/org.apache.iceberg.spark.TestSparkSessionProvider}.
 *
 * <p>The lifecycle methods mirror JUnit constructs so providers can eagerly start/stop the
 * infrastructure they need (embedded services, custom Spark extensions, etc.).
 */
public interface TestSparkSessionProvider {

  /**
    * Gives implementations a chance to perform any global setup before a Spark session is created.
    */
  default void beforeAll() throws Exception {}

  /**
   * Implementations must return the {@link SparkSession} Iceberg tests should use.
   *
   * <p>The method receives the default builder Iceberg would normally use along with the HiveConf
   * backing the embedded metastore. Implementations can either mutate & invoke the builder or
   * ignore it entirely and construct their own session.
   *
   * @param defaultBuilder pre-configured builder that points at Iceberg's embedded HMS
   * @param hiveConf Hive configuration backing the test metastore
   * @return a SparkSession ready for running Iceberg's tests
   */
  SparkSession createSparkSession(SparkSession.Builder defaultBuilder, HiveConf hiveConf)
      throws Exception;

  /**
   * Gives implementations a chance to clean up resources after the test Spark session stops.
   */
  default void afterAll() throws Exception {}
}


