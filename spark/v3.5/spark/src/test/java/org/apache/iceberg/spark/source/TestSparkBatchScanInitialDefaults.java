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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkBatchScanInitialDefaults {

  @Test
  public void testDisablesVectorizationWhenTopLevelDefaultIsProjected() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional("country")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("US"))
                .build());

    Assert.assertFalse(SparkBatch.hasNoInitialDefaults(schema));
  }

  @Test
  public void testDisablesVectorizationWhenNestedDefaultIsProjected() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "location",
                Types.StructType.of(
                    Types.NestedField.optional("country")
                        .withId(2)
                        .ofType(Types.StringType.get())
                        .withInitialDefault(Expressions.lit("US"))
                        .build())));

    Assert.assertFalse(SparkBatch.hasNoInitialDefaults(schema));
  }

  @Test
  public void testAllowsVectorizationWhenNoDefaultIsProjected() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "country", Types.StringType.get()));

    Assert.assertTrue(SparkBatch.hasNoInitialDefaults(schema));
  }
}
