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

package org.apache.iceberg.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;


public class TestAvroNonOptionalUnion {

  @Test
  public void testNonOptionUnionNonNullable() {
    Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .intType()
        .and()
        .stringType()
        .endUnion()
        .noDefault()
        .endRecord();

    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
    String expectedIcebergSchema = "table {\n" +
        "  0: unionCol: required struct<1: tag_1: optional int, 2: tag_2: optional string>\n" + "}";

    Assert.assertEquals(expectedIcebergSchema, icebergSchema.toString());
  }

  @Test
  public void testNonOptionUnionNullable() {
    Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .nullType()
        .and()
        .intType()
        .and()
        .stringType()
        .endUnion()
        .noDefault()
        .endRecord();

    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
    String expectedIcebergSchema =
        "table {\n" + "  0: unionCol: optional struct<1: tag_1: optional int, 2: tag_2: optional string>\n" + "}";

    Assert.assertEquals(expectedIcebergSchema, icebergSchema.toString());
  }

  @Test
  public void testSingleOptionUnion() {
    Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("unionCol")
        .type()
        .unionOf()
        .intType()
        .endUnion()
        .noDefault()
        .endRecord();

    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
    String expectedIcebergSchema = "table {\n" + "  0: unionCol: required struct<1: tag_1: optional int>\n" + "}";

    Assert.assertEquals(expectedIcebergSchema, icebergSchema.toString());
  }
}
