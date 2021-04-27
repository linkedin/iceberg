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
    String expectedIcebergSchema = "table {\n"
        + "  0: unionCol: required struct<1: tag_1: optional int, 2: tag_2: optional string>\n" + "}";

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
    String expectedIcebergSchema = "table {\n" + "  0: unionCol: optional struct<1: tag_1: optional int, 2: tag_2: optional string>\n" + "}";

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
