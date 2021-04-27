package org.apache.iceberg.spark.data;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class TestSparkAvroUnions {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void writeAndValidateNonOptionUnion() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
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

    GenericData.Record unionRecord1 = new GenericData.Record(avroSchema);
    unionRecord1.put("unionCol", "StringType1");
    GenericData.Record unionRecord2 = new GenericData.Record(avroSchema);
    unionRecord2.put("unionCol", 1);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(unionRecord1);
      writer.append(unionRecord2);
    }

    Schema expectedSchema = new Schema(
        Types.NestedField.required(0, "unionCol", Types.StructType.of(
            Types.NestedField.optional(1, "tag_1", Types.IntegerType.get()),
            Types.NestedField.optional(2, "tag_2", Types.StringType.get())))
    );

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
        .createReaderFunc(SparkAvroReader::new)
        .project(expectedSchema)
        .build()) {
      rows = Lists.newArrayList(reader);
    }
  }
}
