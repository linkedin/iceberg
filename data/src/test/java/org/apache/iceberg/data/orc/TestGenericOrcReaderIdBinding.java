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
package org.apache.iceberg.data.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests specifically targeting the id-based field binding path of {@link GenericOrcReaders}. These
 * exercise branches of the id-binding {@code StructReader} constructor that the shared projection
 * tests do not, and assert that id-binding produces the same results positional binding would.
 */
public class TestGenericOrcReaderIdBinding {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private List<Record> writeAndRead(
      String desc, Schema writeSchema, Schema readSchema, List<Record> records) throws IOException {
    return writeAndRead(desc, writeSchema, readSchema, records, ImmutableMap.of());
  }

  private List<Record> writeAndRead(
      String desc,
      Schema writeSchema,
      Schema readSchema,
      List<Record> records,
      Map<Integer, ?> idToConstant)
      throws IOException {
    File file = temp.newFile(desc + ".orc");
    Assert.assertTrue("Delete should succeed", file.delete());

    try (FileAppender<Record> appender =
        ORC.write(Files.localOutput(file))
            .schema(writeSchema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      appender.addAll(records);
    }

    // Project only fields that physically exist in the file (excluding metadata columns) so that
    // buildOrcProjection succeeds, but hand the reader the full read schema so the id-binding
    // constructor resolves metadata/constant fields.
    Schema projection = TypeUtil.selectNot(readSchema, MetadataColumns.metadataFieldIds());
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(file))
            .project(projection)
            .createReaderFunc(
                fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema, idToConstant))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  @Test
  public void testTypePromotion() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "f", Types.FloatType.get()),
            required(3, "dec", Types.DecimalType.of(9, 2)));

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("id", 42);
    record.setField("f", 1.5f);
    record.setField("dec", new BigDecimal("123.45"));

    Schema promotedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "f", Types.DoubleType.get()),
            required(3, "dec", Types.DecimalType.of(11, 2)));

    Record projected =
        writeAndRead("type_promotion", writeSchema, promotedSchema, Lists.newArrayList(record))
            .get(0);

    Assert.assertEquals("int should be promoted to long", 42L, projected.getField("id"));
    Assert.assertEquals(
        "float should be promoted to double", 1.5d, (double) projected.getField("f"), 0.0d);
    Assert.assertEquals(
        "decimal precision should widen", new BigDecimal("123.45"), projected.getField("dec"));
  }

  @Test
  public void testReorderedProjectionValues() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "a", Types.LongType.get()),
            optional(2, "b", Types.StringType.get()),
            required(3, "c", Types.IntegerType.get()));

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("a", 10L);
    record.setField("b", "hello");
    record.setField("c", 7);

    // Read schema requests the fields in a different order than the file's physical column order.
    Schema reordered =
        new Schema(
            required(3, "c", Types.IntegerType.get()),
            required(1, "a", Types.LongType.get()),
            optional(2, "b", Types.StringType.get()));

    Record projected =
        writeAndRead("reordered", writeSchema, reordered, Lists.newArrayList(record)).get(0);

    Assert.assertEquals("c should bind by id", 7, projected.getField("c"));
    Assert.assertEquals("a should bind by id", 10L, projected.getField("a"));
    Assert.assertEquals("b should bind by id", "hello", projected.getField("b").toString());
  }

  @Test
  public void testMetadataColumns() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

    List<Record> records = Lists.newArrayList();
    for (long i = 0; i < 5; i++) {
      Record record = GenericRecord.create(writeSchema.asStruct());
      record.setField("id", i);
      record.setField("data", "row" + i);
      records.add(record);
    }

    Schema readSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            MetadataColumns.ROW_POSITION,
            MetadataColumns.IS_DELETED);

    List<Record> projected = writeAndRead("metadata_columns", writeSchema, readSchema, records);

    Assert.assertEquals(5, projected.size());
    for (int i = 0; i < projected.size(); i++) {
      Record record = projected.get(i);
      Assert.assertEquals("id should read from file", (long) i, record.getField("id"));
      Assert.assertEquals(
          "data should read from file", "row" + i, record.getField("data").toString());
      Assert.assertEquals(
          "_pos should be the row position",
          (long) i,
          record.getField(MetadataColumns.ROW_POSITION.name()));
      Assert.assertEquals(
          "_deleted should be false", false, record.getField(MetadataColumns.IS_DELETED.name()));
    }
  }

  @Test
  public void testIdToConstant() throws IOException {
    Schema writeSchema =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("id", 1L);
    record.setField("data", "value");

    // Read schema adds an identity-partition-style constant field (id 3) not present in the file.
    Schema readSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(3, "part", Types.StringType.get()));

    Record projected =
        writeAndRead(
                "id_to_constant",
                writeSchema,
                readSchema,
                Lists.newArrayList(record),
                ImmutableMap.of(3, "constant-partition"))
            .get(0);

    Assert.assertEquals("id should read from file", 1L, projected.getField("id"));
    Assert.assertEquals(
        "data should read from file", "value", projected.getField("data").toString());
    Assert.assertEquals(
        "part should resolve from idToConstant", "constant-partition", projected.getField("part"));
  }
}
