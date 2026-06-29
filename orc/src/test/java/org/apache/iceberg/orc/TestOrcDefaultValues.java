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
package org.apache.iceberg.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Verifies that a top-level scalar {@code initial-default} is filled on ORC read. */
public class TestOrcDefaultValues {

  private static final Schema WRITE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  // Evolved: adds a top-level scalar with an initial-default that is absent from the written file.
  private static final Schema READ_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional("country")
              .withId(3)
              .ofType(Types.StringType.get())
              .withInitialDefault(Expressions.lit("US"))
              .build());

  private List<Record> records;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void createRecords() {
    GenericRecord record = GenericRecord.create(WRITE_SCHEMA);
    records =
        Lists.newArrayList(
            record.copy(ImmutableMap.of("id", 1L, "data", "a")),
            record.copy(ImmutableMap.of("id", 2L, "data", "b")),
            record.copy(ImmutableMap.of("id", 3L, "data", "c")));
  }

  private OutputFile writeFile() throws IOException {
    OutputFile file = Files.localOutput(temp.newFile());
    DataWriter<Record> writer =
        ORC.writeData(file)
            .schema(WRITE_SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try {
      for (Record record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
    return file;
  }

  @Test
  public void testReadFillsTopLevelScalarDefault() throws IOException {
    OutputFile file = writeFile();

    List<Record> read;
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(READ_SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(READ_SCHEMA, fileSchema))
            .build()) {
      read = Lists.newArrayList(reader);
    }

    Assertions.assertThat(read).hasSize(records.size());
    for (int i = 0; i < read.size(); i += 1) {
      Assertions.assertThat(read.get(i).getField("id")).isEqualTo(records.get(i).getField("id"));
      Assertions.assertThat(read.get(i).getField("data"))
          .isEqualTo(records.get(i).getField("data"));
      Assertions.assertThat(read.get(i).getField("country")).isEqualTo("US");
    }
  }

  @Test
  public void testReadSelectsOnlyDefaultColumn() throws IOException {
    OutputFile file = writeFile();

    Schema onlyDefault =
        new Schema(
            Types.NestedField.optional("country")
                .withId(3)
                .ofType(Types.StringType.get())
                .withInitialDefault(Expressions.lit("US"))
                .build());

    List<Record> read;
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(onlyDefault)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(onlyDefault, fileSchema))
            .build()) {
      read = Lists.newArrayList(reader);
    }

    Assertions.assertThat(read).hasSize(records.size());
    for (Record record : read) {
      Assertions.assertThat(record.getField("country")).isEqualTo("US");
    }
  }
}
