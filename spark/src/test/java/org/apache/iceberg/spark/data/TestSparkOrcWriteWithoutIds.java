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

package org.apache.iceberg.spark.data;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.TableProperties.ORC_DO_NOT_WRITE_FIELD_IDS;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestSparkOrcWriteWithoutIds {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  @Test
  public void testWriteWithoutIds() throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    Iterable<InternalRow> rows = RandomData.generateSpark(SCHEMA, 1, 0L);
    FileAppender<InternalRow> writer = ORC.write(Files.localOutput(testFile))
        .config(ORC_DO_NOT_WRITE_FIELD_IDS, String.valueOf(true))
        .createWriterFunc(SparkOrcWriter::new)
        .schema(SCHEMA)
        .build();

    writer.addAll(rows);
    writer.close();

    // read back using ORC native file reader
    Reader reader = OrcFile.createReader(new Path(testFile.getPath()), new OrcFile.ReaderOptions(new Configuration()));
    Assert.assertFalse(ORCSchemaUtil.hasIds(reader.getSchema()));
  }
}
