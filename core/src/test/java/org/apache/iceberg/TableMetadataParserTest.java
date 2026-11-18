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

import static org.apache.iceberg.PartitionSpec.unpartitioned;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.apache.iceberg.TableMetadataParser.Codec;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.BooleanType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TableMetadataParserTest {

  private static final Schema SCHEMA = new Schema(optional(1, "b", BooleanType.get()));

  @Parameterized.Parameters(name = "codecName = {0}")
  public static Object[] parameters() {
    return new Object[] {"none", "gzip"};
  }

  private final String codecName;

  public TableMetadataParserTest(String codecName) {
    this.codecName = codecName;
  }

  @Test
  public void testCompressionProperty() throws IOException {
    Codec codec = Codec.fromName(codecName);
    String fileExtension = getFileExtension(codec);
    String fileName = "v3" + fileExtension;
    OutputFile outputFile = Files.localOutput(fileName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, codecName);
    String location = "file://tmp/db/table";
    TableMetadata metadata = newTableMetadata(SCHEMA, unpartitioned(), location, properties);
    TableMetadataParser.write(metadata, outputFile);
    Assert.assertEquals(codec == Codec.GZIP, isCompressed(fileName));
    TableMetadata actualMetadata =
        TableMetadataParser.read((FileIO) null, Files.localInput(new File(fileName)));
    verifyMetadata(metadata, actualMetadata);
  }

  @Test
  public void testCloseFailureIsNotSelfSuppressed() {
    Codec codec = Codec.fromName(codecName);
    CloseFailureOutputFile outputFile = new CloseFailureOutputFile("v3" + getFileExtension(codec));
    TableMetadata metadata =
        newTableMetadata(SCHEMA, unpartitioned(), "file://tmp/db/table", Maps.newHashMap());

    RuntimeIOException exception =
        Assert.assertThrows(
            RuntimeIOException.class, () -> TableMetadataParser.write(metadata, outputFile));

    Assert.assertSame(outputFile.closeFailure(), exception.getCause());
    Assert.assertEquals(1, outputFile.closeCount());
  }

  @After
  public void cleanup() throws IOException {
    Codec codec = Codec.fromName(codecName);
    Path metadataFilePath = Paths.get("v3" + getFileExtension(codec));
    java.nio.file.Files.deleteIfExists(metadataFilePath);
  }

  private void verifyMetadata(TableMetadata expected, TableMetadata actual) {
    Assert.assertEquals(expected.schema().asStruct(), actual.schema().asStruct());
    Assert.assertEquals(expected.location(), actual.location());
    Assert.assertEquals(expected.lastColumnId(), actual.lastColumnId());
    Assert.assertEquals(expected.properties(), actual.properties());
  }

  private boolean isCompressed(String path) throws IOException {
    try (InputStream ignored = new GZIPInputStream(new FileInputStream(new File(path)))) {
      return true;
    } catch (ZipException e) {
      if (e.getMessage().equals("Not in GZIP format")) {
        return false;
      } else {
        throw e;
      }
    }
  }

  private static class CloseFailureOutputFile implements OutputFile {
    private final String location;
    private final CloseFailureOutputStream stream = new CloseFailureOutputStream();

    private CloseFailureOutputFile(String location) {
      this.location = location;
    }

    @Override
    public PositionOutputStream create() {
      return stream;
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return stream;
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      throw new UnsupportedOperationException();
    }

    private IOException closeFailure() {
      return stream.closeFailure;
    }

    private int closeCount() {
      return stream.closeCount;
    }
  }

  private static class CloseFailureOutputStream extends PositionOutputStream {
    private final IOException closeFailure = new IOException("close failed");
    private long position = 0;
    private int closeCount = 0;

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void write(int value) {
      position += 1;
    }

    @Override
    public void close() throws IOException {
      closeCount += 1;
      throw closeFailure;
    }
  }
}
