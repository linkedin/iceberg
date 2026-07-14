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
package org.apache.iceberg.hadoop;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link OutputFile} implementation using the Hadoop {@link FileSystem} API. */
public class HadoopOutputFile implements OutputFile, NativelyEncryptedFile {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopOutputFile.class);
  private static final short DEFAULT_REPLICATION_FACTOR = 3;
  private final FileSystem fs;
  private final Path path;
  private final Configuration conf;
  private final short replication;
  private NativeFileCryptoParameters nativeEncryptionParameters;

  public static OutputFile fromLocation(CharSequence location, Configuration conf) {
    Path path = new Path(location.toString());
    return fromPath(path, conf);
  }

  public static OutputFile fromLocation(CharSequence location, FileSystem fs) {
    Path path = new Path(location.toString());
    return fromPath(path, fs);
  }

  public static OutputFile fromPath(Path path, Configuration conf) {
    FileSystem fs = Util.getFs(path, conf);
    return fromPath(path, fs, conf);
  }

  public static OutputFile fromPath(Path path, Configuration conf, Map<String, String> properties) {
    short replicationFactor = DEFAULT_REPLICATION_FACTOR;
    if (properties != null) {
      String replicationFactorAsString = properties.get(OutputFileFactory.FILE_REPLICATION_FACTOR);
      if (replicationFactorAsString != null) {
        try {
          replicationFactor = Short.parseShort(replicationFactorAsString);
        } catch (NumberFormatException e) {
          LOG.warn(
              "Failed to parse replication factor: {}, defaulting to {}",
              replicationFactorAsString,
              DEFAULT_REPLICATION_FACTOR,
              e);
        }
      }
    }
    return fromPath(path, conf, replicationFactor);
  }

  public static OutputFile fromPath(Path path, Configuration conf, short replication) {
    FileSystem fs = Util.getFs(path, conf);
    return new HadoopOutputFile(fs, path, conf, replication);
  }

  public static OutputFile fromPath(Path path, FileSystem fs) {
    return fromPath(path, fs, fs.getConf());
  }

  public static OutputFile fromPath(Path path, FileSystem fs, Configuration conf) {
    return new HadoopOutputFile(fs, path, conf, (short) -1);
  }

  private HadoopOutputFile(FileSystem fs, Path path, Configuration conf, short replication) {
    this.fs = fs;
    this.path = path;
    this.conf = conf;
    this.replication = replication;
  }

  @Override
  public PositionOutputStream create() {
    try {
      if (replication > 0) {
        return HadoopStreams.wrap(
            fs.create(
                path,
                false /* overwrite */,
                conf.getInt(
                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),
                replication,
                fs.getDefaultBlockSize(path)));
      } else {
        return HadoopStreams.wrap(fs.create(path, false /* createOrOverwrite */));
      }
    } catch (FileAlreadyExistsException e) {
      throw new AlreadyExistsException(e, "Path already exists: %s", path);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create file: %s", path);
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    try {
      if (replication > 0) {
        return HadoopStreams.wrap(
            fs.create(
                path,
                true /* overwrite */,
                conf.getInt(
                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),
                replication,
                fs.getDefaultBlockSize(path)));
      } else {
        return HadoopStreams.wrap(fs.create(path, true /* createOrOverwrite */));
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create file: %s", path);
    }
  }

  public Path getPath() {
    return path;
  }

  public Configuration getConf() {
    return conf;
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  /** Returns the configured replication factor, or a non-positive value when not set. */
  public short replication() {
    return replication;
  }

  @Override
  public String location() {
    return path.toString();
  }

  @Override
  public InputFile toInputFile() {
    return HadoopInputFile.fromPath(path, fs, conf);
  }

  @Override
  public String toString() {
    return location();
  }

  @Override
  public NativeFileCryptoParameters nativeCryptoParameters() {
    return nativeEncryptionParameters;
  }

  @Override
  public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
    this.nativeEncryptionParameters = nativeCryptoParameters;
  }
}
