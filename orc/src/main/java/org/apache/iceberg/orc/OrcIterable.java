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

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

/**
 * Iterable used to read rows from ORC.
 */
class OrcIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private final Configuration config;
  private final Schema schema;
  private final InputFile file;
  private final Long start;
  private final Long length;
  private final OrcRowFilter rowFilter;
  private final Function<TypeDescription, OrcValueReader<?>> readerFunction;

  OrcIterable(InputFile file, Configuration config, Schema schema,
              Long start, Long length,
              Function<TypeDescription, OrcValueReader<?>> readerFunction, OrcRowFilter rowFilter) {
    this.schema = schema;
    this.readerFunction = readerFunction;
    this.file = file;
    this.start = start;
    this.length = length;
    this.config = config;
    this.rowFilter = rowFilter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<T> iterator() {
    Reader orcFileReader = ORC.newFileReader(file, config);
    addCloseable(orcFileReader);
    addCloseable(rowFilter);
    TypeDescription readOrcSchema = ORCSchemaUtil.buildOrcProjection(schema, orcFileReader.getSchema());
    TypeDescription rowFilterSchema = rowFilter.expandSchema(readOrcSchema);
    return new OrcIterator(newOrcIterator(file, rowFilterSchema, start, length, orcFileReader),
        readerFunction.apply(readOrcSchema), rowFilter);
  }

  private static VectorizedRowBatchIterator newOrcIterator(InputFile file,
                                                           TypeDescription readerSchema,
                                                           Long start, Long length,
                                                           Reader orcFileReader) {
    final Reader.Options options = orcFileReader.options();
    if (start != null) {
      options.range(start, length);
    }
    options.schema(readerSchema);

    try {
      return new VectorizedRowBatchIterator(file.location(), readerSchema, orcFileReader.rows(options));
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to get ORC rows for file: %s", file);
    }
  }

  private static class OrcIterator<T> implements Iterator<T> {

    private int nextRow;
    private VectorizedRowBatch current;
    private final VectorizedRowBatchIterator batchIter;
    private final OrcValueReader<T> reader;
    private T rowObj = null;
    private OrcRowFilter rowFilter;
    private OrcRow orcRow;

    OrcIterator(VectorizedRowBatchIterator batchIter, OrcValueReader<T> reader, OrcRowFilter rowFilter) {
      this.batchIter = batchIter;
      this.reader = reader;
      this.current = null;
      this.nextRow = 0;
      this.rowFilter = rowFilter;
      this.orcRow = new OrcRow();
    }

    @Override
    public boolean hasNext() {
      while (rowObj == null) {
        if (current == null || nextRow >= current.size) {
          if (!batchIter.hasNext()) {
            break;
          }
          current = batchIter.next();
          nextRow = 0;
        }

        // read opal specific columns
        orcRow.batch = current;
        orcRow.row = nextRow;
        if (rowFilter.accept(orcRow)) {
          rowObj = this.reader.read(current, nextRow);
        }
        nextRow++;
      }
      return rowObj != null;
    }

    @Override
    public T next() {
      hasNext();
      T tmp = rowObj;
      rowObj = null;
      return tmp;
    }
  }
}
