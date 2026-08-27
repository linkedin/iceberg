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

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.DeleteCounter;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkExecutorCache;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Base class of Spark readers.
 *
 * @param <T> is the Java class returned by this reader whose objects contain one or more rows.
 */
public abstract class BaseReader<T, TaskT extends ScanTask> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseReader.class);

  private final Table table;
  private final Schema tableSchema;
  private final Schema expectedSchema;
  private final boolean caseSensitive;
  private final NameMapping nameMapping;
  private final ScanTaskGroup<TaskT> taskGroup;
  private final Iterator<TaskT> tasks;
  private final DeleteCounter counter;

  private Map<String, InputFile> lazyInputFiles;
  private CloseableIterator<T> currentIterator;
  private T current = null;
  private TaskT currentTask = null;

  BaseReader(
      Table table,
      ScanTaskGroup<TaskT> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive) {
    this.table = table;
    this.taskGroup = taskGroup;
    this.tasks = taskGroup.tasks().iterator();
    this.currentIterator = CloseableIterator.empty();
    this.tableSchema = tableSchema;
    this.expectedSchema = expectedSchema;
    this.caseSensitive = caseSensitive;
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    this.nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    this.counter = new DeleteCounter();
  }

  protected abstract CloseableIterator<T> open(TaskT task);

  protected abstract Stream<ContentFile<?>> referencedFiles(TaskT task);

  protected Schema expectedSchema() {
    return expectedSchema;
  }

  protected boolean caseSensitive() {
    return caseSensitive;
  }

  protected NameMapping nameMapping() {
    return nameMapping;
  }

  protected Table table() {
    return table;
  }

  protected DeleteCounter counter() {
    return counter;
  }

  public boolean next() throws IOException {
    try {
      while (true) {
        if (currentIterator.hasNext()) {
          this.current = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          this.currentIterator.close();
          this.currentTask = tasks.next();
          this.currentIterator = open(currentTask);
        } else {
          this.currentIterator.close();
          return false;
        }
      }
    } catch (IOException | RuntimeException e) {
      if (currentTask != null && !currentTask.isDataTask()) {
        String filePaths =
            referencedFiles(currentTask)
                .map(file -> file.path().toString())
                .collect(Collectors.joining(", "));
        LOG.error("Error reading file(s): {}", filePaths, e);
      }
      throw e;
    }
  }

  public T get() {
    return current;
  }

  @Override
  public void close() throws IOException {
    InputFileBlockHolder.unset();

    // close the current iterator
    this.currentIterator.close();

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }

  protected InputFile getInputFile(String location) {
    return inputFiles().get(location);
  }

  private Map<String, InputFile> inputFiles() {
    if (lazyInputFiles == null) {
      this.lazyInputFiles =
          EncryptingFileIO.combine(table().io(), table().encryption())
              .bulkDecrypt(
                  () -> taskGroup.tasks().stream().flatMap(this::referencedFiles).iterator());
    }

    return lazyInputFiles;
  }

  protected Map<Integer, ?> constantsMap(ContentScanTask<?> task, Schema readSchema) {
    if (readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
      StructType partitionType = Partitioning.partitionType(table);
      return PartitionUtil.constantsMap(task, partitionType, BaseReader::convertConstant);
    } else {
      return PartitionUtil.constantsMap(task, BaseReader::convertConstant);
    }
  }

  /**
   * Converts a constant (partition value or initial-default) to Spark's in-memory representation.
   *
   * <p>List/map/struct branches are unused until {@code NestedField.castDefault} allows non-null
   * nested types.
   */
  public static Object convertConstant(Type type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case STRUCT:
        StructType structType = type.asStructType();

        if (structType.fields().isEmpty()) {
          return new GenericInternalRow();
        }

        InternalRow ret = new GenericInternalRow(structType.fields().size());
        for (int i = 0; i < structType.fields().size(); i++) {
          NestedField field = structType.fields().get(i);
          Type fieldType = field.type();
          if (value instanceof Map) {
            ret.update(i, convertConstant(field.type(), ((Map<?, ?>) value).get(field.name())));
          } else {
            ret.update(
                i,
                convertConstant(
                    fieldType, ((StructLike) value).get(i, fieldType.typeId().javaClass())));
          }
        }
        return ret;
      case LIST:
        List<?> javaList =
            ((Collection<?>) value)
                .stream()
                    .map(e -> convertConstant(type.asListType().elementType(), e))
                    .collect(Collectors.toList());
        return ArrayData.toArrayData(
            JavaConverters.collectionAsScalaIterableConverter(javaList).asScala().toSeq());
      case MAP:
        List<Object> keyList = Lists.newArrayList();
        List<Object> valueList = Lists.newArrayList();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          keyList.add(convertConstant(type.asMapType().keyType(), entry.getKey()));
          valueList.add(convertConstant(type.asMapType().valueType(), entry.getValue()));
        }
        return new ArrayBasedMapData(
            new GenericArrayData(keyList.toArray()), new GenericArrayData(valueList.toArray()));
      case DECIMAL:
        return Decimal.apply((BigDecimal) value);
      case STRING:
        if (value instanceof Utf8) {
          Utf8 utf8 = (Utf8) value;
          return UTF8String.fromBytes(utf8.getBytes(), 0, utf8.getByteLength());
        }
        return UTF8String.fromString(value.toString());
      case FIXED:
        if (value instanceof byte[]) {
          return value;
        } else if (value instanceof GenericData.Fixed) {
          return ((GenericData.Fixed) value).bytes();
        }
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case BINARY:
        return ByteBuffers.toByteArray((ByteBuffer) value);
      default:
    }
    return value;
  }

  protected class SparkDeleteFilter extends DeleteFilter<InternalRow> {
    private final InternalRowWrapper asStructLike;

    SparkDeleteFilter(String filePath, List<DeleteFile> deletes, DeleteCounter counter) {
      super(filePath, deletes, tableSchema, expectedSchema, counter);
      this.asStructLike = new InternalRowWrapper(SparkSchemaUtil.convert(requiredSchema()));
    }

    @Override
    protected StructLike asStructLike(InternalRow row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return BaseReader.this.getInputFile(location);
    }

    @Override
    protected void markRowDeleted(InternalRow row) {
      if (!row.getBoolean(columnIsDeletedPosition())) {
        row.setBoolean(columnIsDeletedPosition(), true);
        counter().increment();
      }
    }

    @Override
    protected DeleteLoader newDeleteLoader() {
      return new CachingDeleteLoader(this::loadInputFile);
    }

    private class CachingDeleteLoader extends BaseDeleteLoader {
      private final SparkExecutorCache cache;

      CachingDeleteLoader(Function<DeleteFile, InputFile> loadInputFile) {
        super(loadInputFile);
        this.cache = SparkExecutorCache.getOrCreate();
      }

      @Override
      protected boolean canCache(long size) {
        return cache != null && size < cache.maxEntrySize();
      }

      @Override
      protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
        return cache.getOrLoad(table().name(), key, valueSupplier, valueSize);
      }
    }
  }
}
