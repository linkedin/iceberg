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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.avro.ValueReader;
import org.apache.iceberg.avro.ValueReaders;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SparkValueReaders {

  private SparkValueReaders() {
  }

  static ValueReader<UTF8String> strings() {
    return StringReader.INSTANCE;
  }

  static ValueReader<UTF8String> enums(List<String> symbols) {
    return new EnumReader(symbols);
  }

  static ValueReader<UTF8String> uuids() {
    return UUIDReader.INSTANCE;
  }

  static ValueReader<Decimal> decimal(ValueReader<byte[]> unscaledReader, int scale) {
    return new DecimalReader(unscaledReader, scale);
  }

  static ValueReader<ArrayData> array(ValueReader<?> elementReader) {
    return new ArrayReader(elementReader);
  }

  static ValueReader<ArrayBasedMapData> arrayMap(ValueReader<?> keyReader,
                                                 ValueReader<?> valueReader) {
    return new ArrayMapReader(keyReader, valueReader);
  }

  static ValueReader<ArrayBasedMapData> map(ValueReader<?> keyReader, ValueReader<?> valueReader) {
    return new MapReader(keyReader, valueReader);
  }

  static ValueReader<InternalRow> struct(List<ValueReader<?>> readers, Types.StructType struct,
                                         Map<Integer, ?> idToConstant) {
    return new StructReader(readers, struct, idToConstant);
  }

  static ValueReader<InternalRow> union(Schema schema, List<ValueReader<?>> readers, Type expected) {
    return new UnionReader(schema, readers, expected);
  }

  private static class StringReader implements ValueReader<UTF8String> {
    private static final StringReader INSTANCE = new StringReader();

    private StringReader() {
    }

    @Override
    public UTF8String read(Decoder decoder, Object reuse) throws IOException {
      // use the decoder's readString(Utf8) method because it may be a resolving decoder
      Utf8 utf8 = null;
      if (reuse instanceof UTF8String) {
        utf8 = new Utf8(((UTF8String) reuse).getBytes());
      }

      Utf8 string = decoder.readString(utf8);
      return UTF8String.fromBytes(string.getBytes(), 0, string.getByteLength());
//      int length = decoder.readInt();
//      byte[] bytes = new byte[length];
//      decoder.readFixed(bytes, 0, length);
//      return UTF8String.fromBytes(bytes);
    }
  }

  private static class EnumReader implements ValueReader<UTF8String> {
    private static final Logger LOG = LoggerFactory.getLogger(EnumReader.class);
    private final UTF8String[] symbols;

    private EnumReader(List<String> symbols) {
      this.symbols = new UTF8String[symbols.size()];
      for (int i = 0; i < this.symbols.length; i += 1) {
        this.symbols[i] = UTF8String.fromBytes(symbols.get(i).getBytes(StandardCharsets.UTF_8));
      }
    }

    @Override
    public UTF8String read(Decoder decoder, Object ignore) throws IOException {
      int index = decoder.readEnum();
      if (index >= symbols.length) {
        LOG.error("Unable to read the symbol in the given enum as the deserialized index {} is out of bound", index);
        throw new ArrayIndexOutOfBoundsException();
      } else {
        return symbols[index];
      }
    }
  }

  private static class UUIDReader implements ValueReader<UTF8String> {
    private static final ThreadLocal<ByteBuffer> BUFFER = ThreadLocal.withInitial(() -> {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.order(ByteOrder.BIG_ENDIAN);
      return buffer;
    });

    private static final UUIDReader INSTANCE = new UUIDReader();

    private UUIDReader() {
    }

    @Override
    @SuppressWarnings("ByteBufferBackingArray")
    public UTF8String read(Decoder decoder, Object reuse) throws IOException {
      ByteBuffer buffer = BUFFER.get();
      buffer.rewind();

      decoder.readFixed(buffer.array(), 0, 16);

      return UTF8String.fromString(UUIDUtil.convert(buffer).toString());
    }
  }

  private static class DecimalReader implements ValueReader<Decimal> {
    private final ValueReader<byte[]> bytesReader;
    private final int scale;

    private DecimalReader(ValueReader<byte[]> bytesReader, int scale) {
      this.bytesReader = bytesReader;
      this.scale = scale;
    }

    @Override
    public Decimal read(Decoder decoder, Object reuse) throws IOException {
      byte[] bytes = bytesReader.read(decoder, null);
      return Decimal.apply(new BigDecimal(new BigInteger(bytes), scale));
    }
  }

  private static class ArrayReader implements ValueReader<ArrayData> {
    private final ValueReader<?> elementReader;
    private final List<Object> reusedList = Lists.newArrayList();

    private ArrayReader(ValueReader<?> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public GenericArrayData read(Decoder decoder, Object reuse) throws IOException {
      reusedList.clear();
      long chunkLength = decoder.readArrayStart();

      while (chunkLength > 0) {
        for (int i = 0; i < chunkLength; i += 1) {
          reusedList.add(elementReader.read(decoder, null));
        }

        chunkLength = decoder.arrayNext();
      }

      // this will convert the list to an array so it is okay to reuse the list
      return new GenericArrayData(reusedList.toArray());
    }
  }

  private static class ArrayMapReader implements ValueReader<ArrayBasedMapData> {
    private final ValueReader<?> keyReader;
    private final ValueReader<?> valueReader;

    private final List<Object> reusedKeyList = Lists.newArrayList();
    private final List<Object> reusedValueList = Lists.newArrayList();

    private ArrayMapReader(ValueReader<?> keyReader, ValueReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public ArrayBasedMapData read(Decoder decoder, Object reuse) throws IOException {
      reusedKeyList.clear();
      reusedValueList.clear();

      long chunkLength = decoder.readArrayStart();

      while (chunkLength > 0) {
        for (int i = 0; i < chunkLength; i += 1) {
          reusedKeyList.add(keyReader.read(decoder, null));
          reusedValueList.add(valueReader.read(decoder, null));
        }

        chunkLength = decoder.arrayNext();
      }

      return new ArrayBasedMapData(
          new GenericArrayData(reusedKeyList.toArray()),
          new GenericArrayData(reusedValueList.toArray()));
    }
  }

  private static class MapReader implements ValueReader<ArrayBasedMapData> {
    private final ValueReader<?> keyReader;
    private final ValueReader<?> valueReader;

    private final List<Object> reusedKeyList = Lists.newArrayList();
    private final List<Object> reusedValueList = Lists.newArrayList();

    private MapReader(ValueReader<?> keyReader, ValueReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public ArrayBasedMapData read(Decoder decoder, Object reuse) throws IOException {
      reusedKeyList.clear();
      reusedValueList.clear();

      long chunkLength = decoder.readMapStart();

      while (chunkLength > 0) {
        for (int i = 0; i < chunkLength; i += 1) {
          reusedKeyList.add(keyReader.read(decoder, null));
          reusedValueList.add(valueReader.read(decoder, null));
        }

        chunkLength = decoder.mapNext();
      }

      return new ArrayBasedMapData(
          new GenericArrayData(reusedKeyList.toArray()),
          new GenericArrayData(reusedValueList.toArray()));
    }
  }

  static class StructReader extends ValueReaders.StructReader<InternalRow> {
    private final int numFields;

    protected StructReader(List<ValueReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
      super(readers, struct, idToConstant);
      this.numFields = readers.size();
    }

    @Override
    protected InternalRow reuseOrCreate(Object reuse) {
      if (reuse instanceof GenericInternalRow && ((GenericInternalRow) reuse).numFields() == numFields) {
        return (InternalRow) reuse;
      }
      return new GenericInternalRow(numFields);
    }

    @Override
    protected Object get(InternalRow struct, int pos) {
      return null;
    }

    @Override
    protected void set(InternalRow struct, int pos, Object value) {
      if (value != null) {
        struct.update(pos, value);
      } else {
        struct.setNullAt(pos);
      }
    }
  }

  private static class UnionReader implements ValueReader<InternalRow> {
    private final Schema schema;
    private final ValueReader[] readers;
    private final Map<Integer, Integer> projectedFieldIdsToIdxInReturnedRow;
    private boolean isTagFieldProjected;

    private UnionReader(Schema schema, List<ValueReader<?>> readers, Type expected) {
      this.schema = schema;
      this.readers = new ValueReader[readers.size()];
      for (int i = 0; i < this.readers.length; i += 1) {
        this.readers[i] = readers.get(i);
      }
      this.projectedFieldIdsToIdxInReturnedRow = new HashMap<>();
      this.isTagFieldProjected = false;
      int idxInReturnedRow = 0;
      for (Types.NestedField icebergField : expected.asStructType().fields()) {
        String fieldName = icebergField.name();
        if (fieldName.equals("tag")) {
          this.isTagFieldProjected = true;
          idxInReturnedRow++;
          continue;
        }
        int fieldId = Integer.valueOf(fieldName.substring(5));
        this.projectedFieldIdsToIdxInReturnedRow.put(fieldId, idxInReturnedRow++);
      }
    }

    @Override
    public InternalRow read(Decoder decoder, Object reuse) throws IOException {
      // first we need to filter out NULL alternative if it exists in the union schema
      int nullIndex = -1;
      List<Schema> alts = schema.getTypes();
      for (int i = 0; i < alts.size(); i++) {
        Schema alt = alts.get(i);
        if (Objects.equals(alt.getType(), Schema.Type.NULL)) {
          nullIndex = i;
          break;
        }
      }

      int index = decoder.readIndex();
      if (index == nullIndex) {
        // if it is a null data, directly return null as the whole union result
        // we know for sure it is a null so the casting will always work.
        return (InternalRow) readers[nullIndex].read(decoder, reuse);
      }

      // otherwise, we need to return an InternalRow as a struct data
      int numOfFields = isTagFieldProjected ?
          projectedFieldIdsToIdxInReturnedRow.size() + 1 : projectedFieldIdsToIdxInReturnedRow.size();
      InternalRow struct = new GenericInternalRow(numOfFields);
      for (int i = 0; i < struct.numFields(); i += 1) {
        struct.setNullAt(i);
      }
      int fieldId = (nullIndex < 0 || index < nullIndex) ? index : index - 1;
      if (isTagFieldProjected) {
        struct.setInt(0, fieldId);
      }

      Object value = readers[index].read(decoder, reuse);
      if (projectedFieldIdsToIdxInReturnedRow.containsKey(fieldId)) {
        struct.update(projectedFieldIdsToIdxInReturnedRow.get(fieldId), value);
      }

      return struct;
    }
  }
}
