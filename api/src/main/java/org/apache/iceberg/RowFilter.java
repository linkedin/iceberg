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

import java.io.Closeable;
import java.io.IOException;

public interface RowFilter<S, D> extends Closeable {
  RowFilter DEFAULT = new RowFilter() {
    @Override
    public void close() throws IOException {
    }

    @Override
    public Object expandSchema(Object schema) {
      return schema;
    }

    @Override
    public boolean accept(Object row) {
      return true;
    }
  };

  /**
   * Expand the columns if required for the row filtering.
   * The returned schema will be used to read the rows.
   * The fetched rows will be converted back to the original
   * schema at the end.
   *
   * To make the conversion efficient, the expansion should
   * be backward compatible, which means this method should
   * append columns to the end of the original schema, so
   * that reader can still use the original schema to read
   * data.
   */
  S expandSchema(S originSchema);
  boolean accept(D row);
}
