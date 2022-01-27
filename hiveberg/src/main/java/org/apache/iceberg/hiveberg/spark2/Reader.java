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

package org.apache.iceberg.hiveberg.spark2;

import java.util.OptionalLong;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hiveberg.LegacyHiveTable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.Stats;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.Statistics;


class Reader extends org.apache.iceberg.spark.source.Reader {
  Reader(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
      boolean caseSensitive, DataSourceOptions options) {
    super(table, io, encryptionManager, caseSensitive, options);
  }

  @Override
  public Statistics estimateStatistics() {
    Table table = super.getTable();
    if (table instanceof LegacyHiveTable) {
      // We currently don't have reliable stats for Hive tables
      return EMPTY_STATS;
    }

    // its a fresh table, no data
    if (table.currentSnapshot() == null) {
      return new Stats(0L, 0L);
    }

    // estimate stats using snapshot summary only for partitioned tables (metadata tables are unpartitioned)
    if (!table.spec().isUnpartitioned() && filterExpression() == Expressions.alwaysTrue()) {
      long totalRecords = PropertyUtil.propertyAsLong(table.currentSnapshot().summary(),
          SnapshotSummary.TOTAL_RECORDS_PROP, Long.MAX_VALUE);
      return new Stats(SparkSchemaUtil.estimateSize(lazyType(), totalRecords), totalRecords);
    }

    long sizeInBytes = 0L;
    long numRows = 0L;

    for (CombinedScanTask task : tasks()) {
      for (FileScanTask file : task.files()) {
        sizeInBytes += file.length();
        numRows += file.file().recordCount();
      }
    }

    return new Stats(sizeInBytes, numRows);
  }

  private static final Statistics EMPTY_STATS = new Statistics() {
    @Override
    public OptionalLong sizeInBytes() {
      return OptionalLong.empty();
    }

    @Override
    public OptionalLong numRows() {
      return OptionalLong.empty();
    }
  };
}
