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

package org.apache.iceberg.hive.legacy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LegacyHiveTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(LegacyHiveTableOperations.class);

  private final HiveClientPool metaClients;
  private final String database;
  private final String tableName;
  private final Configuration conf;

  private FileIO fileIO;

  protected LegacyHiveTableOperations(Configuration conf, HiveClientPool metaClients, String database, String table) {
    this.conf = conf;
    this.metaClients = metaClients;
    this.database = database;
    this.tableName = table;
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }

    return fileIO;
  }

  @Override
  protected void doRefresh() {
    try {
      org.apache.hadoop.hive.metastore.api.Table hiveTable =
          metaClients.run(client -> client.getTable(database, tableName));

      Schema schema = LegacyHiveTableUtils.getSchema(hiveTable);
      PartitionSpec spec = LegacyHiveTableUtils.getPartitionSpec(hiveTable, schema);

      TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, hiveTable.getSd().getLocation(),
          LegacyHiveTableUtils.getTableProperties(hiveTable));
      metadata = metadata.replaceCurrentSnapshot(getDummySnapshot());
      setCurrentMetadata(metadata);
    } catch (TException e) {
      String errMsg = String.format("Failed to get table info from metastore %s.%s", database, tableName);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }
    setShouldRefresh(false);
  }

  DirectoryInfo getDirectoryInfo() {
    Preconditions.checkArgument(current().spec().fields().isEmpty(),
        "getDirectoryInfo only allowed for unpartitioned tables");
    try {
      org.apache.hadoop.hive.metastore.api.Table hiveTable =
          metaClients.run(client -> client.getTable(database, tableName));

      return LegacyHiveTableUtils.toDirectoryInfo(hiveTable);
    } catch (TException e) {
      String errMsg = String.format("Failed to get table info from metastore %s.%s", database, tableName);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }
  }

  List<DirectoryInfo> getDirectoryInfosByFilter(Expression expression) {
    Preconditions.checkArgument(!current().spec().fields().isEmpty(),
        "getDirectoryInfosByFilter only allowed for partitioned tables");
    try {
      LOG.info("Fetching partitions for {}.{} with expression: {}", database, tableName, expression);
      Set<String> partitionColumnNames = current().spec()
          .identitySourceIds()
          .stream()
          .map(id -> current().schema().findColumnName(id))
          .collect(Collectors.toSet());
      Expression simplified = HiveExpressions.simplifyPartitionFilter(expression, partitionColumnNames);
      LOG.info("Simplified expression for {}.{} to {}", database, tableName, simplified);
      final List<Partition> partitions;
      // If simplifyPartitionFilter returns TRUE, there are no filters on partition columns or the filter expression is
      // going to match all partitions
      if (simplified.op() == Expression.Operation.TRUE) {
        partitions = metaClients.run(client -> client.listPartitionsByFilter(database, tableName, null, (short) -1));
      } else if (simplified.op() == Expression.Operation.FALSE) {
        // If simplifyPartitionFilter returns FALSE, no partitions are going to match the filter expression
        partitions = ImmutableList.of();
      } else {
        String partitionFilterString = HiveExpressions.toPartitionFilterString(simplified);
        LOG.info("Listing partitions for {}.{} with filter string: {}", database, tableName, partitionFilterString);
        partitions = metaClients.run(
            client -> client.listPartitionsByFilter(database, tableName, partitionFilterString, (short) -1));
      }

      return LegacyHiveTableUtils.toDirectoryInfos(partitions);
    } catch (TException e) {
      String errMsg = String.format("Failed to get partition info from metastore for %s.%s", database, tableName);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getPartitionsByFilter", e);
    }
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public String metadataFileLocation(String filename) {
    throw new UnsupportedOperationException(
        "Metadata file location not available for Hive tables without Iceberg metadata");
  }

  @Override
  public LocationProvider locationProvider() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  /**
   * A dummy snapshot to represent a table. Without a snapshot, Iceberg will not scan the table and instead eagerly exit
   */
  private Snapshot getDummySnapshot() {
    final long currentTime = System.currentTimeMillis();
    final long snapshotId = newSnapshotId();

    return new Snapshot() {
      @Override
      public long snapshotId() {
        return snapshotId;
      }

      @Override
      public Long parentId() {
        return null;
      }

      @Override
      public long timestampMillis() {
        return currentTime;
      }

      @Override
      public List<ManifestFile> manifests() {
        throw new UnsupportedOperationException(
            "Manifest files not available for Hive Tables without Iceberg metadata");
      }

      @Override
      public String operation() {
        return DataOperations.APPEND;
      }

      @Override
      public Map<String, String> summary() {
        return ImmutableMap.of();
      }

      @Override
      public Iterable<DataFile> addedFiles() {
        throw new UnsupportedOperationException(
            "Added files information not available for Hive Tables without Iceberg metadata");
      }

      @Override
      public Iterable<DataFile> deletedFiles() {
        throw new UnsupportedOperationException(
            "Deleted files information not available for Hive Tables without Iceberg metadata");
      }

      @Override
      public String manifestListLocation() {
        throw new UnsupportedOperationException("Manifest list not available for Hive Tables without Iceberg metadata");
      }
    };
  }
}
