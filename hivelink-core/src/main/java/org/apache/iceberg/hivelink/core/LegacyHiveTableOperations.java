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

package org.apache.iceberg.hivelink.core;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hivelink.core.utils.FileSystemUtils;
import org.apache.iceberg.hivelink.core.utils.MappingUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LegacyHiveTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(LegacyHiveTableOperations.class);
  private static final long INITIAL_SEQUENCE_NUMBER = 0;
  private static final int DEFAULT_TABLE_FORMAT_VERSION = 1;
  private static final int INITIAL_SPEC_ID = 0;

  private final HiveClientPool metaClients;
  private final String databaseName;
  private final String tableName;

  private final String fullName;
  private final Configuration conf;

  private FileIO fileIO;

  protected LegacyHiveTableOperations(Configuration conf, HiveClientPool metaClients, String database, String table) {
    this.conf = conf;
    this.metaClients = metaClients;
    this.databaseName = database;
    this.tableName = table;
    this.fullName = database + "." + table;
  }

  @Override
  protected String tableName() {
    return fullName;
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
          metaClients.run(client -> client.getTable(databaseName, tableName));

      Schema schema = LegacyHiveTableUtils.getSchema(hiveTable);
      PartitionSpec spec = LegacyHiveTableUtils.getPartitionSpec(hiveTable, schema);

      Map<String, String> tableProperties = Maps.newHashMap(LegacyHiveTableUtils.getTableProperties(hiveTable));
      // Provide a case insensitive name mapping for Hive tables
      tableProperties.put(TableProperties.DEFAULT_NAME_MAPPING,
          NameMappingParser.toJson(MappingUtil.create(schema, false)));
      TableMetadata metadata = newTableMetadataWithoutFreshIds(schema, spec,
          hiveTable.getSd().getLocation(), tableProperties);
      setCurrentMetadata(metadata);
    } catch (TException e) {
      String errMsg = String.format("Failed to get table info from metastore %s.%s", databaseName, tableName);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }
    setShouldRefresh(false);
  }

  /**
   * Returns an {@link Iterable} of {@link Iterable}s of {@link DataFile}s which belong to the current table and
   * match the partition predicates from the given expression.
   * <p>
   * Each element in the outer {@link Iterable} maps to an {@link Iterable} of {@link DataFile}s originating from the
   * same directory
   */
  Iterable<Iterable<DataFile>> getFilesByFilter(Expression expression) {
    Iterable<DirectoryInfo> matchingDirectories;
    if (current().spec().fields().isEmpty()) {
      matchingDirectories = ImmutableList.of(getDirectoryInfo());
    } else {
      matchingDirectories = getDirectoryInfosByFilter(expression);
    }

    Iterable<Iterable<DataFile>> filesPerDirectory = Iterables.transform(
        matchingDirectories,
        directory -> {
          List<FileStatus> files;
          if (FileSystemUtils.exists(directory.location(), conf)) {
            files = FileSystemUtils.listFiles(directory.location(), conf);
          } else {
            LOG.warn("Cannot find directory: {}. Skipping.", directory.location());
            files = ImmutableList.of();
          }
          return Iterables.transform(
              files,
              file -> createDataFile(file, current().spec(), directory.partitionData(), directory.format())
          );
        });

    // Note that we return an Iterable of Iterables here so that the TableScan can process iterables of individual
    // directories in parallel hence resulting in a parallel file listing
    return filesPerDirectory;
  }

  private DirectoryInfo getDirectoryInfo() {
    Preconditions.checkArgument(current().spec().fields().isEmpty(),
        "getDirectoryInfo only allowed for unpartitioned tables");
    try {
      org.apache.hadoop.hive.metastore.api.Table hiveTable =
          metaClients.run(client -> client.getTable(databaseName, tableName));

      return LegacyHiveTableUtils.toDirectoryInfo(hiveTable);
    } catch (TException e) {
      String errMsg = String.format("Failed to get table info for %s.%s from metastore", databaseName, tableName);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getDirectoryInfo", e);
    }
  }

  private List<DirectoryInfo> getDirectoryInfosByFilter(Expression expression) {
    Preconditions.checkArgument(!current().spec().fields().isEmpty(),
        "getDirectoryInfosByFilter only allowed for partitioned tables");
    try {
      LOG.info("Fetching partitions for {}.{} with expression: {}", databaseName, tableName, expression);
      Set<String> partitionColumnNames = current().spec()
          .identitySourceIds()
          .stream()
          .map(id -> current().schema().findColumnName(id))
          .collect(Collectors.toSet());
      Expression simplified = HiveExpressions.simplifyPartitionFilter(expression, partitionColumnNames);
      Types.StructType partitionSchema = current().spec().partitionType();
      LOG.info("Simplified expression for {}.{} to {}", databaseName, tableName, simplified);

      List<Partition> partitions;
      Expression boundExpression;
      if (simplified.equals(Expressions.alwaysFalse())) {
        // If simplifyPartitionFilter returns FALSE, no partitions are going to match the filter expression
        partitions = ImmutableList.of();
      } else if (simplified.equals(Expressions.alwaysTrue())) {
        // If simplifyPartitionFilter returns TRUE, all partitions are going to match the filter expression
        partitions = metaClients.run(client -> client.listPartitionsByFilter(
            databaseName, tableName, null, (short) -1));
      } else {
        boundExpression = Binder.bind(partitionSchema, simplified, false);
        Evaluator evaluator = new Evaluator(partitionSchema, simplified, false);
        String partitionFilterString = HiveExpressions.toPartitionFilterString(boundExpression);
        LOG.info("Listing partitions for {}.{} with filter string: {}", databaseName, tableName, partitionFilterString);
        try {
          // We first try to use HMS API call to get the filtered partitions.
          partitions = metaClients.run(
              client -> client.listPartitionsByFilter(databaseName, tableName, partitionFilterString, (short) -1));
        } catch (MetaException e) {
          // If the above HMS call fails, we here try to do the partition filtering ourselves,
          // by evaluating all the partitions we got back from HMS against the boundExpression,
          // if the evaluation results in true, we include such partition, if false, we filter.
          List<Partition> allPartitions = metaClients.run(
              client -> client.listPartitionsByFilter(databaseName, tableName, null, (short) -1));
          partitions = allPartitions.stream().filter(partition -> {
            GenericRecord record = GenericRecord.create(partitionSchema);
            for (int i = 0; i < record.size(); i++) {
              String value = partition.getValues().get(i);
              switch (partitionSchema.fields().get(i).type().typeId()) {
                case DATE:
                  record.set(i,
                      (int) LocalDate.parse(value).toEpochDay());
                  break;
                case TIMESTAMP:
                  // This format seems to be matching the hive timestamp column partition string literal value
                  record.set(i,
                      LocalDateTime.parse(value,
                          new DateTimeFormatterBuilder()
                              .parseLenient()
                              .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                              .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                              .toFormatter())
                          .toInstant(ZoneOffset.UTC).toEpochMilli() * 1000);
                  break;
                default:
                  record.set(i, partition.getValues().get(i));
                  break;
              }
            }
            return evaluator.eval(record);
          }).collect(Collectors.toList());
        }
      }

      return LegacyHiveTableUtils.toDirectoryInfos(partitions, current().spec());
    } catch (TException e) {
      String errMsg = String.format("Failed to get partition info for %s.%s + expression %s from metastore",
          databaseName, tableName, expression);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getPartitionsByFilter", e);
    }
  }

  private static DataFile createDataFile(FileStatus fileStatus, PartitionSpec partitionSpec, StructLike partitionData,
                                         FileFormat format) {
    DataFiles.Builder builder = DataFiles.builder(partitionSpec)
        .withPath(fileStatus.getPath().toString())
        .withFormat(format)
        .withFileSizeInBytes(fileStatus.getLen())
        .withMetrics(new Metrics(10000L, null, null, null, null, null));

    if (partitionSpec.fields().isEmpty()) {
      return builder.build();
    } else {
      return builder.withPartition(partitionData).build();
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

  private TableMetadata newTableMetadataWithoutFreshIds(Schema schema,
                                                              PartitionSpec spec,
                                                              String location,
                                                              Map<String, String> properties) {
    return new TableMetadata(null, DEFAULT_TABLE_FORMAT_VERSION, UUID.randomUUID().toString(), location,
            INITIAL_SEQUENCE_NUMBER, System.currentTimeMillis(),
            -1, schema, INITIAL_SPEC_ID, ImmutableList.of(spec),
            SortOrder.unsorted().orderId(), ImmutableList.of(SortOrder.unsorted()),
            ImmutableMap.copyOf(properties), -1, ImmutableList.of(),
            ImmutableList.of(), ImmutableList.of());
  }
}
