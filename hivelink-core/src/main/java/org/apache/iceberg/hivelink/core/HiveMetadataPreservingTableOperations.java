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

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link HiveTableOperations} that does not override any existing Hive metadata.
 * TODO: This extension should be removed once dual-publish of iceberg+hive is stopped.
 *
 * The behaviour of this class differs from {@link HiveTableOperations} in the following ways:
 * 1. Does not modify serde information of existing Hive table, this means that if Iceberg schema is updated
 *    Hive schema will remain stale
 * 2. If the Hive table already exists, no error is thrown. Instead Iceberg metadata is added to the table
 *
 * This behaviour is useful if the Iceberg metadata is being generated/updated in response to Hive metadata being
 * updated.
 */
public class HiveMetadataPreservingTableOperations extends HiveTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetadataPreservingTableOperations.class);

  private final HiveClientPool metaClients;
  private final String database;
  private final String tableName;
  private static final DynMethods.UnboundMethod ALTER_TABLE = DynMethods.builder("alter_table")
      .impl(HiveMetaStoreClient.class, "alter_table_with_environmentContext",
          String.class, String.class, Table.class, EnvironmentContext.class)
      .impl(HiveMetaStoreClient.class, "alter_table",
          String.class, String.class, Table.class, EnvironmentContext.class)
      .build();
  public static final String ORC_COLUMNS = "columns";
  public static final String ORC_COLUMNS_TYPES = "columns.types";

  protected HiveMetadataPreservingTableOperations(Configuration conf, HiveClientPool metaClients, FileIO fileIO,
      String catalogName, String database, String table) {
    super(conf, metaClients, fileIO, catalogName, database, table);
    this.metaClients = metaClients;
    this.database = database;
    this.tableName = table;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    try {
      final Table table = metaClients.run(client -> client.getTable(database, tableName));
      String tableType = table.getParameters().get(TABLE_TYPE_PROP);

      if (tableType == null || !tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE)) {
        // [LINKEDIN] If table type is not Iceberg, that means there is no Iceberg metadata for the table yet.
        // So do not throw an error, instead just continue, currentMetadata will continue to remain null
        // which is what doRefresh would do if the table did not exist and further operations should work correctly

        // throw new IllegalArgumentException(String.format("Type of %s.%s is %s, not %s",
        //     database, tableName,
        //    tableType /* actual type */, ICEBERG_TABLE_TYPE_VALUE /* expected type */));
      } else {
        metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
          String errMsg = String.format("%s.%s is missing %s property", database, tableName, METADATA_LOCATION_PROP);
          throw new IllegalArgumentException(errMsg);
        }

        if (!io().newInputFile(metadataLocation).exists()) {
          String errMsg = String.format("%s property for %s.%s points to a non-existent file %s",
              METADATA_LOCATION_PROP, database, tableName, metadataLocation);
          throw new IllegalArgumentException(errMsg);
        }
      }
    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("No such table: %s.%s", database, tableName);
      }

    } catch (TException e) {
      String errMsg = String.format("Failed to get table info from metastore %s.%s", database, tableName);
      throw new RuntimeException(errMsg, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    CommitStatus commitStatus = CommitStatus.FAILURE;
    Optional<Long> lockId = Optional.empty();
    try {
      lockId = Optional.of(acquireLock());
      // TODO add lock heart beating for cases where default lock timeout is too low.
      Table tbl;
      // [LINKEDIN] Instead of checking if base != null to check for table existence, we query metastore for existence
      // base can be null if not Iceberg metadata exists, but Hive table exists, so we want to get the current table
      // definition and not create a new definition
      boolean tableExists = metaClients.run(client -> client.tableExists(database, tableName));
      if (tableExists) {
        tbl = metaClients.run(client -> client.getTable(database, tableName));
        fixMismatchedSchema(tbl);
      } else {
        final long currentTimeMillis = System.currentTimeMillis();
        tbl = new Table(tableName,
            database,
            System.getProperty("user.name"),
            (int) currentTimeMillis / 1000,
            (int) currentTimeMillis / 1000,
            Integer.MAX_VALUE,
            storageDescriptor(metadata, false),
            Collections.emptyList(),
            new HashMap<>(),
            null,
            null,
            TableType.EXTERNAL_TABLE.toString());
        tbl.getParameters().put("EXTERNAL", "TRUE"); // using the external table type also requires this
      }

      // [LINKEDIN] Do not touch the Hive schema of the table, just modify Iceberg specific properties
      // tbl.setSd(storageDescriptor(metadata)); // set to pickup any schema changes
      final String metadataLocation = tbl.getParameters().get(METADATA_LOCATION_PROP);
      String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
      if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
        throw new CommitFailedException(
            "Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s",
            baseMetadataLocation, metadataLocation, database, tableName);
      }

      // [LINKEDIN] comply to the new signature of setting Hive table's properties by
      // setting newly added parameters as empty container.
      setHmsTableParameters(newMetadataLocation, tbl, ImmutableMap.of(),
          ImmutableSet.of(), false, ImmutableMap.of());

      try {
        persistTableVerbal(tbl, tableExists);
        commitStatus = CommitStatus.SUCCESS;
      } catch (Throwable persistFailure) {
        LOG.error("Cannot tell if commit to {}.{} succeeded, attempting to reconnect and check.",
            database, tableName, persistFailure);
        commitStatus = checkCommitStatus(newMetadataLocation, metadata);
        switch (commitStatus) {
          case SUCCESS:
            break;
          case FAILURE:
            throw persistFailure;
          case UNKNOWN:
            throw new CommitStateUnknownException(persistFailure);
        }
      }
    } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
      throw new AlreadyExistsException("Table already exists: %s.%s", database, tableName);

    } catch (TException | UnknownHostException e) {
      if (e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
        throw new RuntimeException("Failed to acquire locks from metastore because 'HIVE_LOCKS' doesn't " +
            "exist, this probably happened when using embedded metastore or doesn't create a " +
            "transactional meta table. To fix this, use an alternative metastore", e);
      }

      throw new RuntimeException(String.format("Metastore operation failed for %s.%s", database, tableName), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);

    } finally {
      cleanupMetadataAndUnlock(commitStatus, newMetadataLocation, lockId);
    }
  }

  /**
   * [LINKEDIN] Due to an issue that the table read in is sometimes corrupted and has incorrect columns, compare the
   * table columns to the avro.schema.literal property (if it exists) and fix the table columns if there is a mismatch
   * @return true if the schema was mismatched and fixed
   */
  static boolean fixMismatchedSchema(Table table) {
    String avroSchemaLiteral = getAvroSchemaLiteral(table);
    if (Strings.isNullOrEmpty(avroSchemaLiteral)) {
      return false;
    }
    Schema schema = new Schema.Parser().parse(avroSchemaLiteral);
    List<FieldSchema> hiveCols;
    try {
      hiveCols = getColsFromAvroSchema(schema);
    } catch (SerDeException e) {
      LOG.error("Failed to get get columns from avro schema when checking schema", e);
      return false;
    }

    boolean schemaMismatched;
    if (table.getSd().getCols().size() != hiveCols.size()) {
      schemaMismatched = true;
    } else {
      Map<String, String> hiveFieldMap = hiveCols.stream().collect(
          Collectors.toMap(field -> field.getName().toLowerCase(), field -> field.getType().toLowerCase()));
      Map<String, String> tableFieldMap = table.getSd().getCols().stream().collect(
          Collectors.toMap(field -> field.getName().toLowerCase(), field -> field.getType().toLowerCase()));
      schemaMismatched = !hiveFieldMap.equals(tableFieldMap);
    }

    if (schemaMismatched) {
      LOG.warn("Schema columns don't match avro.schema.literal, setting columns to avro.schema.literal. Schema " +
              "columns: {}, avro.schema.literal columns: {}",
          table.getSd().getCols().stream().map(Object::toString).collect(Collectors.joining(", ")),
          hiveCols.stream().map(Object::toString).collect(Collectors.joining(", ")));
      table.getSd().setCols(hiveCols);
      if (!Strings.isNullOrEmpty(table.getSd().getInputFormat()) && table.getSd().getInputFormat()
          .contains("OrcInputFormat")) {
        updateORCStorageDesc(hiveCols, table);
      }
    }

    return schemaMismatched;
  }

  private static List<FieldSchema> getColsFromAvroSchema(Schema schema)
      throws SerDeException {
    AvroObjectInspectorGenerator avroOI = new AvroObjectInspectorGenerator(schema);
    List<String> columnNames = avroOI.getColumnNames();
    List<TypeInfo> columnTypes = avroOI.getColumnTypes();
    if (columnNames.size() != columnTypes.size()) {
      throw new IllegalStateException();
    }

    return IntStream.range(0, columnNames.size())
        .mapToObj(i -> new FieldSchema(columnNames.get(i), columnTypes.get(i).getTypeName(), ""))
        .collect(Collectors.toList());
  }

  private static String getAvroSchemaLiteral(Table table) {
    String schemaStr = table.getParameters().get(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName());
    if (Strings.isNullOrEmpty(schemaStr)) {
      schemaStr = table.getSd().getSerdeInfo().getParameters()
          .get(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName());
    }
    return schemaStr;
  }

  private static void updateORCStorageDesc(List<FieldSchema> hiveCols, Table table) {
    String columnsString = hiveCols.stream().map(FieldSchema::getName).collect(Collectors.joining(","));
    String typesString = hiveCols.stream().map(FieldSchema::getType).collect(Collectors.joining(","));

    if (!table.getSd().isSetSerdeInfo()) {
      table.getSd().setSerdeInfo(new SerDeInfo());
    }
    if (!table.getSd().getSerdeInfo().isSetParameters()) {
      table.getSd().getSerdeInfo().setParameters(Maps.newHashMap());
    }

    Map<String, String> sdParams = table.getSd().getSerdeInfo().getParameters();
    sdParams.put(ORC_COLUMNS, columnsString);
    sdParams.put(ORC_COLUMNS_TYPES, typesString);
  }

  /**
   * [LINKEDIN] a log-enhanced persistTable as a refactoring inspired by
   * org.apache.iceberg.hive.HiveTableOperations#persistTable
   */
  void persistTableVerbal(Table tbl, boolean tableExists) throws TException, InterruptedException {
    if (tableExists) {
      metaClients.run(client -> {
        EnvironmentContext envContext = new EnvironmentContext(
            ImmutableMap.of(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE)
        );
        ALTER_TABLE.invoke(client, database, tableName, tbl, envContext);
        return null;
      });
    } else {
      metaClients.run(client -> {
        client.createTable(tbl);
        return null;
      });
    }
  }
}
