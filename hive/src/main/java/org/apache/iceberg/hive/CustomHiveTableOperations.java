package org.apache.iceberg.hive;

import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.thrift.TException;


/**
 * A {@link HiveTableOperations} that does not override any existing Hive metadata.
 *
 * The behaviour of this class differs from {@link HiveTableOperations} in the following ways:
 * 1. Does not modify serde information of existing Hive table, this means that if Iceberg schema is updated
 *    Hive schema will remain stale
 * 2. If the Hive table already exists, no error is thrown. Instead Iceberg metadata is added to the table
 *
 * This behaviour is useful if the Iceberg metadata is being generated/updated in response to Hive metadata being
 * updated.
 */
public class CustomHiveTableOperations extends HiveTableOperations {
  private final HiveClientPool metaClients;
  private final String database;
  private final String tableName;

  protected CustomHiveTableOperations(Configuration conf, HiveClientPool metaClients, String database,
      String table) {
    super(conf, metaClients, database, table);
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
        // if table type is not Iceberg, do not throw an error. Instead just continue, current metadata will still be
        // null so further operations should work correctly
      } else {
        metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
          String errMsg = String.format("%s.%s is missing %s property", database, tableName, METADATA_LOCATION_PROP);
          throw new IllegalArgumentException(errMsg);
        }
      }
    } catch (NoSuchObjectException e) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(String.format("No such table: %s.%s", database, tableName));
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

    boolean threw = true;
    Optional<Long> lockId = Optional.empty();
    try {
      lockId = Optional.of(acquireLock());
      // TODO add lock heart beating for cases where default lock timeout is too low.
      Table tbl;
      boolean tableExists = metaClients.run(client -> client.tableExists(database, tableName));
      if (tableExists) {
        tbl = metaClients.run(client -> client.getTable(database, tableName));
      } else {
        final long currentTimeMillis = System.currentTimeMillis();
        tbl = new Table(tableName,
            database,
            System.getProperty("user.name"),
            (int) currentTimeMillis / 1000,
            (int) currentTimeMillis / 1000,
            Integer.MAX_VALUE,
            storageDescriptor(metadata),
            Collections.emptyList(),
            new HashMap<>(),
            null,
            null,
            TableType.EXTERNAL_TABLE.toString());
        tbl.getParameters().put("EXTERNAL", "TRUE"); // using the external table type also requires this
      }

      final String metadataLocation = tbl.getParameters().get(METADATA_LOCATION_PROP);
      if (!Objects.equals(currentMetadataLocation(), metadataLocation)) {
        String errMsg = String.format("metadataLocation = %s is not same as table metadataLocation %s for %s.%s",
            currentMetadataLocation(), metadataLocation, database, tableName);
        throw new CommitFailedException(errMsg);
      }

      setParameters(newMetadataLocation, tbl);

      if (tableExists) {
        metaClients.run(client -> {
          client.alter_table(database, tableName, tbl);
          return null;
        });
      } else {
        metaClients.run(client -> {
          client.createTable(tbl);
          return null;
        });
      }
      threw = false;
    } catch (TException | UnknownHostException e) {
      if (e.getMessage().contains("Table/View 'HIVE_LOCKS' does not exist")) {
        throw new RuntimeException("Failed to acquire locks from metastore because 'HIVE_LOCKS' doesn't "
            + "exist, this probably happened when using embedded metastore or doesn't create a "
            + "transactional meta table. To fix this, use an alternative metastore", e);
      }

      throw new RuntimeException(String.format("Metastore operation failed for %s.%s", database, tableName), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);
    } finally {
      if (threw) {
        // if anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(newMetadataLocation);
      }
      unlock(lockId);
    }
  }

  private void setParameters(String newMetadataLocation, Table tbl) {
    Map<String, String> parameters = tbl.getParameters();

    if (parameters == null) {
      parameters = new HashMap<>();
    }

    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);

    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    tbl.setParameters(parameters);
  }

  private StorageDescriptor storageDescriptor(TableMetadata metadata) {

    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(columns(metadata.schema()));
    storageDescriptor.setLocation(metadata.location());
    storageDescriptor.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");
    storageDescriptor.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private List<FieldSchema> columns(Schema schema) {
    return schema.columns().stream()
        .map(col -> new FieldSchema(col.name(), HiveTypeConverter.convert(col.type()), ""))
        .collect(Collectors.toList());
  }

  private long acquireLock() throws UnknownHostException, TException, InterruptedException {
    final LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, database);
    lockComponent.setTablename(tableName);
    final LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent), System.getProperty("user.name"),
        InetAddress.getLocalHost().getHostName());
    LockResponse lockResponse = metaClients.run(client -> client.lock(lockRequest));
    LockState state = lockResponse.getState();
    long lockId = lockResponse.getLockid();
    //TODO add timeout
    while (state.equals(LockState.WAITING)) {
      lockResponse = metaClients.run(client -> client.checkLock(lockId));
      state = lockResponse.getState();
      Thread.sleep(50);
    }

    if (!state.equals(LockState.ACQUIRED)) {
      throw new CommitFailedException(
          String.format("Could not acquire the lock on %s.%s, " + "lock request ended in state %s", database, tableName,
              state));
    }
    return lockId;
  }

  private void unlock(Optional<Long> lockId) {
    if (lockId.isPresent()) {
      try {
        metaClients.run(client -> {
          client.unlock(lockId.get());
          return null;
        });
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to unlock %s.%s", database, tableName), e);
      }
    }
  }
}
