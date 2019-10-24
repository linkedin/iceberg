package org.apache.iceberg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;


/**
 * A {@link HiveCatalog} which uses {@link CustomHiveTableOperations} underneath.
 *
 * This catalog is especially useful if the Iceberg operations are being performed in response to Hive operations
 */
public class CustomHiveCatalog extends HiveCatalog {

  private final HiveClientPool clients;
  private final Configuration conf;

  public CustomHiveCatalog(Configuration conf) {
    super(conf);
    this.clients = new HiveClientPool(2, conf);
    this.conf = conf;
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new CustomHiveTableOperations(conf, clients, dbName, tableName);
  }
}
