package org.apache.iceberg.hive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;


public class FederatedIcebergMetaStoreClient implements IMetaStoreClient {
  private final HiveConf _conf;
  private final Catalog _icebergCatalog;
  private final IMetaStoreClient _client;
  private final boolean _allowEmbedded;

  public FederatedIcebergMetaStoreClient(HiveConf conf, HiveMetaHookLoader loader, Boolean allowEmbedded)
      throws MetaException {
    _conf = new HiveConf(conf);
    _client = new HiveMetaStoreClient(conf);
    _icebergCatalog = HiveCatalogs.loadCatalog(conf);
    _allowEmbedded = allowEmbedded;
  }

  //// Hive 1.x API -- start

  @Override
  public boolean isCompatibleWith(HiveConf conf) {
    return _client.isCompatibleWith(conf);
  }

  @Override
  public void setHiveAddedJars(String addedJars) {
    _client.setHiveAddedJars(addedJars);
  }

  @Override
  public void reconnect() throws MetaException {
    _client.reconnect();
  }

  @Override
  public void close() {
    _client.close();
  }

  @Override
  public void setMetaConf(String key, String value) throws TException {
    _client.setMetaConf(key, value);
  }

  @Override
  public String getMetaConf(String key) throws TException {
    return _client.getMetaConf(key);
  }

  @Override
  public List<String> getDatabases(String databasePattern) throws TException {
    return _client.getAllDatabases();
  }

  @Override
  public List<String> getAllDatabases() throws TException {
    return _client.getAllDatabases();
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern)
      throws TException {
    return _client.getTables(dbName, tablePattern);
  }

  @Override
  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws TException {
    return _client.getTableMeta(dbPatterns, tablePatterns, tableTypes);
  }

  @Override
  public List<String> getAllTables(String dbName) throws TException {
    return _client.getAllTables(dbName);
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws TException {
    return _client.listTableNamesByFilter(dbName, filter, maxTables);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab)
      throws TException {
    _client.dropTable(dbname, tableName, deleteData, ignoreUnknownTab);
  }

  @Override
  public void dropTable(
      String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge)
      throws TException {
    _client.dropTable(dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Override
  public void dropTable(String tableName, boolean deleteData)
      throws TException {
    _client.dropTable(tableName, deleteData);
  }

  @Override
  public void dropTable(String dbname, String tableName) throws TException {
    verifyTableIsNotIcebergBacked(dbname, tableName);
    _client.dropTable(dbname, tableName);
  }

  @Override
  public boolean tableExists(String databaseName, String tableName)
      throws TException {
    return _client.tableExists(databaseName, tableName);
  }

  @Override
  public boolean tableExists(String tableName) throws TException {
    return _client.tableExists(tableName);
  }

  @Override
  public Table getTable(String tableName) throws TException {
    String[] names = tableName.split("\\.");
    return getTable(names[0], names[1]);
  }

  @Override
  public Database getDatabase(String databaseName) throws TException {
    return _client.getDatabase(databaseName);
  }

  @Override
  public Table getTable(String dbName, String tableName) throws TException {
    Table underlyingHiveTable = _client.getTable(dbName, tableName);
    return getTable(underlyingHiveTable);
  }

  private Table getTable(Table underlyingHiveTable) throws TException {
    String dbName = underlyingHiveTable.getDbName();
    String tableName = underlyingHiveTable.getTableName();
    if (!isIcebergBacked(underlyingHiveTable)) {
      return underlyingHiveTable;
    } else {
      Table hiveTable = createEmptyTable(dbName, tableName);
      org.apache.iceberg.Table icebergTable = _icebergCatalog.loadTable(TableIdentifier.of(dbName, tableName));
      hiveTable.getSd().setLocation(icebergTable.location());
      hiveTable.getSd().setCols(fieldSchemas(icebergTable.schema()));if (identityPartitioned(icebergTable)) {
        // If the Iceberg table is identity partitioned, we show the Hive table as partitioned,
        setPartitionSchema(hiveTable, icebergTable);
      }
      return hiveTable;
    }
  }

  private boolean identityPartitioned(org.apache.iceberg.Table icebergTable) {
    List<PartitionField> fields = icebergTable.spec().fields();
    return fields.size() > 0 && fields.stream().allMatch(p -> p.transform().isIdentity());
  }

  private void setPartitionSchema(Table hiveTable, org.apache.iceberg.Table icebergTable) {
    Schema partitionSchema = new Schema(icebergTable.spec().partitionType().fields());
    hiveTable.setPartitionKeys(fieldSchemas(partitionSchema));
  }

  private List<FieldSchema> fieldSchemas(Schema icebergTableSchema) {
    List<String> columnNames = columnNames(icebergTableSchema);
    List<TypeInfo> columnTypes = IcebergSchemaToTypeInfo.getColumnTypes(icebergTableSchema);
    List<FieldSchema> fieldSchemas = new ArrayList<>();
    for (int i = 0; i < columnNames.size(); i++) {
      // TODO set comments from Iceberg schema
      fieldSchemas.add(new FieldSchema(columnNames.get(i), columnTypes.get(i).getTypeName(), null));
    }
    return fieldSchemas;
  }

  private static List<String> columnNames(org.apache.iceberg.Schema schema) {
    List<Types.NestedField> fields = schema.columns();
    List<String> fieldsList = new ArrayList<>(fields.size());
    for (Types.NestedField field : fields) {
      fieldsList.add(field.name());
    }
    return fieldsList;
  }

  private static Table createEmptyTable(String db, String t) {
    final Table table = new org.apache.hadoop.hive.metastore.api.Table();
    table.setDbName(db);
    table.setTableName(t);
    table.setPartitionKeys(new ArrayList<>());
    table.setParameters(new HashMap<>());

    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo());
    sd.setNumBuckets(-1);
    sd.setBucketCols(new ArrayList<>());
    sd.setCols(new ArrayList<>());
    sd.setParameters(new HashMap<>());
    sd.setSortCols(new ArrayList<>());
    sd.getSerdeInfo().setParameters(new HashMap<>());
    SkewedInfo skewInfo = new SkewedInfo();
    skewInfo.setSkewedColNames(new ArrayList<>());
    skewInfo.setSkewedColValues(new ArrayList<>());
    skewInfo.setSkewedColValueLocationMaps(new HashMap<>());
    sd.setSkewedInfo(skewInfo);
    table.setSd(sd);

    table.setTableType(EXTERNAL_TABLE.name());
    table.getParameters().put("EXTERNAL", "TRUE");
    return table;
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws TException {
    List<Table> underlyingTables = _client.getTableObjectsByName(dbName, tableNames);
    List<Table> tables = Lists.newArrayListWithExpectedSize(tableNames.size());
    for (Table table : underlyingTables) {
      tables.add(table);
    }
    return tables;
  }

  @Override
  public Partition appendPartition(String tableName, String dbName, List<String> partVals)
      throws TException {
    return _client.appendPartition(tableName, dbName, partVals);
  }

  @Override
  public Partition appendPartition(String tableName, String dbName, String name)
      throws TException {
    return _client.appendPartition(tableName, dbName, name);
  }

  @Override
  public Partition add_partition(Partition partition)
      throws TException {
    return _client.add_partition(partition);
  }

  @Override
  public int add_partitions(List<Partition> partitions)
      throws TException {
    return _client.add_partitions(partitions);
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpec)
      throws TException {
    return _client.add_partitions_pspec(partitionSpec);
  }

  @Override
  public List<Partition> add_partitions(List<Partition> partitions, boolean ifNotExists, boolean needResults)
      throws TException {
    return _client.add_partitions(partitions, ifNotExists, needResults);
  }

  @Override
  public Partition getPartition(String tblName, String dbName, List<String> partVals)
      throws TException {
    // Used by add partition DDL
    return _client.getPartition(tblName, dbName, partVals);
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceDb, String sourceTable,
      String destdb, String destTableName)
      throws TException {
    return _client.exchange_partition(partitionSpecs, sourceDb, sourceTable, destdb, destTableName);
  }

  @Override
  public Partition getPartition(String dbName, String tblName, String name)
      throws TException {
    // API not used
    return _client.getPartition(dbName, tblName, name);
  }

  private static boolean isIcebergBacked(Table table) {
    // TODO we should ideally check if the Table is backed by a Iceberg StorageHandler
    Map<String, String> parameters = table.getParameters();
    return HiveTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
        parameters.get(HiveTableOperations.TABLE_TYPE_PROP));
  }

  @Override
  public Partition getPartitionWithAuthInfo(String dbName, String tableName, List<String> pvals, String userName,
      List<String> groupNames) throws TException {
    return null;
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws TException {
    return null;
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
    throw new UnsupportedOperationException("HCatalog API not supported");
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name, List<String> part_vals, short max_parts)
      throws TException {
    return null;
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts) {
    List<String> partitionNames = Lists.newArrayList();
    org.apache.iceberg.Table table =
        _icebergCatalog.loadTable(TableIdentifier.of(db_name, tbl_name));
    org.apache.iceberg.Table partitionsTable =
        _icebergCatalog.loadTable(TableIdentifier.of(db_name, tbl_name, MetadataTableType.PARTITIONS.name()));
    try(CloseableIterable<FileScanTask> tasks = partitionsTable.newScan().planFiles()) {
      int numParts = 0;
      for (FileScanTask fileScanTask : tasks) {
        DataTask task = (DataTask) fileScanTask;
        try (CloseableIterable<StructLike> rows = task.rows()) {
          for (StructLike row: rows) {
            if (max_parts == -1 || numParts < max_parts) {
              partitionNames.add(convertToHivePartitionName(table.spec(), row));
              numParts += 1;
            } else {
              return partitionNames;
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return partitionNames;
  }

  private String convertToHivePartitionName(PartitionSpec spec, StructLike row) {
    StringBuilder partitionName = new StringBuilder();
    StructLike partitionData = row.get(0, StructLike.class);
    List<PartitionField> partitionFields = spec.fields();
    for (int pos = 0; pos < partitionFields.size(); pos++) {
      partitionName
          // identity partitioning name matches source column name
          .append(partitionFields.get(pos).name().toLowerCase())
          .append("=")
          .append(partitionData.get(pos, Object.class))
          .append("/");
    }
    return partitionName.substring(0, partitionName.length() - 1);
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, List<String> part_vals, short max_parts)
      throws TException {
    return null;
  }

  @Override
  public List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter, short max_parts)
      throws TException {
    return null;
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter, int max_parts)
      throws TException {
    throw new UnsupportedOperationException("HCatalog api not supported");
  }

  @Override
  public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr, String default_partition_name,
      short max_parts, List<Partition> result) throws TException {
    return false;
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, short s, String userName,
      List<String> groupNames) throws TException {
    return null;
  }

  @Override
  public List<Partition> getPartitionsByNames(String db_name, String tbl_name, List<String> part_names)
      throws TException {
    return null;
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, List<String> partialPvals, short s,
      String userName, List<String> groupNames) throws TException {
    return null;
  }

  @Override
  public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType)
      throws TException {
    throw new UnsupportedOperationException("HCatalog Api not supported");
  }

  @Override
  public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType)
      throws TException {
    throw new UnsupportedOperationException("HCatalog Api not supported");
  }

  @Override
  public void validatePartitionNameCharacters(List<String> partVals) throws TException {
    // Used by write API
    _client.validatePartitionNameCharacters(partVals);
  }

  @Override
  public void createTable(Table tbl)
      throws TException {
    _client.createTable(tbl);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table)
      throws TException {
    _client.alter_table(defaultDatabaseName, tblName, table);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table, boolean cascade)
      throws TException {
    _client.alter_table(defaultDatabaseName, tblName, table);
  }

  @Override
  public void createDatabase(Database db)
      throws TException {
    _client.createDatabase(db);
  }

  @Override
  public void dropDatabase(String name)
      throws TException {
    _client.dropDatabase(name);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws TException {
    _client.dropDatabase(name, deleteData, ignoreUnknownDb);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws TException {
    _client.dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public void alterDatabase(String name, Database db) throws TException {
    _client.alterDatabase(name, db);
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
      throws TException {
    return false;
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, PartitionDropOptions options)
      throws TException {
    return false;
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs,
      boolean deleteData, boolean ignoreProtection, boolean ifExists)
      throws TException {
    return null;
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs,
      PartitionDropOptions options) throws TException {
    return null;
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, String name, boolean deleteData)
      throws TException {
    return false;
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws TException {

  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
      throws TException {

  }

  @Override
  public void renamePartition(String dbname, String name, List<String> part_vals, Partition newPart)
      throws TException {
  }

  @Override
  public List<FieldSchema> getFields(String db, String tableName)
      throws TException {
    // Not used
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public List<FieldSchema> getSchema(String db, String tableName)
      throws TException {
    // Not used
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public String getConfigValue(String name, String defaultValue) throws TException {
    return null;
  }

  @Override
  public List<String> partitionNameToVals(String name) throws TException {
    return null;
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws TException {
    return null;
  }

  @Override
  public void createIndex(Index index, Table indexTable)
      throws TException {

  }

  @Override
  public void alter_index(String dbName, String tblName, String indexName, Index index)
      throws TException {

  }

  @Override
  public Index getIndex(String dbName, String tblName, String indexName)
      throws TException {
    return null;
  }

  @Override
  public List<Index> listIndexes(String db_name, String tbl_name, short max)
      throws TException {
    return null;
  }

  @Override
  public List<String> listIndexNames(String db_name, String tbl_name, short max) throws TException {
    return null;
  }

  @Override
  public boolean dropIndex(String db_name, String tbl_name, String name, boolean deleteData)
      throws TException {
    return false;
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
      throws TException {
    return false;
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj)
      throws TException {
    return false;
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames)
      throws TException {
    return null;
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName,
      List<String> partNames, List<String> colNames) throws TException {
    return null;
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName, String colName)
      throws TException {
    return false;
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
      throws TException {
    return false;
  }

  @Override
  public boolean create_role(Role role) throws TException {
    return false;
  }

  @Override
  public boolean drop_role(String role_name) throws TException {
    return false;
  }

  @Override
  public List<String> listRoleNames() throws TException {
    return _client.listRoleNames();
  }

  @Override
  public boolean grant_role(String role_name, String user_name, PrincipalType principalType, String grantor,
      PrincipalType grantorType, boolean grantOption) throws TException {
    return false;
  }

  @Override
  public boolean revoke_role(String role_name, String user_name, PrincipalType principalType, boolean grantOption)
      throws TException {
    return false;
  }

  @Override
  public List<Role> list_roles(String principalName, PrincipalType principalType) throws TException {
    return null;
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name, List<String> group_names)
      throws TException {
    return null;
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String principal_name, PrincipalType principal_type,
      HiveObjectRef hiveObject) throws TException {
    return null;
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws TException {
    return false;
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws TException {
    return false;
  }

  @Override
  public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws TException {
    return null;
  }

  @Override
  public long renewDelegationToken(String tokenStrForm) throws TException {
    return _client.renewDelegationToken(tokenStrForm);
  }

  @Override
  public void cancelDelegationToken(String tokenStrForm) throws TException {
    _client.cancelDelegationToken(tokenStrForm);
  }

  @Override
  public void createFunction(Function func) throws TException {

  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws TException {

  }

  @Override
  public void dropFunction(String dbName, String funcName)
      throws TException {

  }

  @Override
  public Function getFunction(String dbName, String funcName) throws TException {
    return _client.getFunction(dbName, funcName);
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws TException {
    return _client.getFunctions(dbName, pattern);
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() throws TException {
    return _client.getAllFunctions();
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    return _client.getValidTxns();
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    return _client.getValidTxns(currentTxn);
  }

  @Override
  public long openTxn(String user) throws TException {
    // We do not support transactions at LinkedIn, but there are 4 flows setting a transaction manager
    // For now we, ignore transactions for Iceberg backed tables
    return _client.openTxn(user);
  }

  @Override
  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    return _client.openTxns(user, numTxns);
  }

  @Override
  public void rollbackTxn(long txnid) throws TException {
    _client.rollbackTxn(txnid);
  }

  @Override
  public void commitTxn(long txnid) throws TException {
    _client.commitTxn(txnid);
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return _client.showTxns();
  }

  @Override
  public LockResponse lock(LockRequest request) throws TException {
    return _client.lock(request);
  }

  @Override
  public LockResponse checkLock(long lockid)
      throws TException {
    return _client.checkLock(lockid);
  }

  @Override
  public void unlock(long lockid) throws TException {
    _client.unlock(lockid);
  }

  @Override
  public ShowLocksResponse showLocks() throws TException {
    return _client.showLocks();
  }

  @Override
  public void heartbeat(long txnid, long lockid)
      throws TException {
    _client.heartbeat(txnid, lockid);
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
    return _client.heartbeatTxnRange(min, max);
  }

  @Override
  public void compact(String dbname, String tableName, String partitionName, CompactionType type) throws TException {
    _client.compact(dbname, tableName, partitionName, type);
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    return _client.showCompactions();
  }

  @Override
  public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, NotificationFilter filter)
      throws TException {
    return _client.getNextNotification(lastEventId, maxEvents, filter);
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return _client.getCurrentNotificationEventId();
  }

  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest request) throws TException {
    return _client.fireListenerEvent(request);
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincRoleReq)
      throws TException {
    return _client.get_principals_in_role(getPrincRoleReq);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRolePrincReq) throws TException {
    // roles are not supported
    return _client.get_role_grants_for_principal(getRolePrincReq);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName)
      throws TException {
    return _client.getAggrColStatsFor(dbName, tblName, colNames, partName);
  }

  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
      throws TException {
    return _client.setPartitionColumnStatistics(request);
  }

  //// Hive 1.x API -- end

  ///// Hive 2.x API -- start
  @Override
  public boolean isLocalMetaStore() {
    return _allowEmbedded;
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern, TableType tableType)
      throws TException {
    return _client.getTables(dbName, tablePattern, tableType);
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceDb, String sourceTable,
      String destdb, String destTableName)
      throws TException {
    return _client.exchange_partitions(partitionSpecs, sourceDb, sourceTable, destdb, destTableName);
  }

  @Override
  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
      throws TException {
    return null;
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tableName, String filter)
      throws TException {
    // this api does not seem to be used
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void alter_table_with_environmentContext(String defaultDatabaseName, String tblName, Table table,
      EnvironmentContext environmentContext) throws TException {

  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs,
      boolean deleteData, boolean ifExists) throws TException {
    return null;
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext)
      throws TException {

  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext) throws TException {

  }

  @Override
  public String getTokenStrForm() throws IOException {
    return null;
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
    return false;
  }

  @Override
  public boolean removeToken(String tokenIdentifier) throws TException {
    return false;
  }

  @Override
  public String getToken(String tokenIdentifier) throws TException {
    return null;
  }

  @Override
  public List<String> getAllTokenIdentifiers() throws TException {
    return null;
  }

  @Override
  public int addMasterKey(String key) throws TException {
    return 0;
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key) throws TException {

  }

  @Override
  public boolean removeMasterKey(Integer keySeq) throws TException {
    return false;
  }

  @Override
  public String[] getMasterKeys() throws TException {
    return new String[0];
  }

  @Override
  public void abortTxns(List<Long> txnids) throws TException {

  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    return null;
  }

  @Override
  public void compact(String dbname, String tableName, String partitionName, CompactionType type,
      Map<String, String> tblproperties) throws TException {

  }

  @Override
  public CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type,
      Map<String, String> tblproperties) throws TException {
    return null;
  }

  @Override
  public void addDynamicPartitions(long txnId, String dbName, String tableName, List<String> partNames)
      throws TException {

  }

  @Override
  public void addDynamicPartitions(long txnId, String dbName, String tableName, List<String> partNames,
      DataOperationType operationType) throws TException {

  }

  @Override
  public void insertTable(Table table, boolean overwrite) throws MetaException {

  }

  @Override
  public void flushCache() {

  }

  @Override
  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
    return null;
  }

  @Override
  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(List<Long> fileIds, ByteBuffer sarg,
      boolean doGetFooters) throws TException {
    return null;
  }

  @Override
  public void clearFileMetadata(List<Long> fileIds) throws TException {

  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {

  }

  @Override
  public boolean isSameConfObj(HiveConf c) {
    return false;
  }

  @Override
  public boolean cacheFileMetadata(String dbName, String tableName, String partName, boolean allParts)
      throws TException {
    return false;
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request)
      throws TException {
    return null;
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request)
      throws TException {
    return null;
  }

  @Override
  public void createTableWithConstraints(Table tTbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
      throws TException {

  }

  @Override
  public void dropConstraint(String dbName, String tableName, String constraintName)
      throws TException {

  }

  @Override
  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
      throws TException {

  }

  @Override
  public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
      throws TException {
  }

  private Table verifyTableIsNotIcebergBacked(String db, String table) throws TException {
    Table hiveTable = _client.getTable(db, table);
    if (isIcebergBacked(hiveTable)) {
      throw new UnsupportedOperationException(String.format("Write api is not supported for table %s.%s", db, table));
    }
    return hiveTable;
  }

  ///// Hive 2.x API -- end
}
