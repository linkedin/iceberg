package org.apache.iceberg.hive;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.util.HashMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(StandaloneHiveRunner.class)
public class TestFederatedIcebergMetaStoreClient {
  @HiveSQL(files = {}, autoStart = false)
  private HiveShell shell;

  private static TestHiveMetastore metastore;
  private static HiveCatalog catalog;
  private static HiveMetaStoreClient metastoreClient;

  @BeforeClass
  public static void setup() throws Exception {
    metastore = new TestHiveMetastore();
    metastore.start();

    HiveConf conf = metastore.hiveConf();
    // create test db
    metastoreClient = new HiveMetaStoreClient(conf);
    String dbPath = metastore.getDatabasePath("db");
    Database db = new Database("db", "description", dbPath, new HashMap<>());
    metastoreClient.createDatabase(db);
    catalog = HiveCatalogs.loadCatalog(conf);
  }

  @Test
  public void testHiveMetadataReads() {
    // setup HiveRunner
    shell.setHiveConfValue(
        HiveConf.ConfVars.METASTOREURIS.varname,
        metastore.hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS));
    shell.setHiveConfValue("metastore.client.impl", FederatedIcebergMetaStoreClient.class.getName());
    shell.start();

    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get()),
        Types.NestedField.optional(2, "date", Types.StringType.get())

    );
    PartitionSpec spec = PartitionSpec.builderFor(schema)
                                      .identity("data")
                                      .identity("date")
                                      .build();
    Table table = catalog.createTable(TableIdentifier.of("db", "t"), schema, spec);
    // Verify table read
    Assert.assertEquals(Lists.newArrayList("t"), Lists.newArrayList(shell.executeQuery("show tables in db")));
    for (String s : shell.executeQuery("describe formatted db.t")) {
      System.out.println(s);
    }

    table.newAppend()
         .appendFile(
             DataFiles.builder(spec)
                      .withPath("path/to/file.avro")
                      .withPartitionPath("data=ds/date=2020-06-25")
                      .withRecordCount(1L)
                      .withFileSizeInBytes(1L)
                      .build())
         .commit();
    Assert.assertEquals("data=ds/date=2020-06-25", shell.executeQuery("show partitions db.t").get(0));
  }
}

