/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.rules.TemporaryFolder;

public class TestHiveIcebergStorageHandlerWithHiveCatalogAndLinkedinMetadata extends TestHiveIcebergStorageHandlerWithHiveCatalog {

  private HiveCatalog _hiveCatalog;
  private TemporaryFolder _temporaryFolder;
  private final FileFormat _fileFormat = FileFormat.AVRO;

  @Override
  public TestTables testTables(Configuration conf, TemporaryFolder temp) {
    _hiveCatalog = HiveCatalogs.loadCatalog(conf);
    _temporaryFolder = temp;
    return super.testTables(conf, temp);
  }

  @Override
  protected Table createIcebergTable(String tableName, Schema schema, List<Record> records) throws IOException {
    // This code is derived from TestTables. There was no easy way to alter table location without changing
    // bunch of interfaces. With this code the same outcome is achieved.
    TableIdentifier tableIdentifier = TableIdentifier.parse("default." + tableName);
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    if (tableName.equals("customers")) {
      partitionSpec = PartitionSpec.builderFor(schema)
          .identity("customer_id")
          .build();
    } else if (tableName.equals("orders")) {
      partitionSpec = PartitionSpec.builderFor(schema)
          .identity("order_id")
          .build();
    }
    Table table = _hiveCatalog.createTable(
        tableIdentifier,
        schema,
        partitionSpec,
        getLocationWithoutURI(tableIdentifier),
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, _fileFormat.name()));

    if (!records.isEmpty()) {
      table
          .newAppend()
          .appendFile(TestHelper.writeFile(table, null, records, _fileFormat, _temporaryFolder.newFile()))
          .commit();
    }
    return table;
  }

  private String getLocationWithoutURI(TableIdentifier tableIdentifier) {
    try {
      String location =  DynMethods.builder("defaultWarehouseLocation")
          .hiddenImpl(HiveCatalog.class, TableIdentifier.class)
          .build()
          .invoke(_hiveCatalog, tableIdentifier);
      return new URI(location).getPath();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
