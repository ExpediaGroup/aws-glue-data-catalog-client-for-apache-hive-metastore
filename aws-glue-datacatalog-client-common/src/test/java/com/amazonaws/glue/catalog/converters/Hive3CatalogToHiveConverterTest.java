package com.amazonaws.glue.catalog.converters;

import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class Hive3CatalogToHiveConverterTest {

  private static final String TEST_DB_NAME = "testDb";
  private static final String TEST_TBL_NAME = "testTbl";
  private final CatalogToHiveConverter catalogToHiveConverter = new Hive3CatalogToHiveConverter();

  @Test
  public void testDatabaseCatalogName() {
    Database catalogDb = TestObjects.getTestDatabase();
    org.apache.hadoop.hive.metastore.api.Database hiveDatabase = catalogToHiveConverter
        .convertDatabase(catalogDb);
    assertEquals(DEFAULT_CATALOG_NAME, hiveDatabase.getCatalogName());
  }

  @Test
  public void testTableCatalogName() {
    Table catalogTable = TestObjects.getTestTable();
    org.apache.hadoop.hive.metastore.api.Table hiveTable = catalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertEquals(DEFAULT_CATALOG_NAME, hiveTable.getCatName());
  }

  @Test
  public void testTableMetaCatalogName() {
    Table catalogTable = TestObjects.getTestTable();
    TableMeta tableMeta = catalogToHiveConverter.convertTableMeta(catalogTable, TEST_DB_NAME);
    assertEquals(DEFAULT_CATALOG_NAME, tableMeta.getCatName());
  }

  @Test
  public void testPartitionConversion() {
    Partition partition = TestObjects.getTestPartition(TEST_DB_NAME, TEST_TBL_NAME, ImmutableList.of("1"));
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = catalogToHiveConverter.convertPartition(partition);
    assertEquals(DEFAULT_CATALOG_NAME, hivePartition.getCatName());
  }

  @Test
  public void testFunctionConversion() {
    UserDefinedFunction catalogFunction = TestObjects.getCatalogTestFunction();
    org.apache.hadoop.hive.metastore.api.Function hiveFunction = catalogToHiveConverter.convertFunction(TEST_DB_NAME, catalogFunction);
    assertEquals(DEFAULT_CATALOG_NAME, hiveFunction.getCatName());
  }

  @Test
  public void testConvertIcebergTableWithStrippedStorageDescriptor() {
    // Validator's Iceberg bypass must be followed by a converter that tolerates the nulls it let through.
    Table catalogTable = TestObjects.getTestTable();
    catalogTable.getParameters().put("table_type", "ICEBERG");
    StorageDescriptor sd = catalogTable.getStorageDescriptor();
    sd.setInputFormat(null);
    sd.setOutputFormat(null);
    sd.setSerdeInfo(null);
    sd.setCompressed(null);
    sd.setNumberOfBuckets(null);
    sd.setStoredAsSubDirectories(null);

    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        catalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);

    assertNotNull(hiveTable.getSd());
    assertNotNull(hiveTable.getSd().getSerdeInfo());
    assertEquals(DEFAULT_CATALOG_NAME, hiveTable.getCatName());
  }

  @Test
  public void testConvertIcebergTableMissingSerdeInfoAndPartitionKeys() {
    // Genericized real-world table shape: no InputFormat/OutputFormat/SerdeInfo, no PartitionKeys.
    List<Column> columns = new ArrayList<>();
    columns.add(new Column().withName("id").withType("string").withComment("record identifier"));
    columns.add(new Column().withName("event_time").withType("timestamp").withComment("event timestamp"));

    StorageDescriptor sd = new StorageDescriptor()
        .withColumns(columns)
        .withLocation("s3://example-bucket/some_bronze_table")
        .withCompressed(false)
        .withNumberOfBuckets(0)
        .withSortColumns(new ArrayList<>())
        .withStoredAsSubDirectories(false);
    // InputFormat, OutputFormat, and SerdeInfo are deliberately left unset (null),
    // matching the real Glue GetTable response for this table.

    Table catalogTable = new Table()
        .withName("some_bronze_table")
        .withDatabaseName("some_database")
        .withRetention(0)
        .withStorageDescriptor(sd)
        .withTableType("EXTERNAL_TABLE")
        .withParameters(ImmutableMap.of("table_type", "ICEBERG"));
    // No PartitionKeys set -- absent from the real Glue response for this table.

    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        catalogToHiveConverter.convertTable(catalogTable, "some_database");

    assertEquals("some_bronze_table", hiveTable.getTableName());
    assertEquals(DEFAULT_CATALOG_NAME, hiveTable.getCatName());
    assertEquals("ICEBERG", hiveTable.getParameters().get("table_type"));

    assertNotNull(hiveTable.getPartitionKeys());
    assertTrue(hiveTable.getPartitionKeys().isEmpty());

    assertNotNull(hiveTable.getSd());
    List<FieldSchema> hiveCols = hiveTable.getSd().getCols();
    assertEquals(2, hiveCols.size());
    assertEquals("id", hiveCols.get(0).getName());

    assertEquals(false, hiveTable.getSd().isCompressed());
    assertEquals(0, hiveTable.getSd().getNumBuckets());
    assertEquals(false, hiveTable.getSd().isStoredAsSubDirectories());

    assertNotNull(hiveTable.getSd().getSerdeInfo());
    assertNotNull(hiveTable.getSd().getSerdeInfo().getParameters());
    assertTrue(hiveTable.getSd().getSerdeInfo().getParameters().isEmpty());
  }
}
