package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_AVRO_SCHEMA_FROM_URL_ENABLE;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Glue's stored columns for an Avro table can be a stale copy of its avro.schema.url/literal schema. Hive's metastore
 * answers get_fields/get_schema from the Avro schema, and readers such as Trino rely on that for Avro tables.
 */
public class AvroSchemaFieldsTest {

  private static final String AVRO_SCHEMA = "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"long\"},"
      + "{\"name\":\"Name\",\"type\":[\"null\",\"string\"],\"doc\":\"display name\"},"
      + "{\"name\":\"added_later\",\"type\":\"int\"}]}";

  private Configuration conf;
  private AWSGlue glueClient;
  private AWSCatalogMetastoreClient metastoreClient;

  @Before
  public void setUp() throws Exception {
    Warehouse wh = mock(Warehouse.class);
    Path defaultWhPath = new Path("/tmp");
    when(wh.getDnsPath(defaultWhPath)).thenReturn(defaultWhPath);
    when(wh.isDir(defaultWhPath)).thenReturn(true);
    when(wh.mkdirs(defaultWhPath)).thenReturn(true);
    when(wh.getDefaultDatabasePath(any(String.class))).thenReturn(defaultWhPath);

    conf = spy(MetastoreConf.newMetastoreConf());
    conf.setBoolean(AWS_GLUE_AVRO_SCHEMA_FROM_URL_ENABLE, true);
    glueClient = spy(AWSGlue.class);
    GlueClientFactory clientFactory = mock(GlueClientFactory.class);
    AWSGlueMetastoreFactory metastoreFactory = mock(AWSGlueMetastoreFactory.class);
    when(clientFactory.newClient()).thenReturn(glueClient);
    DefaultAWSGlueMetastore defaultAWSGlueMetastore = new DefaultAWSGlueMetastore(conf, glueClient);
    when(metastoreFactory.newMetastore(conf)).thenReturn(defaultAWSGlueMetastore);

    metastoreClient = new AWSCatalogMetastoreClient.Builder()
        .withClientFactory(clientFactory)
        .withMetastoreFactory(metastoreFactory)
        .withWarehouse(wh)
        .createDefaults(false)
        .withConf(conf)
        .build();
  }

  @Test
  public void avroTableUsesSchemaLiteralNotStaleStoredColumns() throws Exception {
    Table table = avroTable(ImmutableMap.of("avro.schema.literal", AVRO_SCHEMA));

    List<FieldSchema> fields = metastoreClient.getFields(table.getDatabaseName(), table.getName());
    assertEquals(Lists.newArrayList(
        new FieldSchema("id", "bigint", ""),
        new FieldSchema("name", "string", "display name"),
        new FieldSchema("added_later", "int", "")), fields);

    List<FieldSchema> schema = metastoreClient.getSchema(table.getDatabaseName(), table.getName());
    assertEquals(fields.size() + table.getPartitionKeys().size(), schema.size());
    assertEquals(table.getPartitionKeys().get(0).getName(), schema.get(fields.size()).getName());
  }

  @Test
  public void flagOffUsesStoredColumns() throws Exception {
    conf.setBoolean(AWS_GLUE_AVRO_SCHEMA_FROM_URL_ENABLE, false);
    Table table = avroTable(ImmutableMap.of("avro.schema.literal", AVRO_SCHEMA));

    List<FieldSchema> fields = metastoreClient.getFields(table.getDatabaseName(), table.getName());
    assertEquals(1, fields.size());
    assertEquals("stale", fields.get(0).getName());
  }

  @Test
  public void schemaNoneIsTreatedAsUnset() throws Exception {
    // a "none" literal is skipped and the url is used, as in Hive
    Table table = avroTable(ImmutableMap.of("avro.schema.literal", "none", "avro.schema.url", schemaFile()));
    assertEquals(3, metastoreClient.getFields(table.getDatabaseName(), table.getName()).size());

    // only a "none" literal: the stored columns as-is, not rebuilt through an Avro schema (which lower-cases names)
    table = avroTable(ImmutableMap.of("avro.schema.literal", "none"));
    table.getStorageDescriptor().setColumns(Lists.newArrayList(new Column().withName("Stale").withType("string")));
    List<FieldSchema> fields = metastoreClient.getFields(table.getDatabaseName(), table.getName());
    assertEquals(1, fields.size());
    assertEquals("Stale", fields.get(0).getName());
  }

  @Test
  public void unreadableSchemaUrlFallsBackToStoredColumns() throws Exception {
    Table table = avroTable(ImmutableMap.of("avro.schema.url", "file:///does/not/exist.avsc"));

    List<FieldSchema> fields = metastoreClient.getFields(table.getDatabaseName(), table.getName());
    assertEquals(1, fields.size());
    assertEquals("stale", fields.get(0).getName());
  }

  @Test
  public void nonAvroTableUsesStoredColumns() throws Exception {
    Table table = getTestTable("db");
    stubGetTable(table);

    List<FieldSchema> fields = metastoreClient.getFields(table.getDatabaseName(), table.getName());
    assertEquals(table.getStorageDescriptor().getColumns().size(), fields.size());
    assertEquals(table.getStorageDescriptor().getColumns().get(0).getName(), fields.get(0).getName());
  }

  private Table avroTable(Map<String, String> parameters) {
    Table table = getTestTable("db");
    table.setParameters(parameters);
    table.getStorageDescriptor().getSerdeInfo().setSerializationLibrary(AvroSerDe.class.getName());
    table.getStorageDescriptor().setColumns(Lists.newArrayList(new Column().withName("stale").withType("string")));
    stubGetTable(table);
    return table;
  }

  private static String schemaFile() throws Exception {
    File file = File.createTempFile("schema", ".avsc");
    file.deleteOnExit();
    Files.write(file.toPath(), AVRO_SCHEMA.getBytes(StandardCharsets.UTF_8));
    return file.toURI().toString();
  }

  private void stubGetTable(Table table) {
    when(glueClient.getTable(new GetTableRequest().withDatabaseName(table.getDatabaseName()).withName(table.getName())))
        .thenReturn(new GetTableResult().withTable(table));
  }
}
