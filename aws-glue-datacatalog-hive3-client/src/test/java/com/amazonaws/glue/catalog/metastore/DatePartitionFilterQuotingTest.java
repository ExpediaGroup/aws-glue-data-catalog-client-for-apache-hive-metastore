package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.Hive3CatalogToHiveConverter;
import com.amazonaws.glue.catalog.util.ExprBuilder;
import com.amazonaws.glue.catalog.util.ExpressionHelper;
import com.amazonaws.glue.shims.AwsGlueHiveShims;
import com.amazonaws.glue.shims.ShimsLoader;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Partition;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Date;
import java.util.List;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestPartition;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the fix for a production failure via Waggle Dance: Spark pruned partitions on a
 * {@code date} key and sent {@code event_date >= 2026-02-02 and event_date < 2026-08-04}, which
 * Glue rejected with {@code InvalidInputException: Invalid partition expression!} because the
 * literals were unquoted.
 *
 * <p>The {@code byte[]} path already quoted these via {@code QUOTED_TYPES}; the String path did
 * not. {@link ExpressionHelper#quoteDateAndTimestampLiterals(String)} closes that gap.
 */
public class DatePartitionFilterQuotingTest {

  /** The exact filter Waggle Dance logged for the failing call. */
  private static final String UNQUOTED_DATE_FILTER =
      "event_date >= 2026-02-02 and event_date < 2026-08-04";

  /** The form Glue accepts, verified against the real service. */
  private static final String QUOTED_DATE_FILTER =
      "event_date >= '2026-02-02' and event_date < '2026-08-04'";

  private AWSGlue glueClient;
  private AWSCatalogMetastoreClient metastoreClient;
  private final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();
  private final CatalogToHiveConverter catalogToHiveConverter = new Hive3CatalogToHiveConverter();

  private org.apache.hadoop.hive.metastore.api.Database testDB;
  private org.apache.hadoop.hive.metastore.api.Table testTable;
  private org.apache.hadoop.hive.metastore.api.Partition testPartition;

  @Before
  public void setUp() throws Exception {
    testDB = catalogToHiveConverter.convertDatabase(getTestDatabase());
    testTable = catalogToHiveConverter.convertTable(getTestTable(), testDB.getName());
    testPartition = catalogToHiveConverter.convertPartition(
        getTestPartition(testDB.getName(), testTable.getTableName(), Lists.newArrayList("val1")));

    Warehouse wh = mock(Warehouse.class);
    Path defaultWhPath = new Path("/tmp");
    when(wh.getDnsPath(defaultWhPath)).thenReturn(defaultWhPath);
    when(wh.isDir(defaultWhPath)).thenReturn(true);
    when(wh.mkdirs(defaultWhPath)).thenReturn(true);
    when(wh.getDefaultDatabasePath(any(String.class))).thenReturn(defaultWhPath);

    Configuration conf = spy(MetastoreConf.newMetastoreConf());
    conf.setInt(GlueMetastoreClientDelegate.NUM_PARTITION_SEGMENTS_CONF, 1);
    glueClient = spy(AWSGlue.class);

    GlueClientFactory clientFactory = mock(GlueClientFactory.class);
    AWSGlueMetastoreFactory metastoreFactory = mock(AWSGlueMetastoreFactory.class);
    when(clientFactory.newClient()).thenReturn(glueClient);
    // Must be constructed before the stubbing call: building it inline inside when(...) trips
    // Mockito's UnfinishedStubbingException.
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

  /** The bare literals Spark emits are quoted before the request reaches Glue. */
  @Test
  public void listPartitionsByFilterQuotesUnquotedDateLiterals() throws Exception {
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(new GetPartitionsResult().withPartitions(Lists.<Partition>newArrayList()));

    metastoreClient.listPartitionsByFilter(
        testDB.getName(), testTable.getTableName(), UNQUOTED_DATE_FILTER, (short) -1);

    ArgumentCaptor<GetPartitionsRequest> captor = ArgumentCaptor.forClass(GetPartitionsRequest.class);
    verify(glueClient).getPartitions(captor.capture());
    String sentToGlue = captor.getValue().getExpression();

    assertEquals(QUOTED_DATE_FILTER, sentToGlue);
    assertFalse(
        "no bare date literal should survive, got: " + sentToGlue,
        sentToGlue.contains(" 2026-02-02"));
  }

  /**
   * A rejected filter surfaces as {@code InvalidObjectException}, which is not declared on the
   * {@code get_partitions_by_filter} Thrift method — hence Waggle Dance clients only ever saw
   * {@code TApplicationException: Internal error processing get_partitions_by_filter}.
   */
  @Test
  public void filterRejectedByGlueSurfacesAsInvalidObjectException() throws Exception {
    InvalidInputException glueError = new InvalidInputException("Invalid partition expression!");
    glueError.setStatusCode(400);
    glueError.setErrorCode("InvalidInputException");
    when(glueClient.getPartitions(any(GetPartitionsRequest.class))).thenThrow(glueError);

    try {
      metastoreClient.listPartitionsByFilter(
          testDB.getName(), testTable.getTableName(), UNQUOTED_DATE_FILTER, (short) -1);
      fail("expected Glue to reject the expression");
    } catch (InvalidObjectException e) {
      assertTrue(
          "expected Glue's 'Invalid partition expression!' message, got: " + e.getMessage(),
          e.getMessage().contains("Invalid partition expression!"));
    }
  }

  /** An already-correct filter is left alone, so the rewrite is idempotent. */
  @Test
  public void quotedDateFilterIsForwardedUnchanged() throws Exception {
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(new GetPartitionsResult().withPartitions(Lists.<Partition>newArrayList()));

    metastoreClient.listPartitionsByFilter(
        testDB.getName(), testTable.getTableName(), QUOTED_DATE_FILTER, (short) -1);

    ArgumentCaptor<GetPartitionsRequest> captor = ArgumentCaptor.forClass(GetPartitionsRequest.class);
    verify(glueClient).getPartitions(captor.capture());

    assertEquals(QUOTED_DATE_FILTER, captor.getValue().getExpression());
  }

  /** Both partition-filter entry points now quote date literals. */
  @Test
  public void hiveExpressionPathAndStringFilterPathBothQuoteDateLiterals() throws Exception {
    ExprNodeGenericFuncDesc expr = new ExprBuilder("traveler_one_profiles_search_event_data")
        .val(Date.valueOf("2026-02-02"))
        .dateCol("event_date")
        .pred(">=", 2)
        .build();

    byte[] payload = hiveShims.getSerializeExpression(expr);
    String converted = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);

    assertTrue(
        "byte[] expr path is expected to quote date literals, got: " + converted,
        converted.contains("'"));

    // ...and the String-filter path now reaches the same result.
    assertEquals(
        QUOTED_DATE_FILTER,
        ExpressionHelper.quoteDateAndTimestampLiterals(UNQUOTED_DATE_FILTER));
  }

  /** The rewrite must not disturb filters that contain no date literals. */
  @Test
  public void nonDateFiltersAreUntouched() {
    String filter = "region = 'eu' and event_hour > 12 and count <= 2026";
    assertEquals(filter, ExpressionHelper.quoteDateAndTimestampLiterals(filter));
  }

  /** Date-like text inside a quoted string literal must not be re-quoted. */
  @Test
  public void dateLikeTextInsideQuotedStringIsUntouched() {
    String filter = "report_name = 'daily 2026-02-02 summary'";
    assertEquals(filter, ExpressionHelper.quoteDateAndTimestampLiterals(filter));
  }

  /** Timestamp literals and IN lists are handled too. */
  @Test
  public void timestampLiteralsAndInListsAreQuoted() {
    assertEquals(
        "ts >= '2026-02-02 10:15:30'",
        ExpressionHelper.quoteDateAndTimestampLiterals("ts >= 2026-02-02 10:15:30"));
    assertEquals(
        "event_date in ('2026-02-02', '2026-02-03')",
        ExpressionHelper.quoteDateAndTimestampLiterals("event_date in (2026-02-02, 2026-02-03)"));
  }

  /** Applying the rewrite twice must be a no-op. */
  @Test
  public void quotingIsIdempotent() {
    String once = ExpressionHelper.quoteDateAndTimestampLiterals(UNQUOTED_DATE_FILTER);
    assertEquals(once, ExpressionHelper.quoteDateAndTimestampLiterals(once));
  }
}
