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
 * Reproduces the production failure seen through Waggle Dance when Spark prunes partitions on a
 * {@code date}-typed partition key.
 *
 * <p>A Spark 3.2 driver issued the Thrift call:
 *
 * <pre>
 * get_partitions_by_filter(
 *     egdp_prod_datascience,
 *     traveler_one_profiles_search_event_data,
 *     "event_date &gt;= 2026-02-02 and event_date &lt; 2026-08-04",
 *     -1)
 * </pre>
 *
 * <p>and Glue rejected it:
 *
 * <pre>
 * InvalidObjectException: Invalid partition expression!
 *   (Service: AWSGlue; Status Code: 400; Error Code: InvalidInputException)
 *   at GlueMetastoreClientDelegate.getCatalogPartitions
 *   at AWSCatalogMetastoreClient.listPartitionsByFilter
 * </pre>
 *
 * <p>The date literals are unquoted. Glue's expression parser requires {@code '2026-02-02'} for a
 * {@code date} partition key; unquoted it is parsed as an arithmetic expression and rejected.
 *
 * <p>The root cause is an asymmetry between the two partition-filter entry points:
 *
 * <ul>
 *   <li>the {@code byte[]} expression path ({@code listPartitionsByExpr}) runs the filter through
 *       {@link ExpressionHelper#convertHiveExpressionToCatalogExpression(byte[])}, which quotes
 *       {@code date}/{@code timestamp} literals via its {@code QUOTED_TYPES} list;</li>
 *   <li>the {@code String} filter path ({@code listPartitionsByFilter}) applies only
 *       {@link ExpressionHelper#replaceDoubleQuoteWithSingleQuotes(String)} and forwards the caller's
 *       string to Glue verbatim — no date quoting is ever applied.</li>
 * </ul>
 *
 * <p>These tests pin the current (broken) behaviour so a fix has a failing baseline to flip.
 */
public class DatePartitionFilterQuotingTest {

  /** The exact filter string Waggle Dance logged for the failing production call. */
  private static final String UNQUOTED_DATE_FILTER =
      "event_date >= 2026-02-02 and event_date < 2026-08-04";

  /** The same filter in the form Glue accepts, verified against the real service. */
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

  /**
   * The bare literals Spark emits are quoted before the request leaves the client, so Glue receives
   * an expression it can parse.
   */
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
   * Documents the exception chain the customer saw, and why it was so opaque. If Glue rejects an
   * expression, the client converts {@code InvalidInputException} to {@code InvalidObjectException},
   * which is <em>not</em> declared on the {@code get_partitions_by_filter} Thrift method — which is
   * why Waggle Dance clients only ever saw
   * {@code TApplicationException: Internal error processing get_partitions_by_filter} and never the
   * real cause.
   *
   * <p>The quoting fix means a bare date literal no longer reaches Glue, but this remains the
   * behaviour for any other malformed expression.
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

  /**
   * The two partition-filter entry points now agree: the {@code byte[]} expression path quotes date
   * literals via {@code ExpressionHelper}'s {@code QUOTED_TYPES}, and the String-filter path reaches
   * the same result through {@link ExpressionHelper#quoteDateAndTimestampLiterals(String)}.
   */
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
