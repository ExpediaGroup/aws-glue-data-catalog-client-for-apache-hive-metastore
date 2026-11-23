package com.amazonaws.glue.catalog.metastore;

import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.ICEBERG;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.LISTENER_ERROR;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.NONE;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.PARTITION;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.QUEUE_FULL;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.SHUTTING_DOWN;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.TABLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DropEventStatsTest {

  private static final String SNS = "ApiarySnsListener";

  private DropEventMetrics metrics;
  private DropEventStats stats;

  @Before
  public void setup() {
    metrics = mock(DropEventMetrics.class);
    stats = new DropEventStats(metrics);
  }

  @Test
  public void countsAndForwardsEachOutcome() {
    stats.delivered(TABLE, SNS);
    stats.delivered(PARTITION, SNS);
    stats.failed(PARTITION, LISTENER_ERROR, SNS, 3);
    stats.filtered(TABLE, ICEBERG, 2);

    assertEquals(2, stats.getDeliveredCount());
    assertEquals(3, stats.getFailedCount());
    assertEquals(2, stats.getFilteredCount());
    verify(metrics).delivered(TABLE, SNS);
    verify(metrics).delivered(PARTITION, SNS);
    verify(metrics).failed(PARTITION, LISTENER_ERROR, SNS, 3);
    verify(metrics).filtered(TABLE, ICEBERG, 2);
  }

  @Test
  public void skippedCountsDropsAndForwardsEvents() {
    assertEquals(1, stats.skipped(PARTITION, QUEUE_FULL, 5));
    assertEquals(2, stats.skipped(TABLE, SHUTTING_DOWN, 1));

    assertEquals(2, stats.getSkippedCount());
    verify(metrics).skipped(PARTITION, QUEUE_FULL, 5);
    verify(metrics).skipped(TABLE, SHUTTING_DOWN, 1);
  }

  @Test
  public void forwardsTimings() {
    stats.listenerCall(TABLE, SNS, true, 1000L);
    stats.tableRead(2000L, true, NONE);

    verify(metrics).listenerCall(TABLE, SNS, true, 1000L);
    verify(metrics).tableRead(2000L, true, NONE);
  }
}
