package com.amazonaws.glue.catalog.metastore;

import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.PARTITION;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.TABLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DropEventStatsTest {

  private DropEventMetrics metrics;
  private DropEventStats stats;

  @Before
  public void setup() {
    metrics = mock(DropEventMetrics.class);
    stats = new DropEventStats(metrics);
  }

  @Test
  public void countsAndForwardsEachOutcome() {
    stats.delivered(TABLE);
    stats.delivered(PARTITION);
    stats.failed(PARTITION, 3);
    stats.filtered(TABLE, 2);

    assertEquals(2, stats.getDeliveredCount());
    assertEquals(3, stats.getFailedCount());
    assertEquals(2, stats.getFilteredCount());
    verify(metrics).delivered(TABLE, 1);
    verify(metrics).delivered(PARTITION, 1);
    verify(metrics).failed(PARTITION, 3);
    verify(metrics).filtered(TABLE, 2);
  }

  @Test
  public void skippedCountsDropsAndForwardsEvents() {
    assertEquals(1, stats.skipped(PARTITION, 5));
    assertEquals(2, stats.skipped(TABLE, 1));

    assertEquals(2, stats.getSkippedCount());
    verify(metrics).skipped(PARTITION, 5);
    verify(metrics).skipped(TABLE, 1);
  }

  @Test
  public void forwardsTimings() {
    stats.listenerCall(TABLE, 1000L);
    stats.tableRead(2000L, true);

    verify(metrics).listenerCall(TABLE, 1000L);
    verify(metrics).tableRead(2000L, true);
  }
}
