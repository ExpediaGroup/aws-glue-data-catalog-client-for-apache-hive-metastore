package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.LISTENER_ERROR;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.PARTITION;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class InstrumentedListenerTest {

  private MetaStoreEventListener delegate;
  private DropEventMetrics metrics;
  private DropEventStats stats;
  private InstrumentedListener instrumented;

  @Before
  public void setup() {
    delegate = mock(MetaStoreEventListener.class);
    metrics = mock(DropEventMetrics.class);
    stats = new DropEventStats(metrics);
    instrumented = new InstrumentedListener(delegate, stats);
  }

  @Test
  public void passesDropTableThroughAndCountsIt() throws Exception {
    DropTableEvent event = new DropTableEvent(new Table(), true, false, null);

    instrumented.onDropTable(event);

    verify(delegate).onDropTable(event);
    assertEquals(1, stats.getDeliveredCount());
    String name = delegate.getClass().getSimpleName();
    verify(metrics).delivered(TABLE, name);
    verify(metrics).listenerCall(eq(TABLE), eq(name), eq(true), anyLong());
  }

  @Test
  public void passesDropPartitionThroughAndCountsIt() throws Exception {
    DropPartitionEvent event = mock(DropPartitionEvent.class);

    instrumented.onDropPartition(event);

    verify(delegate).onDropPartition(event);
    assertEquals(1, stats.getDeliveredCount());
    verify(metrics).listenerCall(eq(PARTITION), anyString(), eq(true), anyLong());
  }

  @Test
  public void countsAFailureAndRethrowsIt() throws Exception {
    MetaException failure = new MetaException("boom");
    DropTableEvent event = new DropTableEvent(new Table(), true, false, null);
    doThrow(failure).when(delegate).onDropTable(event);

    try {
      instrumented.onDropTable(event);
      fail("the listener's failure must be rethrown for the notifier to log");
    } catch (MetaException e) {
      assertSame(failure, e);
    }

    assertEquals(0, stats.getDeliveredCount());
    assertEquals(1, stats.getFailedCount());
    String name = delegate.getClass().getSimpleName();
    verify(metrics).failed(TABLE, LISTENER_ERROR, name, 1);
    verify(metrics, never()).delivered(anyString(), anyString());
    verify(metrics).listenerCall(eq(TABLE), eq(name), eq(false), anyLong());
  }

  @Test
  public void countsAnUncheckedFailureToo() throws Exception {
    DropTableEvent event = new DropTableEvent(new Table(), true, false, null);
    doThrow(new IllegalStateException("boom")).when(delegate).onDropTable(event);

    try {
      instrumented.onDropTable(event);
      fail("expected the IllegalStateException to be rethrown");
    } catch (IllegalStateException expected) {
      // rethrown unchanged
    }

    assertEquals(1, stats.getFailedCount());
  }

  @Test
  public void keepsTheDelegatesConfAndName() {
    Configuration conf = new Configuration(false);
    MetaStoreEventListener withConf = new MetaStoreEventListener(conf) {};

    InstrumentedListener wrapped = new InstrumentedListener(withConf, stats);

    assertSame(conf, wrapped.getConf());
    assertEquals(withConf.getClass().getName(), wrapped.delegateClassName());
  }
}
