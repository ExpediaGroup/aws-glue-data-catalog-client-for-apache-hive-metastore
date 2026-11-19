package com.amazonaws.glue.catalog.metastore;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_EVENT_LISTENERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DropEventNotifierTest {

  private static final Executor DIRECT = new Executor() {
    @Override
    public void execute(Runnable command) {
      command.run();
    }
  };

  private final FakeTicker ticker = new FakeTicker();
  private MetaStoreEventListener listener;
  private DropEventNotifier.TableLoader tableLoader;
  private Table table;
  private DropEventNotifier notifier;

  @Before
  public void setup() throws Exception {
    listener = mock(MetaStoreEventListener.class);
    tableLoader = mock(DropEventNotifier.TableLoader.class);
    table = table("db", "tbl", "s3://bucket/db/tbl");
    when(tableLoader.getTable("db", "tbl")).thenReturn(table);
    notifier = new DropEventNotifier(ImmutableList.of(listener), DIRECT, ticker);
  }

  @Test
  public void disabledWithoutListenersConfigured() {
    DropEventNotifier disabled = DropEventNotifier.create(new Configuration(false));
    assertFalse(disabled.isEnabled());
    disabled.tableDropped(table, true);
    assertEquals(0, disabled.getDeliveredCount());
  }

  @Test
  public void disabledWhenAListenerCannotBeLoaded() {
    Configuration conf = new Configuration(false);
    conf.set(AWS_GLUE_EVENT_LISTENERS, "com.example.DoesNotExist");
    assertFalse(DropEventNotifier.create(conf).isEnabled());
  }

  @Test
  public void loadsConfiguredListeners() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(AWS_GLUE_EVENT_LISTENERS, " " + RecordingListener.class.getName() + " ");
    DropEventNotifier created = DropEventNotifier.create(conf);
    assertTrue(created.isEnabled());

    RecordingListener.EVENTS.clear();
    created.tableDropped(table, false);
    ListenerEvent event = RecordingListener.EVENTS.poll(10, TimeUnit.SECONDS);
    assertNotNull("no event delivered on the background thread", event);
    assertSame(table, ((DropTableEvent) event).getTable());
  }

  @Test
  public void listenersAreCreatedOnceAndSharedByClients() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(AWS_GLUE_EVENT_LISTENERS, RecordingListener.class.getName());

    DropEventNotifier first = DropEventNotifier.create(conf);
    int createdAfterFirst = RecordingListener.CREATED.get();
    DropEventNotifier second = DropEventNotifier.create(new Configuration(conf));

    assertTrue(createdAfterFirst >= 1);
    assertEquals("a second client must not create new listeners", createdAfterFirst, RecordingListener.CREATED.get());
    assertTrue(first.isEnabled() && second.isEnabled());
  }

  @Test
  public void tableDroppedSendsDropTableEvent() throws Exception {
    notifier.tableDropped(table, true);

    ArgumentCaptor<DropTableEvent> captor = ArgumentCaptor.forClass(DropTableEvent.class);
    verify(listener).onDropTable(captor.capture());
    DropTableEvent event = captor.getValue();
    assertSame(table, event.getTable());
    assertTrue(event.getStatus());
    assertTrue(event.getDeleteData());
    assertEquals(1, notifier.getDeliveredCount());
  }

  @Test
  public void partitionsDroppedSendsOneEventPerPartitionWithTheTable() throws Exception {
    Partition p1 = partition("2026-11-01");
    Partition p2 = partition("2026-11-02");

    notifier.partitionsDropped("db", "tbl", Arrays.asList(p1, p2), false, tableLoader);

    ArgumentCaptor<DropPartitionEvent> captor = ArgumentCaptor.forClass(DropPartitionEvent.class);
    verify(listener, times(2)).onDropPartition(captor.capture());
    List<Partition> sent = new ArrayList<>();
    for (DropPartitionEvent event : captor.getAllValues()) {
      assertSame(table, event.getTable());
      assertTrue(event.getStatus());
      assertFalse(event.getDeleteData());
      Iterator<Partition> it = event.getPartitionIterator();
      sent.add(it.next());
      assertFalse(it.hasNext());
    }
    assertEquals(Arrays.asList(p1, p2), sent);
    verify(tableLoader, times(1)).getTable("db", "tbl");
  }

  @Test
  public void tableIsCachedForPartitionBursts() throws Exception {
    notifier.partitionsDropped("db", "tbl", Collections.singletonList(partition("a")), false, tableLoader);
    ticker.advance(DropEventNotifier.TABLE_CACHE_TTL_SECONDS - 1, TimeUnit.SECONDS);
    notifier.partitionsDropped("DB", "TBL", Collections.singletonList(partition("b")), false, tableLoader);
    verify(tableLoader, times(1)).getTable(any(String.class), any(String.class));

    ticker.advance(2, TimeUnit.SECONDS);
    notifier.partitionsDropped("db", "tbl", Collections.singletonList(partition("c")), false, tableLoader);
    verify(tableLoader, times(2)).getTable(any(String.class), any(String.class));
  }

  @Test
  public void tableDropClearsCachedTable() throws Exception {
    notifier.partitionsDropped("db", "tbl", Collections.singletonList(partition("a")), false, tableLoader);
    notifier.tableDropped(table, false);
    notifier.partitionsDropped("db", "tbl", Collections.singletonList(partition("b")), false, tableLoader);
    verify(tableLoader, times(2)).getTable("db", "tbl");
  }

  @Test
  public void noPartitionEventsWhenTheTableCannotBeRead() throws Exception {
    when(tableLoader.getTable("db", "tbl")).thenThrow(new NoSuchObjectException("db.tbl"));

    notifier.partitionsDropped("db", "tbl", Arrays.asList(partition("a"), partition("b")), false, tableLoader);

    verify(listener, never()).onDropPartition(any(DropPartitionEvent.class));
    assertEquals(2, notifier.getFailedCount());
  }

  @Test
  public void failingListenerDoesNotStopOthers() throws Exception {
    MetaStoreEventListener failing = mock(MetaStoreEventListener.class);
    doThrow(new RuntimeException("SNS down")).when(failing).onDropTable(any(DropTableEvent.class));
    DropEventNotifier both = new DropEventNotifier(ImmutableList.of(failing, listener), DIRECT, ticker);

    both.tableDropped(table, false);

    verify(listener).onDropTable(any(DropTableEvent.class));
    assertEquals(1, both.getFailedCount());
    assertEquals(1, both.getDeliveredCount());
  }

  @Test
  public void fullQueueSkipsTheEventWithoutFailing() throws Exception {
    Executor full = new Executor() {
      @Override
      public void execute(Runnable command) {
        throw new RejectedExecutionException("full");
      }
    };
    DropEventNotifier rejecting = new DropEventNotifier(ImmutableList.of(listener), full, ticker);

    rejecting.tableDropped(table, false);
    rejecting.partitionsDropped("db", "tbl", Collections.singletonList(partition("a")), false, tableLoader);

    verify(listener, never()).onDropTable(any(DropTableEvent.class));
    assertEquals(2, rejecting.getRejectedCount());
  }

  @Test
  public void emptyPartitionListSendsNothing() throws Exception {
    notifier.partitionsDropped("db", "tbl", Collections.<Partition>emptyList(), false, tableLoader);
    notifier.partitionsDropped("db", "tbl", null, false, tableLoader);
    verify(tableLoader, never()).getTable(any(String.class), any(String.class));
  }

  static Table table(String db, String name, String location) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(location);
    Table t = new Table();
    t.setDbName(db);
    t.setTableName(name);
    t.setSd(sd);
    t.setParameters(ImmutableMap.of("beekeeper.remove.unreferenced.data", "true"));
    return t;
  }

  static Partition partition(String value) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("s3://bucket/db/tbl/dt=" + value);
    Partition p = new Partition();
    p.setDbName("db");
    p.setTableName("tbl");
    p.setValues(Collections.singletonList(value));
    p.setSd(sd);
    return p;
  }

  /** Loaded by class name through aws.glue.event.listeners. */
  public static class RecordingListener extends MetaStoreEventListener {
    static final BlockingQueue<ListenerEvent> EVENTS = new LinkedBlockingQueue<>();
    static final AtomicInteger CREATED = new AtomicInteger();

    public RecordingListener(Configuration config) {
      super(config);
      CREATED.incrementAndGet();
    }

    @Override
    public void onDropTable(DropTableEvent event) {
      EVENTS.add(event);
    }

    @Override
    public void onDropPartition(DropPartitionEvent event) {
      EVENTS.add(event);
    }
  }

  private static final class FakeTicker extends Ticker {
    private final AtomicLong nanos = new AtomicLong();

    void advance(long time, TimeUnit unit) {
      nanos.addAndGet(unit.toNanos(time));
    }

    @Override
    public long read() {
      return nanos.get();
    }
  }
}
