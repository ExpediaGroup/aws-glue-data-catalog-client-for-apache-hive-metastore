package com.amazonaws.glue.catalog.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_EVENT_LISTENERS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_EVENT_LISTENERS_REQUIRED_TABLE_PARAMETER;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_EVENT_LISTENERS_SKIP_ICEBERG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DropEventNotifierTest {

  private static final Executor DIRECT = new Executor() {
    @Override
    public void execute(Runnable command) {
      command.run();
    }
  };

  private static final String BEEKEEPER_UNREFERENCED = "beekeeper.remove.unreferenced.data";

  private MetaStoreEventListener listener;
  private Table table;
  private DropEventNotifier notifier;

  @Before
  public void setup() throws Exception {
    listener = mock(MetaStoreEventListener.class);
    table = table(ImmutableMap.of(BEEKEEPER_UNREFERENCED, "true"));
    notifier = notifier(DIRECT, null, false);
  }

  @Test
  public void disabledWithoutListenersConfigured() {
    DropEventNotifier disabled = DropEventNotifier.create(new Configuration(false));
    assertFalse(disabled.isEnabled());
    disabled.tableDropped(table, true);
    disabled.partitionsDropped(table, Collections.singletonList(partition("a")), true);
    assertEquals(0, disabled.getStats().getDeliveredCount());
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
  public void createReadsTheTableFilters() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(AWS_GLUE_EVENT_LISTENERS, RecordingListener.class.getName());
    conf.set(AWS_GLUE_EVENT_LISTENERS_REQUIRED_TABLE_PARAMETER, " " + BEEKEEPER_UNREFERENCED + " ");
    conf.setBoolean(AWS_GLUE_EVENT_LISTENERS_SKIP_ICEBERG, true);
    DropEventNotifier created = DropEventNotifier.create(conf);

    RecordingListener.EVENTS.clear();
    created.tableDropped(table(Collections.<String, String>emptyMap()), false);
    created.tableDropped(table(ImmutableMap.of(BEEKEEPER_UNREFERENCED, "true", "table_type", "ICEBERG")), false);
    created.tableDropped(table, false);

    ListenerEvent event = RecordingListener.EVENTS.poll(10, TimeUnit.SECONDS);
    assertNotNull(event);
    assertSame(table, ((DropTableEvent) event).getTable());
    assertNull(RecordingListener.EVENTS.poll(300, TimeUnit.MILLISECONDS));
    assertEquals(2, created.getStats().getFilteredCount());
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
    assertEquals(1, notifier.getStats().getDeliveredCount());
  }

  @Test
  public void partitionsDroppedSendsOneEventPerPartitionWithTheGivenTable() throws Exception {
    Partition p1 = partition("2026-11-01");
    Partition p2 = partition("2026-11-02");

    notifier.partitionsDropped(table, Arrays.asList(p1, p2), false);

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
  }

  @Test
  public void requiredTableParameterMustBeTrue() throws Exception {
    DropEventNotifier filtering = notifier(DIRECT, BEEKEEPER_UNREFERENCED, false);
    List<Partition> partitions = Collections.singletonList(partition("a"));

    filtering.tableDropped(table(Collections.<String, String>emptyMap()), false);
    filtering.tableDropped(table(ImmutableMap.of(BEEKEEPER_UNREFERENCED, "false")), false);
    filtering.tableDropped(table(null), false);
    filtering.partitionsDropped(table(ImmutableMap.of(BEEKEEPER_UNREFERENCED, "false")), partitions, false);
    verify(listener, never()).onDropTable(any(DropTableEvent.class));
    verify(listener, never()).onDropPartition(any(DropPartitionEvent.class));
    assertEquals(4, filtering.getStats().getFilteredCount());

    filtering.tableDropped(table(ImmutableMap.of(BEEKEEPER_UNREFERENCED, " TRUE ")), false);
    filtering.partitionsDropped(table, partitions, false);
    verify(listener).onDropTable(any(DropTableEvent.class));
    verify(listener).onDropPartition(any(DropPartitionEvent.class));
  }

  @Test
  public void skipIcebergSkipsIcebergTablesEvenWhenTagged() throws Exception {
    DropEventNotifier filtering = notifier(DIRECT, BEEKEEPER_UNREFERENCED, true);

    filtering.tableDropped(table(ImmutableMap.of(BEEKEEPER_UNREFERENCED, "true", "table_type", "ICEBERG")), false);
    filtering.tableDropped(table(ImmutableMap.of(BEEKEEPER_UNREFERENCED, "true",
        "metadata_location", "s3://bucket/db/tbl/metadata/00001.metadata.json")), false);
    verify(listener, never()).onDropTable(any(DropTableEvent.class));
    assertEquals(2, filtering.getStats().getFilteredCount());

    filtering.tableDropped(table, false);
    verify(listener).onDropTable(any(DropTableEvent.class));
  }

  @Test
  public void icebergTablesAreSentWhenSkipIcebergIsOff() throws Exception {
    notifier.tableDropped(table(ImmutableMap.of("table_type", "ICEBERG")), false);

    verify(listener).onDropTable(any(DropTableEvent.class));
  }

  @Test
  public void isIcebergMatchesBeekeepersTest() {
    assertTrue(DropEventNotifier.isIceberg(ImmutableMap.of("table_type", "ICEBERG")));
    assertTrue(DropEventNotifier.isIceberg(ImmutableMap.of("table_type", "iceberg")));
    assertTrue(DropEventNotifier.isIceberg(ImmutableMap.of("metadata_location", "s3://b/m.json")));
    assertFalse(DropEventNotifier.isIceberg(ImmutableMap.of("metadata_location", "  ")));
    assertFalse(DropEventNotifier.isIceberg(ImmutableMap.of("table_type", "EXTERNAL_TABLE")));
    assertFalse(DropEventNotifier.isIceberg(Collections.<String, String>emptyMap()));
    assertFalse(DropEventNotifier.isIceberg(null));
  }

  @Test
  public void tableUnavailableCountsTheSkippedEvents() throws Exception {
    notifier.tableUnavailable("db", "tbl", 3, new NoSuchObjectException("db.tbl"));

    verify(listener, never()).onDropPartition(any(DropPartitionEvent.class));
    assertEquals(3, notifier.getStats().getFailedCount());
  }

  @Test
  public void failingListenerDoesNotStopOthers() throws Exception {
    MetaStoreEventListener failing = mock(MetaStoreEventListener.class);
    doThrow(new RuntimeException("SNS down")).when(failing).onDropTable(any(DropTableEvent.class));
    DropEventNotifier both = new DropEventNotifier(ImmutableList.of(failing, listener), DIRECT, null, false);

    both.tableDropped(table, false);

    verify(listener).onDropTable(any(DropTableEvent.class));
    assertEquals(1, both.getStats().getFailedCount());
    assertEquals(1, both.getStats().getDeliveredCount());
  }

  @Test
  public void fullQueueSkipsTheEventWithoutFailing() throws Exception {
    Executor full = new Executor() {
      @Override
      public void execute(Runnable command) {
        throw new RejectedExecutionException("full");
      }
    };
    DropEventNotifier rejecting = notifier(full, null, false);

    rejecting.tableDropped(table, false);
    rejecting.partitionsDropped(table, Collections.singletonList(partition("a")), false);

    verify(listener, never()).onDropTable(any(DropTableEvent.class));
    assertEquals(2, rejecting.getStats().getSkippedCount());
  }

  @Test
  public void neverThrowsOnTheDropPath() throws Exception {
    Executor broken = new Executor() {
      @Override
      public void execute(Runnable command) {
        throw new IllegalStateException("executor broken");
      }
    };
    DropEventNotifier failing = notifier(broken, null, false);

    failing.tableDropped(table, false);
    failing.partitionsDropped(table, Collections.singletonList(partition("a")), false);
    notifier.partitionsDropped(table, Arrays.asList(partition("a"), null), false);

    verify(listener, never()).onDropTable(any(DropTableEvent.class));
    verify(listener, never()).onDropPartition(any(DropPartitionEvent.class));
    assertEquals(2, failing.getStats().getFailedCount());
    assertEquals(2, notifier.getStats().getFailedCount());
  }

  @Test
  public void drainSendsQueuedEventsBeforeShutdown() throws Exception {
    ThreadPoolExecutor executor = singleThreadExecutor();
    final AtomicInteger sent = new AtomicInteger();
    for (int i = 0; i < 5; i++) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          sent.incrementAndGet();
        }
      });
    }

    int left = DropEventNotifier.drain(executor, 5, TimeUnit.SECONDS);

    assertEquals(0, left);
    assertEquals(5, sent.get());
    assertTrue(executor.isTerminated());
  }

  @Test
  public void drainReportsEventsLeftWhenTimeoutExpires() throws Exception {
    ThreadPoolExecutor executor = singleThreadExecutor();
    final CountDownLatch release = new CountDownLatch(1);
    Runnable blocked = new Runnable() {
      @Override
      public void run() {
        try {
          release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    executor.execute(blocked);
    executor.execute(blocked);
    executor.execute(blocked);

    try {
      assertEquals(2, DropEventNotifier.drain(executor, 100, TimeUnit.MILLISECONDS));
    } finally {
      release.countDown();
    }
  }

  @Test
  public void eventsAfterShutdownAreSkippedWithoutFailing() throws Exception {
    ThreadPoolExecutor executor = singleThreadExecutor();
    DropEventNotifier stopping = notifier(executor, null, false);
    DropEventNotifier.drain(executor, 1, TimeUnit.SECONDS);

    stopping.tableDropped(table, false);

    verify(listener, never()).onDropTable(any(DropTableEvent.class));
    assertEquals(1, stopping.getStats().getSkippedCount());
  }

  @Test
  public void reportsMetricsForEachOutcome() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetaStoreEventListener failing = mock(MetaStoreEventListener.class);
    doThrow(new RuntimeException("SNS down")).when(failing).onDropTable(any(DropTableEvent.class));
    DropEventNotifier measured = new DropEventNotifier(ImmutableList.of(listener, failing), DIRECT,
        BEEKEEPER_UNREFERENCED, false, new MicrometerDropEventMetrics(registry));

    measured.partitionsDropped(table, Arrays.asList(partition("a"), partition("b")), false);
    measured.tableDropped(table, false);
    measured.tableDropped(table(Collections.<String, String>emptyMap()), false);
    measured.tableUnavailable("db", "tbl", 3, new NoSuchObjectException("db.tbl"));
    measured.getStats().tableRead(TimeUnit.MILLISECONDS.toNanos(12), true);

    assertEquals(4.0, events(registry, "partition", "delivered"), 0);
    assertEquals(6.0, events(registry, "partition", "failed"), 0);
    assertEquals(1.0, events(registry, "table", "delivered"), 0);
    assertEquals(1.0, events(registry, "table", "failed"), 0);
    assertEquals(1.0, events(registry, "table", "filtered"), 0);
    assertEquals(4, registry.get(MicrometerDropEventMetrics.LISTENER).tag("type", "partition").timer().count());
    assertEquals(1, registry.get(MicrometerDropEventMetrics.TABLE_READ).tag("result", "success").timer().count());
  }

  @Test
  public void reportsSkippedEventsWhenTheQueueIsFull() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    Executor full = new Executor() {
      @Override
      public void execute(Runnable command) {
        throw new RejectedExecutionException("full");
      }
    };
    DropEventNotifier measured = new DropEventNotifier(ImmutableList.of(listener), full, null, false,
        new MicrometerDropEventMetrics(registry));

    measured.partitionsDropped(table, Arrays.asList(partition("a"), partition("b")), false);

    assertEquals(2.0, events(registry, "partition", "skipped"), 0);
  }

  private static double events(SimpleMeterRegistry registry, String type, String result) {
    return registry.get(MicrometerDropEventMetrics.EVENTS).tag("type", type).tag("result", result).counter().count();
  }

  @Test
  public void emptyPartitionListSendsNothing() throws Exception {
    notifier.partitionsDropped(table, Collections.<Partition>emptyList(), false);
    notifier.partitionsDropped(table, null, false);
    notifier.partitionsDropped(null, Collections.singletonList(partition("a")), false);

    verify(listener, never()).onDropPartition(any(DropPartitionEvent.class));
  }

  private DropEventNotifier notifier(Executor executor, String requiredTableParameter, boolean skipIceberg) {
    return new DropEventNotifier(ImmutableList.of(listener), executor, requiredTableParameter, skipIceberg);
  }

  private static ThreadPoolExecutor singleThreadExecutor() {
    return new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(10));
  }

  static Table table(Map<String, String> parameters) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("s3://bucket/db/tbl");
    Table t = new Table();
    t.setDbName("db");
    t.setTableName("tbl");
    t.setSd(sd);
    t.setParameters(parameters == null ? null : new HashMap<>(parameters));
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
}
