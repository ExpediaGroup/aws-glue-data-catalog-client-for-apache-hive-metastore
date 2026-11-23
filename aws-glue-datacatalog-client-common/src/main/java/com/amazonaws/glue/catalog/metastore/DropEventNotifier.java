package com.amazonaws.glue.catalog.metastore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.PARTITION;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.TABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_EVENT_LISTENERS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_EVENT_LISTENERS_REQUIRED_TABLE_PARAMETER;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_EVENT_LISTENERS_SKIP_ICEBERG;

/**
 * Calls Hive {@link MetaStoreEventListener}s after this client drops a table or partition.
 *
 * <p>A Hive metastore calls its listeners while it still holds the dropped table, so the events carry the table's
 * location and parameters. Glue keeps no copy of a dropped table, and its own delete events carry names only, so
 * the only place that still knows them is this client, which reads the table or partition before deleting it.
 * Listeners such as Apiary's {@code ApiarySnsListener} can then publish the same drop events they did from a Hive
 * metastore, for tools like Beekeeper that clean up the dropped data.
 *
 * <p>The caller passes the table as it was at the time of the drop, read on the drop call itself, as a Hive
 * metastore does. Only the listener calls run later, on a single background thread, so they can never slow down or
 * fail a drop. When the queue is full, an event is skipped and logged.
 *
 * <p>Events can be limited to tables with a given parameter set to {@code true}, and Iceberg tables can be skipped;
 * see {@code aws.glue.event.listeners.required-table-parameter} and {@code aws.glue.event.listeners.skip-iceberg}.
 * Counts and timings are also reported as Micrometer metrics when the host has Micrometer; see
 * {@link MicrometerDropEventMetrics}.
 */
public class DropEventNotifier {

  private static final Logger logger = Logger.getLogger(DropEventNotifier.class);

  static final String THREAD_NAME_FORMAT = "glue-drop-event-listener-%d";
  static final int QUEUE_SIZE = 10000;
  /** How long a JVM shutdown waits for queued events to be sent. */
  static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

  // Same test as Beekeeper's IsIcebergTablePredicate.
  static final String METADATA_LOCATION = "metadata_location";
  static final String TABLE_TYPE = "table_type";
  static final String ICEBERG = "iceberg";

  private static final DropEventNotifier DISABLED = new DropEventNotifier(
      Collections.<MetaStoreEventListener>emptyList(), null, null, false, DropEventMetrics.NOOP);

  /**
   * Listeners by the {@code aws.glue.event.listeners} value that created them, shared by every client in the JVM.
   * Hosts can create clients often: Waggle Dance builds a new one for every metastore status check, every few
   * minutes. Listeners such as ApiarySnsListener hold an SNS client that is never closed, so they are created once.
   */
  private static final ConcurrentMap<String, List<MetaStoreEventListener>> LISTENERS = new ConcurrentHashMap<>();

  private final List<MetaStoreEventListener> listeners;
  private final Executor executor;
  private final String requiredTableParameter;
  private final boolean skipIceberg;
  private final DropEventStats stats;

  /**
   * Returns a notifier for the listeners named in {@code aws.glue.event.listeners}. Listeners are created once per
   * JVM for each value, with the configuration of the first client that names them, and shared after that. A
   * listener that can't be loaded is logged and turns the notifier off, rather than failing the client; the next
   * client tries again.
   */
  public static DropEventNotifier create(Configuration conf) {
    String listenerNames = conf.getTrimmed(AWS_GLUE_EVENT_LISTENERS, "");
    if (listenerNames.isEmpty()) {
      return DISABLED;
    }
    List<MetaStoreEventListener> listeners = LISTENERS.get(listenerNames);
    if (listeners == null) {
      synchronized (LISTENERS) {
        listeners = LISTENERS.get(listenerNames);
        if (listeners == null) {
          try {
            listeners = ImmutableList.copyOf(
                MetaStoreUtils.getMetaStoreListeners(MetaStoreEventListener.class, conf, listenerNames));
          } catch (MetaException | RuntimeException | LinkageError e) {
            logger.error("Unable to load " + AWS_GLUE_EVENT_LISTENERS + "=" + listenerNames
                + "; drop events will not be sent", e);
            return DISABLED;
          }
          LISTENERS.put(listenerNames, listeners);
          logger.info("Sending drop table and drop partition events to " + listenerNames);
        }
      }
    }
    String requiredTableParameter = conf.getTrimmed(AWS_GLUE_EVENT_LISTENERS_REQUIRED_TABLE_PARAMETER, "");
    return new DropEventNotifier(listeners, SharedExecutor.INSTANCE,
        requiredTableParameter.isEmpty() ? null : requiredTableParameter,
        conf.getBoolean(AWS_GLUE_EVENT_LISTENERS_SKIP_ICEBERG, false), SharedExecutor.METRICS);
  }

  @VisibleForTesting
  DropEventNotifier(List<MetaStoreEventListener> listeners, Executor executor, String requiredTableParameter,
                    boolean skipIceberg) {
    this(listeners, executor, requiredTableParameter, skipIceberg, DropEventMetrics.NOOP);
  }

  @VisibleForTesting
  DropEventNotifier(List<MetaStoreEventListener> listeners, Executor executor, String requiredTableParameter,
                    boolean skipIceberg, DropEventMetrics metrics) {
    this.stats = new DropEventStats(metrics);
    this.listeners = instrument(listeners, stats);
    this.executor = executor;
    this.requiredTableParameter = requiredTableParameter;
    this.skipIceberg = skipIceberg;
  }

  /**
   * Wraps each listener so that its calls are counted and timed in {@code stats}. The listeners themselves are
   * shared by every client in the JVM; only these thin wrappers belong to this notifier.
   */
  private static List<MetaStoreEventListener> instrument(List<MetaStoreEventListener> listeners,
                                                         DropEventStats stats) {
    ImmutableList.Builder<MetaStoreEventListener> instrumented = ImmutableList.builder();
    for (MetaStoreEventListener listener : listeners) {
      instrumented.add(new InstrumentedListener(listener, stats));
    }
    return instrumented.build();
  }

  public boolean isEnabled() {
    return !listeners.isEmpty();
  }

  /**
   * Call after Glue confirmed the table was deleted, with the table as read before the delete. Never throws: this
   * runs on the drop path.
   */
  public void tableDropped(final Table table, final boolean deleteData) {
    if (!isEnabled() || table == null) {
      return;
    }
    try {
      if (!wanted(table, TABLE, 1)) {
        return;
      }
      final String name = table.getDbName() + "." + table.getTableName();
      submit(TABLE, 1, "drop table " + name, new Runnable() {
        @Override
        public void run() {
          final DropTableEvent event;
          try {
            event = new DropTableEvent(table, true, deleteData, null);
          } catch (Throwable t) {
            stats.failed(TABLE, DropEventMetrics.EVENT_ERROR, DropEventMetrics.NONE, listeners.size());
            logger.warn("Unable to build drop table event for " + name, t);
            return;
          }
          callListeners("drop table " + name, new Delivery() {
            @Override
            public void deliver(MetaStoreEventListener listener) throws MetaException {
              listener.onDropTable(event);
            }
          });
        }
      });
    } catch (RuntimeException e) {
      stats.failed(TABLE, DropEventMetrics.QUEUE_ERROR, DropEventMetrics.NONE, listeners.size());
      logger.warn("Unable to queue drop table event for " + table.getDbName() + "." + table.getTableName(), e);
    }
  }

  /**
   * Call after Glue confirmed the partitions were deleted, with the partitions as read before the delete and their
   * table as read on the drop call. Never throws: this runs on the drop path.
   */
  public void partitionsDropped(final Table table, List<Partition> partitions, final boolean deleteData) {
    if (!isEnabled() || table == null || partitions == null || partitions.isEmpty()) {
      return;
    }
    try {
      if (!wanted(table, PARTITION, partitions.size())) {
        return;
      }
      final List<Partition> dropped = ImmutableList.copyOf(partitions);
      final String name = table.getDbName() + "." + table.getTableName();
      submit(PARTITION, dropped.size(), "drop " + dropped.size() + " partition(s) of " + name, new Runnable() {
        @Override
        public void run() {
          for (Partition partition : dropped) {
            final DropPartitionEvent event;
            try {
              event = new DropPartitionEvent(table, partition, true, deleteData, null);
            } catch (Throwable t) {
              stats.failed(PARTITION, DropEventMetrics.EVENT_ERROR, DropEventMetrics.NONE, listeners.size());
              logger.warn("Unable to build drop partition event for " + partition.getValues() + " of " + name, t);
              continue;
            }
            callListeners("drop partition " + partition.getValues() + " of " + name, new Delivery() {
              @Override
              public void deliver(MetaStoreEventListener listener) throws MetaException {
                listener.onDropPartition(event);
              }
            });
          }
        }
      });
    } catch (RuntimeException e) {
      stats.failed(PARTITION, DropEventMetrics.QUEUE_ERROR, DropEventMetrics.NONE,
          (long) partitions.size() * listeners.size());
      logger.warn("Unable to queue drop partition events for " + table.getDbName() + "." + table.getTableName(), e);
    }
  }

  /**
   * Call when partitions were dropped but their table could not be read, so no events can be sent for them.
   * Logs and counts them. Never throws.
   */
  public void tableUnavailable(String dbName, String tableName, int partitionCount, Exception cause) {
    if (!isEnabled() || partitionCount <= 0) {
      return;
    }
    stats.failed(PARTITION, DropEventMetrics.TABLE_UNAVAILABLE, DropEventMetrics.NONE,
        (long) partitionCount * listeners.size());
    logger.warn("Unable to read table " + dbName + "." + tableName + " for " + partitionCount
        + " drop partition event(s); events not sent", cause);
  }

  private boolean wanted(Table table, String type, int events) {
    Map<String, String> parameters = table.getParameters();
    if (requiredTableParameter != null
        && (parameters == null || !"true".equalsIgnoreCase(trim(parameters.get(requiredTableParameter))))) {
      stats.filtered(type, DropEventMetrics.TABLE_PARAMETER_NOT_TRUE, events);
      return false;
    }
    if (skipIceberg && isIceberg(parameters)) {
      stats.filtered(type, DropEventMetrics.ICEBERG, events);
      return false;
    }
    return true;
  }

  @VisibleForTesting
  static boolean isIceberg(Map<String, String> parameters) {
    if (parameters == null) {
      return false;
    }
    String metadataLocation = trim(parameters.get(METADATA_LOCATION));
    String tableType = parameters.get(TABLE_TYPE);
    return (metadataLocation != null && !metadataLocation.isEmpty())
        || (tableType != null && tableType.toLowerCase(Locale.ROOT).contains(ICEBERG));
  }

  private static String trim(String value) {
    return value == null ? null : value.trim();
  }

  /** One call to a listener for an event. */
  private interface Delivery {
    void deliver(MetaStoreEventListener listener) throws MetaException;
  }

  /**
   * Calls every listener in turn. A listener that throws is logged and the others are still called; counting and
   * timing the calls is left to {@link InstrumentedListener}.
   */
  private void callListeners(String what, Delivery delivery) {
    for (MetaStoreEventListener listener : listeners) {
      try {
        delivery.deliver(listener);
      } catch (Throwable t) {
        logger.warn("Listener " + className(listener) + " failed on " + what, t);
      }
    }
  }

  private static String className(MetaStoreEventListener listener) {
    return listener instanceof InstrumentedListener
        ? ((InstrumentedListener) listener).delegateClassName()
        : listener.getClass().getName();
  }

  private void submit(String type, int events, String description, Runnable task) {
    try {
      executor.execute(task);
    } catch (RejectedExecutionException e) {
      boolean shuttingDown = executor instanceof ThreadPoolExecutor && ((ThreadPoolExecutor) executor).isShutdown();
      long count = stats.skipped(type,
          shuttingDown ? DropEventMetrics.SHUTTING_DOWN : DropEventMetrics.QUEUE_FULL, events);
      logger.warn("Drop event queue is full or shutting down; skipped " + description
          + " (" + count + " skipped so far)");
    }
  }

  /**
   * Stops taking new events and waits up to {@code timeout} for the queued ones to be sent. Returns how many were
   * still queued when it gave up.
   */
  @VisibleForTesting
  static int drain(ThreadPoolExecutor executor, long timeout, TimeUnit unit) {
    executor.shutdown();
    try {
      if (executor.awaitTermination(timeout, unit)) {
        return 0;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    int left = executor.getQueue().size();
    logger.warn("Shutting down with " + left + " drop event(s) not sent after waiting " + timeout + " "
        + unit.toString().toLowerCase(Locale.ROOT));
    return left;
  }

  /** Counts and timings of this notifier's events. */
  public DropEventStats getStats() {
    return stats;
  }

  /**
   * One daemon thread for every client in the JVM, created only when listeners are configured. On JVM shutdown, a
   * hook sends the queued events, waiting up to {@link #SHUTDOWN_TIMEOUT_SECONDS}, so a normal stop or redeploy
   * doesn't lose them. Its queue size is reported as a metric.
   */
  private static final class SharedExecutor {
    static final ThreadPoolExecutor INSTANCE;
    static final DropEventMetrics METRICS;

    static {
      INSTANCE = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
          new ArrayBlockingQueue<Runnable>(QUEUE_SIZE),
          new ThreadFactoryBuilder().setNameFormat(THREAD_NAME_FORMAT).setDaemon(true).build(),
          new ThreadPoolExecutor.AbortPolicy());
      INSTANCE.allowCoreThreadTimeOut(true);
      try {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
          @Override
          public void run() {
            drain(INSTANCE, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          }
        }, "glue-drop-event-listener-shutdown"));
      } catch (IllegalStateException | SecurityException e) {
        // The JVM is already shutting down, or hooks are not allowed: never fail the client for this.
        logger.warn("Unable to register the drop event shutdown hook; queued events may be lost on shutdown", e);
      }
      METRICS = DropEventMetrics.create();
      METRICS.queueSize(INSTANCE.getQueue());
    }
  }
}
