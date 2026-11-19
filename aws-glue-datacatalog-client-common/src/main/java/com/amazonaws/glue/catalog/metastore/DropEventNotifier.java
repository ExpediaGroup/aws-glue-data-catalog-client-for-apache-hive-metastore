package com.amazonaws.glue.catalog.metastore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_EVENT_LISTENERS;

/**
 * Calls Hive {@link MetaStoreEventListener}s after this client drops a table or partition.
 *
 * <p>A Hive metastore calls its listeners while it still holds the dropped table, so the events carry the table's
 * location and parameters. Glue keeps no copy of a dropped table, and its own delete events carry names only, so
 * the only place that still knows them is this client, which reads the table or partition before deleting it.
 * Listeners such as Apiary's {@code ApiarySnsListener} can then publish the same drop events they did from a Hive
 * metastore, for tools like Beekeeper that clean up the dropped data.
 *
 * <p>Listeners run on a single background thread after Glue confirms the delete, so they can never slow down or
 * fail a drop. When the queue is full, an event is dropped and logged. Partition events need the table too, for
 * its parameters; it is read on the background thread and cached briefly, as partitions are often dropped in
 * bursts on the same table.
 */
public class DropEventNotifier {

  private static final Logger logger = Logger.getLogger(DropEventNotifier.class);

  static final String THREAD_NAME_FORMAT = "glue-drop-event-listener-%d";
  static final int QUEUE_SIZE = 10000;
  static final long TABLE_CACHE_TTL_SECONDS = 60;
  static final int TABLE_CACHE_MAX_SIZE = 1000;

  /** Reads the table that a dropped partition belongs to. */
  interface TableLoader {
    Table getTable(String dbName, String tableName) throws TException;
  }

  private static final DropEventNotifier DISABLED =
      new DropEventNotifier(Collections.<MetaStoreEventListener>emptyList(), null, Ticker.systemTicker());

  private final List<MetaStoreEventListener> listeners;
  private final Executor executor;
  private final Cache<String, Table> tableCache;

  private final AtomicLong delivered = new AtomicLong();
  private final AtomicLong failed = new AtomicLong();
  private final AtomicLong rejected = new AtomicLong();

  /**
   * Loads the listeners named in {@code aws.glue.event.listeners}. A listener that can't be loaded is logged and
   * turns the notifier off, rather than failing the client.
   */
  public static DropEventNotifier create(Configuration conf) {
    String listenerNames = conf.getTrimmed(AWS_GLUE_EVENT_LISTENERS, "");
    if (listenerNames.isEmpty()) {
      return DISABLED;
    }
    List<MetaStoreEventListener> listeners;
    try {
      listeners = MetaStoreUtils.getMetaStoreListeners(MetaStoreEventListener.class, conf, listenerNames);
    } catch (MetaException | RuntimeException | LinkageError e) {
      logger.error("Unable to load " + AWS_GLUE_EVENT_LISTENERS + "=" + listenerNames
          + "; drop events will not be sent", e);
      return DISABLED;
    }
    logger.info("Sending drop table and drop partition events to " + listenerNames);
    return new DropEventNotifier(listeners, SharedExecutor.INSTANCE, Ticker.systemTicker());
  }

  @VisibleForTesting
  DropEventNotifier(List<MetaStoreEventListener> listeners, Executor executor, Ticker ticker) {
    this.listeners = ImmutableList.copyOf(listeners);
    this.executor = executor;
    this.tableCache = CacheBuilder.newBuilder()
        .maximumSize(TABLE_CACHE_MAX_SIZE)
        .expireAfterWrite(TABLE_CACHE_TTL_SECONDS, TimeUnit.SECONDS)
        .ticker(ticker)
        .build();
  }

  public boolean isEnabled() {
    return !listeners.isEmpty();
  }

  /** Call after Glue confirmed the table was deleted, with the table as read before the delete. */
  public void tableDropped(final Table table, final boolean deleteData) {
    if (!isEnabled() || table == null) {
      return;
    }
    tableCache.invalidate(key(table.getDbName(), table.getTableName()));
    submit("drop table " + table.getDbName() + "." + table.getTableName(), new Runnable() {
      @Override
      public void run() {
        DropTableEvent event;
        try {
          event = new DropTableEvent(table, true, deleteData, null);
        } catch (Throwable t) {
          failed.addAndGet(listeners.size());
          logger.warn("Unable to build drop table event for " + table.getDbName() + "." + table.getTableName(), t);
          return;
        }
        for (MetaStoreEventListener listener : listeners) {
          try {
            listener.onDropTable(event);
            delivered.incrementAndGet();
          } catch (Throwable t) {
            failed.incrementAndGet();
            logger.warn("Listener " + listener.getClass().getName() + " failed on drop table "
                + table.getDbName() + "." + table.getTableName(), t);
          }
        }
      }
    });
  }

  /**
   * Call after Glue confirmed the partitions were deleted, with the partitions as read before the delete. The
   * table is read on the background thread, through {@code tableLoader}, and cached for a short time.
   */
  public void partitionsDropped(final String dbName, final String tableName, List<Partition> partitions,
                                final boolean deleteData, final TableLoader tableLoader) {
    if (!isEnabled() || partitions == null || partitions.isEmpty()) {
      return;
    }
    final List<Partition> dropped = ImmutableList.copyOf(partitions);
    final String name = dbName + "." + tableName;
    submit("drop " + dropped.size() + " partition(s) of " + name, new Runnable() {
      @Override
      public void run() {
        Table table;
        try {
          table = cachedTable(dbName, tableName, tableLoader);
        } catch (Throwable t) {
          failed.addAndGet((long) dropped.size() * listeners.size());
          logger.warn("Unable to read table " + name + " for " + dropped.size()
              + " drop partition event(s); events not sent", t);
          return;
        }
        for (Partition partition : dropped) {
          DropPartitionEvent event;
          try {
            event = new DropPartitionEvent(table, partition, true, deleteData, null);
          } catch (Throwable t) {
            failed.addAndGet(listeners.size());
            logger.warn("Unable to build drop partition event for " + partition.getValues() + " of " + name, t);
            continue;
          }
          for (MetaStoreEventListener listener : listeners) {
            try {
              listener.onDropPartition(event);
              delivered.incrementAndGet();
            } catch (Throwable t) {
              failed.incrementAndGet();
              logger.warn("Listener " + listener.getClass().getName() + " failed on drop partition "
                  + partition.getValues() + " of " + name, t);
            }
          }
        }
      }
    });
  }

  private Table cachedTable(String dbName, String tableName, TableLoader tableLoader) throws TException {
    String key = key(dbName, tableName);
    Table table = tableCache.getIfPresent(key);
    if (table == null) {
      table = tableLoader.getTable(dbName, tableName);
      tableCache.put(key, table);
    }
    return table;
  }

  private void submit(String description, Runnable task) {
    try {
      executor.execute(task);
    } catch (RejectedExecutionException e) {
      long count = rejected.incrementAndGet();
      logger.warn("Drop event queue is full; skipped " + description + " (" + count + " skipped so far)");
    }
  }

  private static String key(String dbName, String tableName) {
    return (dbName + "." + tableName).toLowerCase(Locale.ROOT);
  }

  /** Listener calls that completed. */
  public long getDeliveredCount() {
    return delivered.get();
  }

  /** Listener calls that threw, or that were not made because the table could not be read. */
  public long getFailedCount() {
    return failed.get();
  }

  /** Drops whose events were skipped because the queue was full. */
  public long getRejectedCount() {
    return rejected.get();
  }

  /** One daemon thread for every client in the JVM, created only when listeners are configured. */
  private static final class SharedExecutor {
    static final ThreadPoolExecutor INSTANCE;

    static {
      INSTANCE = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
          new ArrayBlockingQueue<Runnable>(QUEUE_SIZE),
          new ThreadFactoryBuilder().setNameFormat(THREAD_NAME_FORMAT).setDaemon(true).build(),
          new ThreadPoolExecutor.AbortPolicy());
      INSTANCE.allowCoreThreadTimeOut(true);
    }
  }
}
