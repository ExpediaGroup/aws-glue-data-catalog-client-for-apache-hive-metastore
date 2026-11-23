package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;

import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.LISTENER_ERROR;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.PARTITION;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.TABLE;

/**
 * Wraps a listener to record each drop call's outcome and duration in {@link DropEventStats}, so the notifier only
 * decides what to send and sends it. A failure is recorded and then rethrown for the notifier to log.
 *
 * <p>Only {@code onDropTable} and {@code onDropPartition} are passed through: they are the only events this client
 * sends. Every other method keeps {@link MetaStoreEventListener}'s no-op default.
 */
final class InstrumentedListener extends MetaStoreEventListener {

  /** One call to the wrapped listener. */
  private interface ListenerCall {
    void run() throws MetaException;
  }

  private final MetaStoreEventListener delegate;
  private final String name;
  private final DropEventStats stats;

  InstrumentedListener(MetaStoreEventListener delegate, DropEventStats stats) {
    super(delegate.getConf());
    this.delegate = delegate;
    this.name = delegate.getClass().getSimpleName();
    this.stats = stats;
  }

  @Override
  public void onDropTable(final DropTableEvent event) throws MetaException {
    record(TABLE, new ListenerCall() {
      @Override
      public void run() throws MetaException {
        delegate.onDropTable(event);
      }
    });
  }

  @Override
  public void onDropPartition(final DropPartitionEvent event) throws MetaException {
    record(PARTITION, new ListenerCall() {
      @Override
      public void run() throws MetaException {
        delegate.onDropPartition(event);
      }
    });
  }

  /** The wrapped listener's class name, for log messages. */
  String delegateClassName() {
    return delegate.getClass().getName();
  }

  private void record(String type, ListenerCall call) throws MetaException {
    long start = System.nanoTime();
    boolean succeeded = false;
    try {
      call.run();
      succeeded = true;
      stats.delivered(type, name);
    } catch (Throwable t) {
      // Caught as Throwable so that a listener throwing an undeclared checked exception is counted too; the precise
      // rethrow below still only throws what call.run() can.
      stats.failed(type, LISTENER_ERROR, name, 1);
      throw t;
    } finally {
      stats.listenerCall(type, name, succeeded, System.nanoTime() - start);
    }
  }
}
