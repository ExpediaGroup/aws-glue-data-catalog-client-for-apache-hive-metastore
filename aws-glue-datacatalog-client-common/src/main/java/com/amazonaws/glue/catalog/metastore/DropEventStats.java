package com.amazonaws.glue.catalog.metastore;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts and timings for the drop events of one {@link DropEventNotifier}: kept as in-process counters and
 * forwarded to {@link DropEventMetrics} (Micrometer when available). Never throws.
 */
public class DropEventStats {

  private final DropEventMetrics metrics;

  private final AtomicLong delivered = new AtomicLong();
  private final AtomicLong failed = new AtomicLong();
  private final AtomicLong skipped = new AtomicLong();
  private final AtomicLong filtered = new AtomicLong();

  DropEventStats(DropEventMetrics metrics) {
    this.metrics = metrics;
  }

  /** One listener call completed. */
  void delivered(String type) {
    delivered.incrementAndGet();
    metrics.delivered(type, 1);
  }

  /** {@code count} listener calls threw or could not be made. */
  void failed(String type, long count) {
    failed.addAndGet(count);
    metrics.failed(type, count);
  }

  /**
   * A drop's {@code events} were skipped because the queue was full or shutting down. Returns how many drops have
   * been skipped so far.
   */
  long skipped(String type, int events) {
    metrics.skipped(type, events);
    return skipped.incrementAndGet();
  }

  /** {@code events} were not sent because of the table filters. */
  void filtered(String type, int events) {
    filtered.addAndGet(events);
    metrics.filtered(type, events);
  }

  /** One listener call took {@code nanos}. */
  void listenerCall(String type, long nanos) {
    metrics.listenerCall(type, nanos);
  }

  /** Reading a dropped partition's table on the drop call took {@code nanos}. */
  public void tableRead(long nanos, boolean succeeded) {
    metrics.tableRead(nanos, succeeded);
  }

  /** Listener calls that completed. */
  public long getDeliveredCount() {
    return delivered.get();
  }

  /** Listener calls that threw, or that were not made because the table could not be read. */
  public long getFailedCount() {
    return failed.get();
  }

  /** Drops whose events were skipped because the queue was full or shutting down. */
  public long getSkippedCount() {
    return skipped.get();
  }

  /** Events not sent because of the table filters. */
  public long getFilteredCount() {
    return filtered.get();
  }
}
