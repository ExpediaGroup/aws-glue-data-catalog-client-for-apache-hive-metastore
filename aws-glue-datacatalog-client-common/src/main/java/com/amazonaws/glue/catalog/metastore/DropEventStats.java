package com.amazonaws.glue.catalog.metastore;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts and timings for the drop events of one {@link DropEventNotifier}: kept as in-process counters and
 * forwarded, with their reason and listener, to {@link DropEventMetrics} (Micrometer when available). Never throws.
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

  /** One call to {@code listener} completed. */
  void delivered(String type, String listener) {
    delivered.incrementAndGet();
    metrics.delivered(type, listener);
  }

  /** {@code count} listener calls failed, or could not be made, for {@code reason}. */
  void failed(String type, String reason, String listener, long count) {
    failed.addAndGet(count);
    metrics.failed(type, reason, listener, count);
  }

  /**
   * A drop's {@code events} were skipped for {@code reason}: the queue was full or shutting down. Returns how many
   * drops have been skipped so far.
   */
  long skipped(String type, String reason, int events) {
    metrics.skipped(type, reason, events);
    return skipped.incrementAndGet();
  }

  /** {@code events} were not sent because of a table filter, for {@code reason}. */
  void filtered(String type, String reason, int events) {
    filtered.addAndGet(events);
    metrics.filtered(type, reason, events);
  }

  /** One call to {@code listener} took {@code nanos}. */
  void listenerCall(String type, String listener, boolean succeeded, long nanos) {
    metrics.listenerCall(type, listener, succeeded, nanos);
  }

  /**
   * Reading a dropped partition's table on the drop call took {@code nanos}. {@code reason} is
   * {@link DropEventMetrics#NONE} on success, otherwise {@link DropEventMetrics#NOT_FOUND} or
   * {@link DropEventMetrics#ERROR}.
   */
  public void tableRead(long nanos, boolean succeeded, String reason) {
    metrics.tableRead(nanos, succeeded, reason);
  }

  /** Listener calls that completed. */
  public long getDeliveredCount() {
    return delivered.get();
  }

  /** Listener calls that threw, or that were not made because the event couldn't be built, queued or completed. */
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
