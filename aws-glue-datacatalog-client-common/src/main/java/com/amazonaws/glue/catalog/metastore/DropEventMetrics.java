package com.amazonaws.glue.catalog.metastore;

import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * Metrics for the drop events sent to {@code aws.glue.event.listeners}.
 *
 * <p>Backed by Micrometer's global registry when the host has Micrometer on its classpath (Waggle Dance, through
 * Spring Boot, adds its registries to the global registry), and a no-op otherwise. Recording a metric never throws.
 * Tag values are fixed constants or listener class names, so the number of series stays small.
 */
public interface DropEventMetrics {

  /** Event type tag value for drop table events. */
  String TABLE = "table";
  /** Event type tag value for drop partition events. */
  String PARTITION = "partition";

  /** Tag value when a reason or listener does not apply. */
  String NONE = "none";

  /** Failed: a listener threw. */
  String LISTENER_ERROR = "listener_error";
  /** Failed: the Hive event could not be built. */
  String EVENT_ERROR = "event_error";
  /** Failed: an unexpected error while queuing the event. */
  String QUEUE_ERROR = "queue_error";
  /** Failed: a dropped partition's table could not be read on the drop call. */
  String TABLE_UNAVAILABLE = "table_unavailable";

  /** Skipped: the queue was full. */
  String QUEUE_FULL = "queue_full";
  /** Skipped: the queue was shutting down. */
  String SHUTTING_DOWN = "shutting_down";

  /** Filtered: the table's required parameter was not {@code true}. */
  String TABLE_PARAMETER_NOT_TRUE = "table_parameter_not_true";
  /** Filtered: the table is an Iceberg table. */
  String ICEBERG = "iceberg";

  /** Table read failed: the table no longer exists. */
  String NOT_FOUND = "not_found";
  /** Table read failed: any other error. */
  String ERROR = "error";

  /** One listener call completed. */
  void delivered(String type, String listener);

  /** {@code count} listener calls failed, or could not be made, for {@code reason}. */
  void failed(String type, String reason, String listener, long count);

  /** {@code count} events skipped for {@code reason} ({@link #QUEUE_FULL}, {@link #SHUTTING_DOWN}). */
  void skipped(String type, String reason, long count);

  /** {@code count} events not sent because of a table filter ({@link #TABLE_PARAMETER_NOT_TRUE}, {@link #ICEBERG}). */
  void filtered(String type, String reason, long count);

  /** One listener call took {@code nanos}. */
  void listenerCall(String type, String listener, boolean succeeded, long nanos);

  /**
   * Reading a dropped partition's table on the drop call took {@code nanos}. {@code reason} is {@link #NONE} on
   * success, otherwise {@link #NOT_FOUND} or {@link #ERROR}.
   */
  void tableRead(long nanos, boolean succeeded, String reason);

  /** Reports the size of the event queue. */
  void queueSize(Collection<?> queue);

  DropEventMetrics NOOP = new DropEventMetrics() {
    @Override
    public void delivered(String type, String listener) {
    }

    @Override
    public void failed(String type, String reason, String listener, long count) {
    }

    @Override
    public void skipped(String type, String reason, long count) {
    }

    @Override
    public void filtered(String type, String reason, long count) {
    }

    @Override
    public void listenerCall(String type, String listener, boolean succeeded, long nanos) {
    }

    @Override
    public void tableRead(long nanos, boolean succeeded, String reason) {
    }

    @Override
    public void queueSize(Collection<?> queue) {
    }
  };

  /** Micrometer-backed metrics if Micrometer is available, otherwise {@link #NOOP}. */
  static DropEventMetrics create() {
    try {
      return MicrometerDropEventMetrics.global();
    } catch (LinkageError | RuntimeException e) {
      Logger.getLogger(DropEventMetrics.class)
          .info("Micrometer is not available; drop event metrics are off (" + e + ")");
      return NOOP;
    }
  }
}
