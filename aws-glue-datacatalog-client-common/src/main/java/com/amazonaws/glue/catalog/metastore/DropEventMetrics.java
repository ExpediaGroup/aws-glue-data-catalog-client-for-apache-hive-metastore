package com.amazonaws.glue.catalog.metastore;

import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * Metrics for the drop events sent to {@code aws.glue.event.listeners}.
 *
 * <p>Backed by Micrometer's global registry when the host has Micrometer on its classpath (Waggle Dance, through
 * Spring Boot, adds its registries to the global registry), and a no-op otherwise. Recording a metric never throws.
 */
public interface DropEventMetrics {

  /** Event type tag value for drop table events. */
  String TABLE = "table";
  /** Event type tag value for drop partition events. */
  String PARTITION = "partition";

  /** {@code count} listener calls completed. */
  void delivered(String type, long count);

  /** {@code count} listener calls threw or could not be made. */
  void failed(String type, long count);

  /** {@code count} events skipped because the queue was full or shutting down. */
  void skipped(String type, long count);

  /** {@code count} events not sent because of the table filters. */
  void filtered(String type, long count);

  /** One listener call took {@code nanos}. */
  void listenerCall(String type, long nanos);

  /** Reading a dropped partition's table on the drop call took {@code nanos}. */
  void tableRead(long nanos, boolean succeeded);

  /** Reports the size of the event queue. */
  void queueSize(Collection<?> queue);

  DropEventMetrics NOOP = new DropEventMetrics() {
    @Override
    public void delivered(String type, long count) {
    }

    @Override
    public void failed(String type, long count) {
    }

    @Override
    public void skipped(String type, long count) {
    }

    @Override
    public void filtered(String type, long count) {
    }

    @Override
    public void listenerCall(String type, long nanos) {
    }

    @Override
    public void tableRead(long nanos, boolean succeeded) {
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
