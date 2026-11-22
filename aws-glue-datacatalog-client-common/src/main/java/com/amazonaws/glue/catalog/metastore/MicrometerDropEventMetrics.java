package com.amazonaws.glue.catalog.metastore;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * {@link DropEventMetrics} on a Micrometer registry, by default the global one.
 *
 * <p>Meters, all tagged {@code type=table|partition} unless noted:
 * <ul>
 *   <li>{@code glue.drop.events}, counter, tagged {@code result=delivered|failed|skipped|filtered}: delivered and
 *   failed count listener calls; skipped and filtered count events</li>
 *   <li>{@code glue.drop.events.listener}, timer: one listener call</li>
 *   <li>{@code glue.drop.events.table.read}, timer, tagged {@code result=success|failure} only: reading a dropped
 *   partition's table on the drop call, the time it adds to a partition drop</li>
 *   <li>{@code glue.drop.events.queue.size}, gauge, no tags: events waiting to be sent</li>
 * </ul>
 */
class MicrometerDropEventMetrics implements DropEventMetrics {

  private static final Logger logger = Logger.getLogger(MicrometerDropEventMetrics.class);

  static final String EVENTS = "glue.drop.events";
  static final String LISTENER = "glue.drop.events.listener";
  static final String TABLE_READ = "glue.drop.events.table.read";
  static final String QUEUE_SIZE = "glue.drop.events.queue.size";
  static final String TAG_TYPE = "type";
  static final String TAG_RESULT = "result";

  private final MeterRegistry registry;

  static MicrometerDropEventMetrics global() {
    return new MicrometerDropEventMetrics(Metrics.globalRegistry);
  }

  MicrometerDropEventMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  @Override
  public void delivered(String type, long count) {
    count(type, "delivered", count);
  }

  @Override
  public void failed(String type, long count) {
    count(type, "failed", count);
  }

  @Override
  public void skipped(String type, long count) {
    count(type, "skipped", count);
  }

  @Override
  public void filtered(String type, long count) {
    count(type, "filtered", count);
  }

  @Override
  public void listenerCall(String type, long nanos) {
    try {
      registry.timer(LISTENER, TAG_TYPE, type).record(nanos, TimeUnit.NANOSECONDS);
    } catch (RuntimeException e) {
      logger.debug("Unable to record " + LISTENER, e);
    }
  }

  @Override
  public void tableRead(long nanos, boolean succeeded) {
    try {
      registry.timer(TABLE_READ, TAG_RESULT, succeeded ? "success" : "failure").record(nanos, TimeUnit.NANOSECONDS);
    } catch (RuntimeException e) {
      logger.debug("Unable to record " + TABLE_READ, e);
    }
  }

  @Override
  public void queueSize(Collection<?> queue) {
    try {
      Gauge.builder(QUEUE_SIZE, queue, Collection::size).register(registry);
    } catch (RuntimeException e) {
      logger.debug("Unable to register " + QUEUE_SIZE, e);
    }
  }

  private void count(String type, String result, long count) {
    if (count <= 0) {
      return;
    }
    try {
      registry.counter(EVENTS, TAG_TYPE, type, TAG_RESULT, result).increment(count);
    } catch (RuntimeException e) {
      logger.debug("Unable to record " + EVENTS, e);
    }
  }
}
