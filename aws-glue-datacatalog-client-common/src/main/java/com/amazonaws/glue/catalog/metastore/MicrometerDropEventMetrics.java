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
 * <p>Meters (every series of a meter has the same tag keys, as Prometheus requires; {@code none} where a tag does
 * not apply):
 * <ul>
 *   <li>{@code glue.drop.events}, counter, tags {@code type} ({@code table}, {@code partition}), {@code result}
 *   ({@code delivered}, {@code failed}, {@code skipped}, {@code filtered}), {@code reason} and {@code listener}
 *   (listener class simple name). {@code delivered} and {@code failed} count listener calls; {@code skipped} and
 *   {@code filtered} count events. Reasons: failed {@code listener_error}, {@code event_error},
 *   {@code queue_error}, {@code table_unavailable}; skipped {@code queue_full}, {@code shutting_down}; filtered
 *   {@code table_parameter_not_true}, {@code iceberg}.</li>
 *   <li>{@code glue.drop.events.listener}, timer, tags {@code type}, {@code listener}, {@code result}
 *   ({@code success}, {@code failure}): one listener call</li>
 *   <li>{@code glue.drop.events.table.read}, timer, tags {@code result} ({@code success}, {@code failure}) and
 *   {@code reason} ({@code none}, {@code not_found}, {@code error}): reading a dropped partition's table on the drop
 *   call, the time it adds to a partition drop</li>
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
  static final String TAG_REASON = "reason";
  static final String TAG_LISTENER = "listener";
  static final String SUCCESS = "success";
  static final String FAILURE = "failure";

  private final MeterRegistry registry;

  static MicrometerDropEventMetrics global() {
    return new MicrometerDropEventMetrics(Metrics.globalRegistry);
  }

  MicrometerDropEventMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  @Override
  public void delivered(String type, String listener) {
    count(type, "delivered", NONE, listener, 1);
  }

  @Override
  public void failed(String type, String reason, String listener, long count) {
    count(type, "failed", reason, listener, count);
  }

  @Override
  public void skipped(String type, String reason, long count) {
    count(type, "skipped", reason, NONE, count);
  }

  @Override
  public void filtered(String type, String reason, long count) {
    count(type, "filtered", reason, NONE, count);
  }

  @Override
  public void listenerCall(String type, String listener, boolean succeeded, long nanos) {
    try {
      registry.timer(LISTENER, TAG_TYPE, type, TAG_LISTENER, orNone(listener), TAG_RESULT,
          succeeded ? SUCCESS : FAILURE).record(nanos, TimeUnit.NANOSECONDS);
    } catch (RuntimeException e) {
      logger.debug("Unable to record " + LISTENER, e);
    }
  }

  @Override
  public void tableRead(long nanos, boolean succeeded, String reason) {
    try {
      registry.timer(TABLE_READ, TAG_RESULT, succeeded ? SUCCESS : FAILURE, TAG_REASON, orNone(reason))
          .record(nanos, TimeUnit.NANOSECONDS);
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

  private void count(String type, String result, String reason, String listener, long count) {
    if (count <= 0) {
      return;
    }
    try {
      registry.counter(EVENTS, TAG_TYPE, type, TAG_RESULT, result, TAG_REASON, orNone(reason),
          TAG_LISTENER, orNone(listener)).increment(count);
    } catch (RuntimeException e) {
      logger.debug("Unable to record " + EVENTS, e);
    }
  }

  private static String orNone(String value) {
    return value == null || value.isEmpty() ? NONE : value;
  }
}
