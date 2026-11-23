package com.amazonaws.glue.catalog.metastore;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.ICEBERG;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.LISTENER_ERROR;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.NONE;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.NOT_FOUND;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.PARTITION;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.QUEUE_FULL;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.TABLE;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.TABLE_UNAVAILABLE;
import static com.amazonaws.glue.catalog.metastore.MicrometerDropEventMetrics.EVENTS;
import static com.amazonaws.glue.catalog.metastore.MicrometerDropEventMetrics.LISTENER;
import static com.amazonaws.glue.catalog.metastore.MicrometerDropEventMetrics.QUEUE_SIZE;
import static com.amazonaws.glue.catalog.metastore.MicrometerDropEventMetrics.TABLE_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MicrometerDropEventMetricsTest {

  private static final String SNS = "ApiarySnsListener";

  private MeterRegistry registry;
  private MicrometerDropEventMetrics metrics;

  @Before
  public void setup() {
    registry = new SimpleMeterRegistry();
    metrics = new MicrometerDropEventMetrics(registry);
  }

  @Test
  public void countsEventsByTypeResultReasonAndListener() {
    metrics.delivered(TABLE, SNS);
    metrics.delivered(PARTITION, SNS);
    metrics.delivered(PARTITION, SNS);
    metrics.failed(PARTITION, LISTENER_ERROR, SNS, 1);
    metrics.failed(PARTITION, TABLE_UNAVAILABLE, NONE, 3);
    metrics.skipped(TABLE, QUEUE_FULL, 1);
    metrics.filtered(PARTITION, ICEBERG, 4);

    assertEquals(1.0, count(TABLE, "delivered", NONE, SNS), 0);
    assertEquals(2.0, count(PARTITION, "delivered", NONE, SNS), 0);
    assertEquals(1.0, count(PARTITION, "failed", LISTENER_ERROR, SNS), 0);
    assertEquals(3.0, count(PARTITION, "failed", TABLE_UNAVAILABLE, NONE), 0);
    assertEquals(1.0, count(TABLE, "skipped", QUEUE_FULL, NONE), 0);
    assertEquals(4.0, count(PARTITION, "filtered", ICEBERG, NONE), 0);
  }

  @Test
  public void everyEventSeriesHasTheSameTagKeys() {
    metrics.delivered(TABLE, SNS);
    metrics.failed(PARTITION, TABLE_UNAVAILABLE, null, 1);
    metrics.skipped(TABLE, QUEUE_FULL, 1);
    metrics.filtered(PARTITION, ICEBERG, 1);

    Set<Set<String>> tagKeys = new HashSet<>();
    for (Meter meter : registry.find(EVENTS).meters()) {
      Set<String> keys = new HashSet<>();
      for (Tag tag : meter.getId().getTags()) {
        keys.add(tag.getKey());
      }
      tagKeys.add(keys);
    }
    assertEquals(1, tagKeys.size());
    assertEquals(4, tagKeys.iterator().next().size());
  }

  @Test
  public void zeroCountsCreateNoCounter() {
    metrics.failed(TABLE, LISTENER_ERROR, SNS, 0);

    assertNull(registry.find(EVENTS).tag("result", "failed").counter());
  }

  @Test
  public void timesListenerCallsAndTableReads() {
    metrics.listenerCall(PARTITION, SNS, true, TimeUnit.MILLISECONDS.toNanos(20));
    metrics.listenerCall(PARTITION, SNS, false, TimeUnit.MILLISECONDS.toNanos(5));
    metrics.tableRead(TimeUnit.MILLISECONDS.toNanos(15), true, NONE);
    metrics.tableRead(TimeUnit.MILLISECONDS.toNanos(5), false, NOT_FOUND);

    assertEquals(20.0, registry.get(LISTENER).tag("type", PARTITION).tag("listener", SNS).tag("result", "success")
        .timer().totalTime(TimeUnit.MILLISECONDS), 0.001);
    assertEquals(1, registry.get(LISTENER).tag("result", "failure").timer().count());
    assertEquals(1, registry.get(TABLE_READ).tag("result", "success").tag("reason", NONE).timer().count());
    assertEquals(1, registry.get(TABLE_READ).tag("result", "failure").tag("reason", NOT_FOUND).timer().count());
  }

  @Test
  public void gaugeFollowsTheQueue() {
    List<Runnable> queue = new ArrayList<>();
    metrics.queueSize(queue);
    assertEquals(0.0, registry.get(QUEUE_SIZE).gauge().value(), 0);

    queue.add(new Runnable() {
      @Override
      public void run() {
      }
    });
    assertEquals(1.0, registry.get(QUEUE_SIZE).gauge().value(), 0);
  }

  @Test
  public void createUsesMicrometerWhenAvailable() {
    assertTrue(DropEventMetrics.create() instanceof MicrometerDropEventMetrics);
  }

  private double count(String type, String result, String reason, String listener) {
    return registry.get(EVENTS).tag("type", type).tag("result", result).tag("reason", reason)
        .tag("listener", listener).counter().count();
  }
}
