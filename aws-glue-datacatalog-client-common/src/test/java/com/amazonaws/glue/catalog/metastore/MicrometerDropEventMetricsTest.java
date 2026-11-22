package com.amazonaws.glue.catalog.metastore;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.PARTITION;
import static com.amazonaws.glue.catalog.metastore.DropEventMetrics.TABLE;
import static com.amazonaws.glue.catalog.metastore.MicrometerDropEventMetrics.EVENTS;
import static com.amazonaws.glue.catalog.metastore.MicrometerDropEventMetrics.LISTENER;
import static com.amazonaws.glue.catalog.metastore.MicrometerDropEventMetrics.QUEUE_SIZE;
import static com.amazonaws.glue.catalog.metastore.MicrometerDropEventMetrics.TABLE_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MicrometerDropEventMetricsTest {

  private MeterRegistry registry;
  private MicrometerDropEventMetrics metrics;

  @Before
  public void setup() {
    registry = new SimpleMeterRegistry();
    metrics = new MicrometerDropEventMetrics(registry);
  }

  @Test
  public void countsEventsByTypeAndResult() {
    metrics.delivered(TABLE, 1);
    metrics.delivered(PARTITION, 3);
    metrics.failed(PARTITION, 2);
    metrics.skipped(TABLE, 1);
    metrics.filtered(PARTITION, 4);

    assertEquals(1.0, count(TABLE, "delivered"), 0);
    assertEquals(3.0, count(PARTITION, "delivered"), 0);
    assertEquals(2.0, count(PARTITION, "failed"), 0);
    assertEquals(1.0, count(TABLE, "skipped"), 0);
    assertEquals(4.0, count(PARTITION, "filtered"), 0);
  }

  @Test
  public void zeroCountsCreateNoCounter() {
    metrics.failed(TABLE, 0);

    assertNull(registry.find(EVENTS).tag("result", "failed").counter());
  }

  @Test
  public void timesListenerCallsAndTableReads() {
    metrics.listenerCall(PARTITION, TimeUnit.MILLISECONDS.toNanos(20));
    metrics.tableRead(TimeUnit.MILLISECONDS.toNanos(15), true);
    metrics.tableRead(TimeUnit.MILLISECONDS.toNanos(5), false);

    assertEquals(1, registry.get(LISTENER).tag("type", PARTITION).timer().count());
    assertEquals(20.0, registry.get(LISTENER).tag("type", PARTITION).timer().totalTime(TimeUnit.MILLISECONDS), 0.001);
    assertEquals(1, registry.get(TABLE_READ).tag("result", "success").timer().count());
    assertEquals(1, registry.get(TABLE_READ).tag("result", "failure").timer().count());
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

  private double count(String type, String result) {
    return registry.get(EVENTS).tag("type", type).tag("result", result).counter().count();
  }
}
