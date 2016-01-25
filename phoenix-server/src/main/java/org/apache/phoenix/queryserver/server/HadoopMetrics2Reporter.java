/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.queryserver.server;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

/**
 * Dropwizard-Metrics {@link com.codahale.metrics.Reporter} which also acts as a Hadoop Metrics2
 * {@link MetricsSource}.
 */
public class HadoopMetrics2Reporter extends ScheduledReporter implements MetricsSource {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopMetrics2Reporter.class);
  private static final String EMPTY_STRING = "";

  /**
   * Returns a new {@link Builder} for {@link HadoopMetrics2Reporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link HadoopMetrics2Reporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
      return new Builder(registry);
  }

  public static class Builder {
    private final MetricRegistry registry;
    private MetricFilter filter;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.filter = MetricFilter.ALL;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
        this.rateUnit = rateUnit;
        return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
        this.durationUnit = durationUnit;
        return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Builds a {@link HadoopMetrics2Reporter} with the given properties, making metrics available
     * to the Hadoop Metrics2 framework (any configured {@link MetricsSource}s.
     *
     * @return a {@link HadoopMetrics2Reporter}
     */
    public HadoopMetrics2Reporter build(MetricsSystem metrics2System, String name, String description, String recordName, String context) {
        return new HadoopMetrics2Reporter(registry,
                               rateUnit,
                               durationUnit,
                               filter,
                               metrics2System,
                               name,
                               description,
                               recordName,
                               context);
    }
  }

  private final MetricsRegistry metrics2Registry;
  private final MetricsSystem metrics2System;
  private final String recordName;
  private final String context;

  private final ConcurrentLinkedQueue<Entry<String,Gauge>> dropwizardGauges;
  private final ConcurrentLinkedQueue<Entry<String,Counter>> dropwizardCounters;
  //private final ConcurrentLinkedQueue<Entry<String,Histogram>> dropwizardHistograms;
  private final ConcurrentLinkedQueue<Entry<String,Meter>> dropwizardMeters;

  private HadoopMetrics2Reporter(MetricRegistry registry, TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, MetricsSystem metrics2System,
      String name, String description, String recordName, String context) {
    super(registry, "hadoop-metrics2-reporter", filter, rateUnit, durationUnit);
    this.metrics2Registry = new MetricsRegistry(Interns.info(name, description));
    this.metrics2System = metrics2System;
    // Register this source with the Metrics2 system
    this.metrics2System.register(name, description, this);
    this.recordName = recordName;
    this.context = context;

    this.dropwizardGauges = new ConcurrentLinkedQueue<>();
    this.dropwizardCounters = new ConcurrentLinkedQueue<>();
    //this.dropwizardHistograms = new ConcurrentLinkedQueue<>();
    this.dropwizardMeters = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(recordName).setContext(context);

    snapshotAllMetrics(builder);

    metrics2Registry.snapshot(builder, all);
  }

  void snapshotAllMetrics(MetricsRecordBuilder builder) {
    for (Entry<String,Gauge> gauge : dropwizardGauges) {
      final MetricsInfo info = Interns.info(gauge.getKey(), EMPTY_STRING);
      final Object o = gauge.getValue().getValue();

      // Figure out which gauge types metrics2 supports and call the right method
      if (o instanceof Integer) {
        builder.addGauge(info, (int) o);
      } else if (o instanceof Long) {
        builder.addGauge(info, (long) o);
      } else if (o instanceof Float) {
        builder.addGauge(info, (float) o);
      } else if (o instanceof Double) {
        builder.addGauge(info, (double) o);
      } else {
        LOG.trace("Ignoring Gauge ({}) with unhandled type: {}", gauge.getKey(), o.getClass());
      }
    }

    // Pass through the counters
    for (Entry<String,Counter> counter : dropwizardCounters) {
      builder.addCounter(Interns.info(counter.getKey(), EMPTY_STRING),
          counter.getValue().getCount());
    }

    // Pass through the meter values
    for (Entry<String,Meter> meter : dropwizardMeters) {
      builder.addGauge(Interns.info(meter.getKey() + "_mean", EMPTY_STRING),
          meter.getValue().getMeanRate());
      builder.addGauge(Interns.info(meter.getKey() + "_1min", EMPTY_STRING),
          meter.getValue().getOneMinuteRate());
      builder.addGauge(Interns.info(meter.getKey() + "_5min", EMPTY_STRING),
          meter.getValue().getFiveMinuteRate());
      builder.addGauge(Interns.info(meter.getKey() + "_15min", EMPTY_STRING),
          meter.getValue().getFifteenMinuteRate());
    }
  }

  @Override
  public void report(SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters, SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters,
      SortedMap<String,Timer> timers) {
    for (Entry<String,Gauge> gauge : gauges.entrySet()) {
      dropwizardGauges.add(gauge);
    }

    for (Entry<String,Counter> counter : counters.entrySet()) {
      dropwizardCounters.add(counter);
    }

    // TODO Figure out how to handle histograms.
    //for (Entry<String,Histogram> histogram : histograms.entrySet()) {
    //  dropwizardHistograms.add(histogram);
    //}

    for (Entry<String,Meter> meter : meters.entrySet()) {
      dropwizardMeters.add(meter);
    }
  }

}
