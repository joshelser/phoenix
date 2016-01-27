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

import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;

/**
 * An extension of {@link Configured} which can pass through an {@link MetricRegistry}.
 */
public class ConfiguredWithMetrics extends Configured {

  private final Optional<MetricRegistry> metrics;

  /** Construct a Configured. */
  public ConfiguredWithMetrics() {
    super(null);
    this.metrics = Optional.absent();
  }

  /**
   * Construct a Configured with the given {@code metrics}.
   *
   * @param metrics A non-null {@link MetricRegistry}.
   */
  public ConfiguredWithMetrics(MetricRegistry metrics) {
    super(null);
    this.metrics = Optional.of(Objects.requireNonNull(metrics));
  }
  
  /** Construct a Configured. */
  public ConfiguredWithMetrics(Configuration conf) {
    super(conf);
    this.metrics = Optional.absent();
  }

  /**
   * Construct a Configured with the given {@code metrics}.
   *
   * @param conf The Configuration.
   * @param metrics A non-null {@link MetricRegistry}.
   */
  public ConfiguredWithMetrics(Configuration conf, MetricRegistry metrics) {
    super(conf);
    this.metrics = Optional.of(Objects.requireNonNull(metrics));
  }

  /**
   * @return True if an instance of {@link MetricRegistry} was provided.
   */
  public boolean hasMetrics() {
    return this.metrics.isPresent();
  }

  /**
   * @return The {@link MetricRegistry} instance, may be null.
   * @see #hasMetrics()
   */
  public MetricRegistry getMetrics() {
    return metrics.orNull();
  }
}
