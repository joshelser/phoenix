/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.metrics;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.query.HBaseFactoryProvider;

/**
 *  This interface is used instead of o.a.h.hbase.metrics.MetricRegistry
 *  to provide backwards compatibility with older HBase versions, which don't
 *  have the native HBase metrics API.
 *
 */
public interface PMetricRegistry {

    // config property
    final String P_METRIC_REGISTRY_ENABLED = "phoenix.metrics.pmetricregistry.enabled";
    final boolean DEFAULT_P_METRIC_REGISTRY_ENABLED = true;

    /** Metric names */
    final String NUM_INDEXES_DEPLOYED_METRIC = "NumIndexesDeployed";
    // metrics relating to all indexing work for a batch mutation
    final String INDEX_WRITE_FAILURE_COUNT_SUFFIX = "_IndexWriteFailureCount";
    final String INDEX_PREPARE_FAILURE_COUNT_SUFFIX = "_IndexPrepareFailureCount";
    final String INDEX_UPDATES_PER_MUTATION_SUFFIX = "_IndexUpdatesPerMutation";
    final String INDEX_TOTAL_UPDATE_COUNT_SUFFIX = "_GlobalIndexUpdateCount";
    final String INDEX_PREPARE_TIME_SUFFIX = "_IndexPrepareTime";
    final String INDEX_WRITE_COMPLETION_TIME_SUFFIX = "_IndexWriteTime";
    // metrics for a single index write
    final String SINGLE_INDEX_UPDATE_WRITE_TIME_SUFFIX = "_SingleIndexUpdateWriteTime";
    final String SINGLE_INDEX_WRITE_FAILURES_SUFFIX = "_SingleIndexWriteFailures";

    class Factory {
        public static PMetricRegistry create(RegionCoprocessorEnvironment env) {
            Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
            boolean registryEnabled = config.getBoolean(P_METRIC_REGISTRY_ENABLED, DEFAULT_P_METRIC_REGISTRY_ENABLED);
            boolean hasNativeHBaseMetrics =
                    VersionUtil.encodeVersion(env.getHBaseVersion()) >= VersionUtil
                            .encodeVersion("1.4");
            return registryEnabled && hasNativeHBaseMetrics ? new HBasePMetricRegistry(env)
                    : new NoopPMetricRegistry();
        }
    }

    /**
     * Incrememt a counter metric
     * @param name name of the metric
     * @param i increment
     */
    void incrementCounter(String name, int i);

    /**
     * Update a timer metric
     * @param name name of the metric
     * @param duration duration
     * @param unit TimeUnit to use
     */
    void updateTimer(String name, long duration, TimeUnit unit);

    /**
     * Update a histogram metric
     * @param name name of the metric
     * @param update update value
     */
    void updateHistogram(String name, int update);
}
