/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.iterate.NonAggregateRegionScannerFactory;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.function.ArrayIndexFunction;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.iterate.OffsetResultIterator;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.RegionScannerResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.metrics.PMetricRegistry;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.tephra.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/**
 *
 * Wraps the scan performing a non aggregate query to prevent needless retries
 * if a Phoenix bug is encountered from our custom filter expression evaluation.
 * Unfortunately, until HBASE-7481 gets fixed, there's no way to do this from our
 * custom filters.
 *
 *
 * @since 0.1
 */
public class ScanRegionObserver extends BaseScannerRegionObserver {
    private PMetricRegistry pMetricRegistry;
    private static final Logger logger = LoggerFactory
            .getLogger(ScanRegionObserver.class);

    public static void serializeIntoScan(Scan scan, int thresholdBytes, int limit, List<OrderByExpression> orderByExpressions, int estimatedRowSize) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(); // TODO: size?
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, thresholdBytes);
            WritableUtils.writeVInt(output, limit);
            WritableUtils.writeVInt(output, estimatedRowSize);
            WritableUtils.writeVInt(output, orderByExpressions.size());
            for (OrderByExpression orderingCol : orderByExpressions) {
                orderingCol.write(output);
            }
            scan.setAttribute(BaseScannerRegionObserver.TOPN, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        pMetricRegistry = PMetricRegistry.Factory.create((RegionCoprocessorEnvironment) e);
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        super.postOpen(e);
        // maintain a metric of # of indexes deployed on this regionserver
        RegionCoprocessorEnvironment env = e.getEnvironment();
        // if this is an index table, it will have priority set
        HTableDescriptor tableDesc = env.getRegion().getTableDesc();
        if (tableDesc.getValue(QueryConstants.PRIORITY) != null) {
            ConcurrentMap<String,Object> sharedData = env.getSharedData();
            // maintain a count of # of regions so we can decrement the metric appropriately at region close
            AtomicInteger prior = (AtomicInteger) sharedData.putIfAbsent("metric" + tableDesc.getNameAsString(), new AtomicInteger(1));
            // we only want to increment the counter once per table
            if (prior == null) {
                pMetricRegistry.incrementCounter(PMetricRegistry.NUM_INDEXES_DEPLOYED_METRIC, 1);
            } else {
                prior.incrementAndGet();
            }
        }
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
        super.postClose(e, abortRequested);
        // decrement the metric of # of indexes deployed on this regionserver
        RegionCoprocessorEnvironment env = e.getEnvironment();
        HTableDescriptor tableDesc = env.getRegion().getTableDesc();
        if (tableDesc.getValue(QueryConstants.PRIORITY) != null) {
            ConcurrentMap<String,Object> sharedData = env.getSharedData();
            AtomicInteger count = (AtomicInteger) sharedData.get("metric" + tableDesc.getNameAsString());
            if (count == null) { // shouldn't happen
                logger.warn("Didn't find index metric while closing region: " + tableDesc.getNameAsString());
                return;
            }
            int currCount = count.decrementAndGet();
            if (currCount == 0) {
                pMetricRegistry.incrementCounter(PMetricRegistry.NUM_INDEXES_DEPLOYED_METRIC, -1);
            }
        }
    }

    @Override
    protected RegionScanner doPostScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan, final RegionScanner s) throws Throwable {
        NonAggregateRegionScannerFactory nonAggregateROUtil = new NonAggregateRegionScannerFactory(c.getEnvironment(), useNewValueColumnQualifier, encodingScheme);
        return nonAggregateROUtil.getRegionScanner(scan, s);
    }

    @Override
    protected boolean isRegionObserverFor(Scan scan) {
        return scan.getAttribute(BaseScannerRegionObserver.NON_AGGREGATE_QUERY) != null;
    }

}
