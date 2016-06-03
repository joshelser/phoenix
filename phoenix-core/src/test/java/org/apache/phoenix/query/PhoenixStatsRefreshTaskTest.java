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
package org.apache.phoenix.query;

import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Timer;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.ConnectionQueryServicesImpl.PhoenixStatsRefreshTask;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.stats.PTableStats;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for the asynchronous Phoenix stats refreshing
 */
@SuppressWarnings("deprecation")
public class PhoenixStatsRefreshTaskTest {

    private ConnectionQueryServicesImpl queryServices;
    private HTableInterface statsTable;
    private Timer timer;

    private PhoenixStatsRefreshTask refreshTask;

    @Before
    public void setup() throws IOException {
        this.queryServices = mock(ConnectionQueryServicesImpl.class);
        this.statsTable = mock(HTableInterface.class);
        this.timer = mock(Timer.class);
        this.refreshTask = mock(PhoenixStatsRefreshTask.class);

        doCallRealMethod().when(refreshTask).run();
        when(refreshTask.getStatsTable()).thenReturn(statsTable);
        when(refreshTask.getTimer()).thenReturn(timer);
        when(refreshTask.getQueryServices()).thenReturn(queryServices);
    }

    private PTable mockPTable(byte[] tableName) {
        PTable table = mock(PTable.class);
        PName name = mock(PName.class);
        // Mock out a PTable with a given name
        when(table.getName()).thenReturn(name);
        when(name.getBytes()).thenReturn(tableName);
        return table;
    }
    
    @Test
    public void failedStatsComputationsAreNotCached() throws Exception {
        byte[] tableName = Bytes.toBytes("table_with_failed_stats_lookup");
        PTable table = mockPTable(tableName);

        // Throw an exception trying to read statistics for that table
        when(refreshTask.readStatistics(eq(statsTable), aryEq(tableName), eq(Long.MAX_VALUE)))
            .thenThrow(new IOException("Stats lookup failed!"));

        // Set the list of phoenix tables
        when(refreshTask.getPhoenixTables()).thenReturn(Arrays.asList(table));

        // Invoke the stats refresh task
        refreshTask.run();

        // Verify nothing was asked to be cached
        verify(refreshTask, never()).cacheTableStats(any(byte[].class), any(PTableStats.class));
    }

    @Test
    public void taskRequeuesItself() {
        final long refreshPeriod = QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS;

        // No tables to process
        when(refreshTask.getPhoenixTables()).thenReturn(Collections.<PTable> emptyList());

        // Return the default refresh period
        when(queryServices.getStatsRefreshPeriod()).thenReturn(refreshPeriod);

        // Call the real requeueTask() method
        doCallRealMethod().when(refreshTask).requeueTask();

        // Run the task
        refreshTask.run();

        // The task should requeue itself with the time after
        verify(refreshTask).requeueTask();
        verify(timer).schedule(refreshTask, refreshPeriod);
    }

    @Test
    public void testFailedStatsLookupOnOneTableDoesNotBlockOther() throws Exception {
      final byte[] tableName1 = Bytes.toBytes("table_with_failed_stats_lookup");
        final byte[] tableName2 = Bytes.toBytes("table_with_passing_stats_lookup");
        final PTable table1 = mockPTable(tableName1);
        final PTable table2 = mockPTable(tableName2);

        PTableStats table2Stats = mock(PTableStats.class);

        // Throw an exception trying to read statistics for table1
        when(refreshTask.readStatistics(eq(statsTable), aryEq(tableName1), eq(Long.MAX_VALUE)))
            .thenThrow(new IOException("Stats lookup failed!"));
        // Return some stats for table2
        when(refreshTask.readStatistics(eq(statsTable), aryEq(tableName2), eq(Long.MAX_VALUE)))
            .thenReturn(table2Stats);

        // Set the list of phoenix tables
        when(refreshTask.getPhoenixTables()).thenReturn(Arrays.asList(table1, table2));

        // Invoke the stats refresh task
        refreshTask.run();

        // Verify tableName1 is not cached
        verify(refreshTask, never()).cacheTableStats(aryEq(tableName1), any(PTableStats.class));
        // Verify tableName2 is cache
        verify(refreshTask).cacheTableStats(aryEq(tableName2), eq(table2Stats));
    }

    @Test
    public void failureToGetStatsHTableRequeues() throws Exception {
        final long refreshPeriod = QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS;
        final byte[] tableName1 = Bytes.toBytes("table1");
        final byte[] tableName2 = Bytes.toBytes("table2");
        final PTable table1 = mockPTable(tableName1);
        final PTable table2 = mockPTable(tableName2);

        // Fail to get the HTable for the stats table
        when(refreshTask.getStatsTable()).thenThrow(new IOException("Failed to get HTable"));

        // Shouldn't be invoked, but providing records to prove it isn't called
        when(refreshTask.getPhoenixTables()).thenReturn(Arrays.asList(table1, table2));

        // Return the default refresh period
        when(queryServices.getStatsRefreshPeriod()).thenReturn(refreshPeriod);

        // Call the real requeueTask() method
        doCallRealMethod().when(refreshTask).requeueTask();

        // Invoke the stats refresh task
        refreshTask.run();

        // The task should be requeued and scheduled on the timer
        verify(refreshTask).requeueTask();
        verify(timer).schedule(refreshTask, refreshPeriod);
    }
}
