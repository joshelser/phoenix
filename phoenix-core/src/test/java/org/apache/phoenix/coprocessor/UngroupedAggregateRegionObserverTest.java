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
package org.apache.phoenix.coprocessor;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

public class UngroupedAggregateRegionObserverTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testTimestampNotClearedOnSystemTables() throws IOException {
        UngroupedAggregateRegionObserver obs = mock(UngroupedAggregateRegionObserver.class);
        // Throw an exception if we call this method
        doThrow(new IllegalStateException("Should not have invoked clearTsOnDisabledIndexes"))
            .when(obs).clearTsOnDisabledIndexes(anyString());
        // Call the real postCompact method
        doCallRealMethod().when(obs).postCompact(any(ObserverContext.class), any(Store.class),
            any(StoreFile.class), any(CompactionRequest.class));

        // Make sure that calls to normal tables still go through this method
        try {
            runPostCompactOnTableName(obs, TableName.valueOf("foo"));
            fail("Expected the call to clearTsOnDisabledIndexes to fail");
        } catch (IOException e) {
            // The runAsLoginUser re-wraps our IllegalStateException
            assertTrue(e.getCause() instanceof IllegalStateException);
        }

        // Verify that our system tables do *not*
        List<TableName> tables = Arrays.asList(
            PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME,
            PhoenixDatabaseMetaData.SYSTEM_FUNCTION_HBASE_TABLE_NAME,
            PhoenixDatabaseMetaData.SYSTEM_MUTEX_HBASE_TABLE_NAME,
            PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_HBASE_TABLE_NAME,
            PhoenixDatabaseMetaData.SYSTEM_STATS_HBASE_TABLE_NAME,
            SchemaUtil.getPhysicalTableName(Bytes.toBytes(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME), true),
            SchemaUtil.getPhysicalTableName(Bytes.toBytes(PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME), true),
            SchemaUtil.getPhysicalTableName(Bytes.toBytes(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME), true),
            SchemaUtil.getPhysicalTableName(Bytes.toBytes(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME), true),
            SchemaUtil.getPhysicalTableName(Bytes.toBytes(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME), true));

        for (TableName tn : tables) {
            runPostCompactOnTableName(obs, tn);
        }
    }

    @SuppressWarnings("unchecked")
    private void runPostCompactOnTableName(UngroupedAggregateRegionObserver obs, TableName tn) throws IOException {
        // Mock out objects for postCompact
        ObserverContext<RegionCoprocessorEnvironment> context = mock(ObserverContext.class);
        RegionCoprocessorEnvironment environment = mock(RegionCoprocessorEnvironment.class);
        Region region = mock(Region.class);
        HRegionInfo regionInfo = new HRegionInfo(tn);
        when(context.getEnvironment()).thenReturn(environment);
        when(environment.getRegion()).thenReturn(region);
        when(region.getRegionInfo()).thenReturn(regionInfo);

        CompactionRequest req = new CompactionRequest();
        req.setIsMajor(false, true);

        obs.postCompact(context, null, null, req);
    }
}
