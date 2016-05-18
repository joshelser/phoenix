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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl.StatsUpdateTask;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test cases for the StatsUpdateTask.
 */
public class StatsUpdateTaskTest {
  
  @Test
  public void testPhoenixSystemTablesAreIgnoredForStatsCollection() {
    List<String> tablesToIgnore = Arrays.asList(
        PhoenixDatabaseMetaData.SYSTEM_CATALOG,
        PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME,
        PhoenixDatabaseMetaData.SYSTEM_SEQUENCE,
        PhoenixDatabaseMetaData.SYSTEM_STATS_NAME);

    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    StatsUpdateTask task = new StatsUpdateTask(env);

    for (String tableToIgnore : tablesToIgnore) {
        Assert.assertFalse("Should not collect stats for " + tableToIgnore,
            task.shouldCollectStatsForTable(Bytes.toBytes(tableToIgnore)));
    }
  }
  
  @Test
  public void testUserPhoenixTablesShouldHaveStatsCollected() {
    List<String> tablesToIgnore = Arrays.asList(
        "FOO",
        "SYSTEMTHING.FOO",
        "SYS",
        "SYSTE.FOO",
        "SYSTEM");

    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    StatsUpdateTask task = new StatsUpdateTask(env);

    for (String tableToIgnore : tablesToIgnore) {
        Assert.assertTrue("Should not collect stats for " + tableToIgnore,
            task.shouldCollectStatsForTable(Bytes.toBytes(tableToIgnore)));
    }
  }
  
}
