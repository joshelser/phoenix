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
package org.apache.phoenix.end2end;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.*;

@Category(NeedsOwnMiniClusterTest.class)
public class CompactionDeadlockOnSystemNamespaceIT extends BaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(
        CompactionDeadlockOnSystemNamespaceIT.class);

    private static final Set<String> PHOENIX_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
            "SYSTEM.CATALOG", "SYSTEM.SEQUENCE", "SYSTEM.STATS", "SYSTEM.FUNCTION",
            "SYSTEM.MUTEX"));
    private static final Set<String> PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES = new HashSet<>(
            Arrays.asList("SYSTEM:CATALOG", "SYSTEM:SEQUENCE", "SYSTEM:STATS", "SYSTEM:FUNCTION",
                    "SYSTEM:MUTEX"));
    private static final String SCHEMA_NAME = "MIGRATETEST";
    private static final String TABLE_NAME =
            SCHEMA_NAME + "." + CompactionDeadlockOnSystemNamespaceIT.class.getSimpleName().toUpperCase();
    private static final int NUM_RECORDS = 5;

    private HBaseTestingUtility testUtil = null;
    private Set<String> hbaseTables;

    // Create Multiple users since Phoenix caches the connection per user
    // Migration or upgrade code will run every time for each user.
    final UserGroupInformation user1 =
            UserGroupInformation.createUserForTesting("user1", new String[0]);
    final UserGroupInformation user2 =
            UserGroupInformation.createUserForTesting("user2", new String[0]);
    final UserGroupInformation user3 =
            UserGroupInformation.createUserForTesting("user3", new String[0]);
    final UserGroupInformation user4 =
            UserGroupInformation.createUserForTesting("user4", new String[0]);

    @After
    public void tearDownMiniCluster() {
        try {
            if (testUtil != null) {
                testUtil.shutdownMiniCluster();
                testUtil = null;
            }
        } catch (Exception e) {
            // ignore
        }
    }

    @Test(timeout = 120 * 1000)
    public void testDeadlockOnCompaction() throws Exception {
        testUtil = new HBaseTestingUtility();
        Configuration conf = testUtil.getConfiguration();
        configureRandomHMasterPort(conf);
        disableNamespacesOnServer(conf);
        testUtil.startMiniCluster(1);

        // Force system tables to be created
        user1.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createConnection(getClientPropertiesWithSystemMappingDisabled());
                return null;
            }
        });

        // Sanity check
        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.containsAll(PHOENIX_SYSTEM_TABLES));

        // There should be one file for system.catalog
        testUtil.flush(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
        assertEquals(1, testUtil.getNumHFiles(
            PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME,
            QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES));

        // Create another table to get some more mutations written to system.catalog
        user1.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createTable(getClientPropertiesWithSystemMappingDisabled());
                return null;
            }
        });

        // There should be two files for system.catalog now
        testUtil.flush(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
        assertEquals(2, testUtil.getNumHFiles(
            PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME,
            QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES));

        testUtil.shutdownMiniHBaseCluster();
        enableNamespacesOnServer(conf);
        testUtil.startMiniHBaseCluster(1, 1);

        // This should not hang.
        LOG.info("Starting compaction");
        testUtil.compact(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME, true);
        LOG.info("Compaction finished");

        // System tables should already be migrated, but just to be sure.
        user1.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createConnection(getClientPropertiesWithSystemMappingEnabled());
                return null;
            }
        });

        // Make sure the mapped tables are there.
        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));
    }


    private void enableNamespacesOnServer(Configuration conf) {
        conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
    }

    // For PHOENIX-4389 (Flapping tests SystemTablePermissionsIT and MigrateSystemTablesToSystemNamespaceIT)
    private void configureRandomHMasterPort(Configuration conf) {
        // Avoid multiple clusters trying to bind the master's info port (16010)
        conf.setInt(HConstants.MASTER_INFO_PORT, -1);
    }

    private Properties getClientPropertiesWithSystemMappingEnabled() {
        Properties clientProps = new Properties();
        clientProps.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        clientProps.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.TRUE.toString());
        return clientProps;
    }

    private void disableNamespacesOnServer(Configuration conf) {
      conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
      conf.set(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.FALSE.toString());
    }

    private Properties getClientPropertiesWithSystemMappingDisabled() {
        Properties clientProps = new Properties();
        clientProps.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        clientProps.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.FALSE.toString());
        return clientProps;
    }

    private Set<String> getHBaseTables() throws IOException {
        Set<String> tables = new HashSet<>();
        for (TableName tn : testUtil.getHBaseAdmin().listTableNames()) {
            tables.add(tn.getNameAsString());
        }
        return tables;
    }

    private void createConnection(Properties clientProps) throws SQLException, IOException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), clientProps);
             Statement stmt = conn.createStatement();) {
            verifySyscatData(clientProps, conn.toString(), stmt);
        }
    }

    private void createTable(Properties clientProps) throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), clientProps);
             Statement stmt = conn.createStatement();) {
            assertFalse(stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME));
            stmt.execute("CREATE SCHEMA " + SCHEMA_NAME);
            assertFalse(stmt.execute("CREATE TABLE " + TABLE_NAME
                    + "(pk INTEGER not null primary key, data VARCHAR)"));
            try (PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO "
                    + TABLE_NAME + " values(?, ?)")) {
                for (int i = 0; i < NUM_RECORDS; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, Integer.toString(i));
                    assertEquals(1, pstmt.executeUpdate());
                }
            }
            conn.commit();
        }
    }

    private void verifySyscatData(Properties clientProps, String connName, Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM SYSTEM.CATALOG");

        @SuppressWarnings({"unchecked", "rawtypes"})
        Map<String,String> data = (Map) clientProps;
        ReadOnlyProps props = new ReadOnlyProps(data);
        boolean systemTablesMapped = SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, props);
        boolean systemSchemaExists = false;
        Set<String> namespaceMappedSystemTablesSet = new HashSet<>(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES);
        Set<String> systemTablesSet = new HashSet<>(PHOENIX_SYSTEM_TABLES);

        while(rs.next()) {

            if(rs.getString("IS_NAMESPACE_MAPPED") == null) {
                systemSchemaExists = rs.getString("TABLE_SCHEM").equals(PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME) ? true : systemSchemaExists;
            } else if (rs.getString("COLUMN_NAME") == null) {
                String schemaName = rs.getString("TABLE_SCHEM");
                String tableName = rs.getString("TABLE_NAME");

                LOG.info("SchemaName: {}, TableName: {}", schemaName, tableName);
                if(schemaName.equals(PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME)) {
                    if (systemTablesMapped) {
                        namespaceMappedSystemTablesSet.remove(String.valueOf
                                (TableName.valueOf(schemaName + QueryConstants.NAMESPACE_SEPARATOR + tableName)));
                        assertEquals(Boolean.TRUE.toString(), rs.getString("IS_NAMESPACE_MAPPED"));
                    } else {
                        systemTablesSet.remove(String.valueOf
                                (TableName.valueOf(schemaName + QueryConstants.NAME_SEPARATOR + tableName)));
                        assertEquals(Boolean.FALSE.toString(), rs.getString("IS_NAMESPACE_MAPPED"));
                    }
                }
            }
        }

        if(!systemSchemaExists) {
            fail(PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME + " entry doesn't exist in SYSTEM.CATALOG table.");
        }

        // The set will contain SYSMUTEX table since that table is not exposed in SYSCAT
        if (systemTablesMapped) {
            assertTrue(namespaceMappedSystemTablesSet.size() == 1);
        } else {
            assertTrue(systemTablesSet.size() == 1);
        }
    }

    private String getJdbcUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

}
