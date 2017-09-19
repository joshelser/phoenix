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

import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.BaseMasterAndRegionObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.AuthResult;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost.PhoenixMetaDataControllerEnvironment;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;

import com.google.common.collect.Lists;
import com.google.protobuf.RpcCallback;

public class PhoenixAccessController extends BaseMetaDataEndpointObserver {

    private PhoenixMetaDataControllerEnvironment env;
    private ArrayList<BaseMasterAndRegionObserver> accessControllers;
    private boolean accessCheckEnabled;
    private UserProvider userProvider;
    public static final Log LOG = LogFactory.getLog(PhoenixAccessController.class);
    private static final Log AUDITLOG =
            LogFactory.getLog("SecurityLogger."+PhoenixAccessController.class.getName());

    private List<BaseMasterAndRegionObserver> getAccessControllers() throws IOException {
        if (accessControllers == null) {
            synchronized (this) {
                if (accessControllers == null) {
                    accessControllers = new ArrayList<BaseMasterAndRegionObserver>();
                    RegionCoprocessorHost cpHost = this.env.getCoprocessorHost();
                    List<BaseMasterAndRegionObserver> coprocessors = cpHost
                            .findCoprocessors(BaseMasterAndRegionObserver.class);
                    for (BaseMasterAndRegionObserver cp : coprocessors) {
                        if (cp instanceof AccessControlService.Interface) {
                            accessControllers.add(cp);
                        }
                    }
                }
            }
        }
        return accessControllers;
    }

    @Override
    public void preGetTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName) throws IOException {
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preGetTableDescriptors(new ObserverContext<MasterCoprocessorEnvironment>(),
                    Lists.newArrayList(physicalTableName), Collections.<HTableDescriptor> emptyList());
        }
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        this.accessCheckEnabled = env.getConfiguration().getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
                QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
        if (!this.accessCheckEnabled) {
            LOG.warn("PhoenixAccessController has been loaded with authorization checks disabled.");
        }
        if (env instanceof PhoenixMetaDataControllerEnvironment) {
            this.env = (PhoenixMetaDataControllerEnvironment)env;
        } else {
            throw new IllegalArgumentException(
                    "Not a valid environment, should be loaded by PhoenixMetaDataControllerEnvironment");
        }
        // set the user-provider.
        this.userProvider = UserProvider.instantiate(env.getConfiguration());


    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {}

    @Override
    public void preCreateTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType,
            Set<byte[]> familySet) throws IOException {
        if (!accessCheckEnabled) { return; }
        final HTableDescriptor htd = new HTableDescriptor(physicalTableName);
        for (byte[] familyName : familySet) {
            htd.addFamily(new HColumnDescriptor(familyName));
        }
        if (tableType != PTableType.VIEW) {
            for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
                observer.preCreateTable(new ObserverContext<MasterCoprocessorEnvironment>(), htd, null);
            }
        }
        //Index and view require read access on parent physical table.
        if (tableType == PTableType.VIEW || tableType == PTableType.INDEX) {
            requireAccess("Create"+tableType, parentPhysicalTableName, Action.READ, Action.EXEC);
        }
        if (tableType == PTableType.VIEW) {
            //TODO: should we grant this user a read access to all indexes of the base table?
        }
        
        if (tableType == PTableType.INDEX) {
          // All the users who have READ access on data table should have access to Index table as well.
          // WRITE is needed for the index updates done by the user who has WRITE access on data table. 
          // CREATE is needed during the drop of the table.  
          // We are doing this because existing user while querying data table should not see access denied for the new indexes.
          //TODO: confirm whether granting permission from coprocessor is a security leak.  
            grantAccessToUsers("Create" + tableType, parentPhysicalTableName,
                    Arrays.asList(Action.READ, Action.WRITE, Action.CREATE, Action.ADMIN), physicalTableName,
                    Action.READ, Action.EXEC);
        }

    }

    private void grantAccessToUsers(String string, final TableName fromTable, final List<Action> requiredActionsOnTable,
            final TableName toTable, final Action... permissions) throws IOException {
        AUDITLOG.info("Granting " + permissions + " to users on " + toTable + " who has " + requiredActionsOnTable
                + " on table " + fromTable);
        User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try (Connection conn = ConnectionFactory.createConnection(env.getConfiguration())) {
                    List<UserPermission> userPermissions = getUserPermissions(fromTable.getNameAsString());
                    List<UserPermission> permissionsOnTheTable = getUserPermissions(fromTable.getNameAsString());
                    if (userPermissions != null) {
                        for (UserPermission userPermission : userPermissions) {
                            for (Action action : requiredActionsOnTable) {

                                if (userPermission.implies(action)) {
                                    List<UserPermission> permsToTable = getPermissionForUser(permissionsOnTheTable,
                                            userPermission.getUser());
                                    if (permsToTable == null) {
                                        AccessControlClient.grant(conn, toTable,
                                                Bytes.toString(userPermission.getUser()), null, null, action);
                                    } else {
                                        for (UserPermission permToTable : permsToTable) {
                                            if (!permToTable.implies(action)) {
                                                AccessControlClient
                                                        .grant(conn, toTable, Bytes.toString(userPermission.getUser()),
                                                                null, null,
                                                                /*
                                                                 * We need to append the new role and write all access
                                                                 * as ACLs are immutable
                                                                 */(Action[])Arrays
                                                                        .asList(permToTable.getActions(), action)
                                                                        .toArray());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Throwable e) {
                    new Exception(e);
                }
                return null;
            }
        });
    }
    
    private List<UserPermission> getPermissionForUser(List<UserPermission> perms, byte[] user) {
        if (perms != null) {
            // get list of permissions for th user as multiple implementation of AccessControl coprocessors can give
            // permissions for same users
            List<UserPermission> permissions=new ArrayList<UserPermission>();
            for (UserPermission p : perms) {
                if (Bytes.equals(p.getUser(),user)){
                     permissions.add(p);
                }
            }
            if(!permissions.isEmpty()){
               return permissions;
            }
        }
        return null;
    }

    @Override
    public void preDropTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType,
            List<PTable> indexes) throws IOException {
        if (!accessCheckEnabled) { return; }

        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            if (tableType != PTableType.VIEW) {
                observer.preDeleteTable(new ObserverContext<MasterCoprocessorEnvironment>(), physicalTableName);
            }
            if (indexes != null) {
                for (PTable index : indexes) {
                    observer.preDeleteTable(new ObserverContext<MasterCoprocessorEnvironment>(),
                            TableName.valueOf(index.getPhysicalName().getBytes()));
                }
            }
        }
        //checking similar permission checked during the create of the view.
        if (tableType == PTableType.VIEW || tableType == PTableType.INDEX) {
            requireAccess("Drop "+tableType, parentPhysicalTableName, Action.READ, Action.EXEC);
        }

    }

    @Override
    public void preAlterTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType) throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            if (tableType != PTableType.VIEW) {
            observer.preModifyTable(new ObserverContext<MasterCoprocessorEnvironment>(), physicalTableName,
                    new HTableDescriptor(physicalTableName));
            }
        }
        if (tableType == PTableType.VIEW) {
            requireAccess("Alter "+tableType, parentPhysicalTableName, Action.READ, Action.EXEC);
        }
    }

    @Override
    public void preGetSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preListNamespaceDescriptors(new ObserverContext<MasterCoprocessorEnvironment>(),
                    Arrays.asList(NamespaceDescriptor.create(schemaName).build()));
        }

    }

    @Override
    public void preCreateSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preCreateNamespace(new ObserverContext<MasterCoprocessorEnvironment>(),
                    NamespaceDescriptor.create(schemaName).build());
        }

    }

    @Override
    public void preDropSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preDeleteNamespace(new ObserverContext<MasterCoprocessorEnvironment>(), schemaName);
        }
    }

    @Override
    public void preIndexUpdate(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String indexName, TableName physicalTableName, TableName parentPhysicalTableName, PIndexState newState)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preModifyTable(new ObserverContext<MasterCoprocessorEnvironment>(), physicalTableName,
                    new HTableDescriptor(physicalTableName));
        }
        // Check for read access in case of rebuild
        if (newState == PIndexState.BUILDING) {
            requireAccess("Rebuild:", parentPhysicalTableName, Action.READ, Action.EXEC);
        }
    }
    
    private List<UserPermission> getUserPermissions(final String tableName) throws IOException {
        return User.runAsLoginUser(new PrivilegedExceptionAction<List<UserPermission>>() {
            @Override
            public List<UserPermission> run() throws Exception {
                final List<UserPermission> userPermissions = new ArrayList<UserPermission>();
                try (Connection connection = ConnectionFactory.createConnection(env.getConfiguration())) {
                    for (BaseMasterAndRegionObserver service : accessControllers) {
                        if (service.getClass().getName().equals(org.apache.hadoop.hbase.security.access.AccessController.class.getName())) {
                            userPermissions.addAll(AccessControlClient.getUserPermissions(connection, tableName));
                        } else {
                            AccessControlProtos.GetUserPermissionsRequest.Builder builder = AccessControlProtos.GetUserPermissionsRequest
                                    .newBuilder();
                            builder.setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf(tableName)));
                            builder.setType(AccessControlProtos.Permission.Type.Table);
                            AccessControlProtos.GetUserPermissionsRequest request = builder.build();

                            PayloadCarryingRpcController controller = ((ClusterConnection)connection)
                                    .getRpcControllerFactory().newController();
                            ((AccessControlService.Interface)service).getUserPermissions(controller, request,
                                    new RpcCallback<AccessControlProtos.GetUserPermissionsResponse>() {
                                        @Override
                                        public void run(AccessControlProtos.GetUserPermissionsResponse message) {
                                            if (message != null) {
                                                for (AccessControlProtos.UserPermission perm : message
                                                        .getUserPermissionList()) {
                                                    userPermissions.add(ProtobufUtil.toUserPermission(perm));
                                                }
                                            }
                                        }
                                    });
                        }
                    }
                } catch (Throwable e) {
                    throw new Exception(e);
                }
                return userPermissions;
            }
        });
    }
    
    private void requireAccess(String request, TableName tableName, Action... permissions) throws IOException {
        User user = getActiveUser();
        AuthResult result = null;

        for (Action permission : permissions) {
            if (hasAccess(getUserPermissions(tableName.getNameAsString()), tableName, permission, user)) {
                result = AuthResult.allow(request, "Table permission granted", user, permission, tableName, null, null);
                break;
            } else {
                // rest of the world
                result = AuthResult.deny(request, "Insufficient permissions", user, permission, tableName, null, null);
            }
        }
        logResult(result);
        if (!result.isAllowed()) { throw new AccessDeniedException(
                "Insufficient permissions " + result.toContextString()); }
    }

    private boolean hasAccess(List<UserPermission> perms, TableName table, Permission.Action action, User user) {
        if (perms != null) {
            List<UserPermission> permissionsForUser = getPermissionForUser(perms, user.getName().getBytes());
            if (permissionsForUser != null) for (UserPermission permissionForUser : permissionsForUser) {
                if (permissionForUser.implies(action)) { return true; }
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("No permissions found for table=" + table);
        }
        return false;
    }

    private User getActiveUser() throws IOException {
        User user = RpcServer.getRequestUser();
        if (user == null) {
            // for non-rpc handling, fallback to system user
            user = userProvider.getCurrent();
        }
        return user;
    }

    private void logResult(AuthResult result) {
        if (AUDITLOG.isTraceEnabled()) {
            InetAddress remoteAddr = RpcServer.getRemoteAddress();
            AUDITLOG.trace("Access " + (result.isAllowed() ? "allowed" : "denied") + " for user "
                    + (result.getUser() != null ? result.getUser().getShortName() : "UNKNOWN") + "; reason: "
                    + result.getReason() + "; remote address: " + (remoteAddr != null ? remoteAddr : "") + "; request: "
                    + result.getRequest() + "; context: " + result.toContextString());
        }
    }
}
