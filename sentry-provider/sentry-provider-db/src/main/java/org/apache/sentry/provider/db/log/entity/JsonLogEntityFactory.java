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

package org.apache.sentry.provider.db.log.entity;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.log.util.CommandUtil;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddGroupsResponse;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddUsersRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleAddUsersResponse;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteGroupsResponse;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteUsersRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleDeleteUsersResponse;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleGrantPrivilegeResponse;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;
import org.apache.sentry.provider.db.service.thrift.TAlterSentryRoleRevokePrivilegeResponse;
import org.apache.sentry.provider.db.service.thrift.TCreateSentryRoleRequest;
import org.apache.sentry.provider.db.service.thrift.TCreateSentryRoleResponse;
import org.apache.sentry.provider.db.service.thrift.TDropSentryRoleRequest;
import org.apache.sentry.provider.db.service.thrift.TDropSentryRoleResponse;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.core.common.utils.ThriftUtil;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.Status;
import org.apache.sentry.service.thrift.TSentryResponseStatus;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

public final class JsonLogEntityFactory {

  private static JsonLogEntityFactory factory = new JsonLogEntityFactory();

  private JsonLogEntityFactory() {
  }

  public static JsonLogEntityFactory getInstance() {
    return factory;
  }

  // log entity for hive/impala create role
  public JsonLogEntity createJsonLogEntity(TCreateSentryRoleRequest request,
      TCreateSentryRoleResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    hamle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(
        request.getRoleName(), true));

    return hamle;
  }

  // log entity for hive/impala drop role
  public JsonLogEntity createJsonLogEntity(TDropSentryRoleRequest request,
      TDropSentryRoleResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    hamle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(
        request.getRoleName(), false));

    return hamle;
  }

  // log entity for hive/impala grant privilege
  public Set<JsonLogEntity> createJsonLogEntitys(
      TAlterSentryRoleGrantPrivilegeRequest request,
      TAlterSentryRoleGrantPrivilegeResponse response, Configuration conf) {
    ImmutableSet.Builder<JsonLogEntity> setBuilder = ImmutableSet.builder();
    if (request.isSetPrivileges()) {
      for (TSentryPrivilege privilege : request.getPrivileges()) {
        JsonLogEntity logEntity = createJsonLogEntity(request, privilege, response, conf);
        setBuilder.add(logEntity);
      }
    }
    return setBuilder.build();
  }

  private JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleGrantPrivilegeRequest request, TSentryPrivilege privilege,
      TAlterSentryRoleGrantPrivilegeResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    hamle.setOperationText(CommandUtil.createCmdForGrantPrivilege(request));
    hamle.setDatabaseName(privilege.getDbName());
    hamle.setTableName(privilege.getTableName());
    hamle.setResourcePath(privilege.getURI());
    return hamle;
  }

  // log entity for hive/impala revoke privilege
  public Set<JsonLogEntity> createJsonLogEntitys(
      TAlterSentryRoleRevokePrivilegeRequest request,
      TAlterSentryRoleRevokePrivilegeResponse response, Configuration conf) {
    ImmutableSet.Builder<JsonLogEntity> setBuilder = ImmutableSet.builder();
    if (request.isSetPrivileges()) {
      for (TSentryPrivilege privilege : request.getPrivileges()) {
        JsonLogEntity logEntity = createJsonLogEntity(request, privilege, response, conf);
        setBuilder.add(logEntity);
      }
    }
    return setBuilder.build();
  }

  private JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleRevokePrivilegeRequest request, TSentryPrivilege privilege,
      TAlterSentryRoleRevokePrivilegeResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    hamle.setOperationText(CommandUtil.createCmdForRevokePrivilege(request));
    hamle.setDatabaseName(privilege.getDbName());
    hamle.setTableName(privilege.getTableName());
    hamle.setResourcePath(privilege.getURI());

    return hamle;
  }

  // log entity for hive/impala add role to group
  public JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleAddGroupsRequest request,
      TAlterSentryRoleAddGroupsResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    String groups = getGroupsStr(request.getGroupsIterator());
    hamle.setOperationText(CommandUtil.createCmdForRoleAddGroup(request.getRoleName(), groups));

    return hamle;
  }

  // log entity for hive/impala delete role from group
  public JsonLogEntity createJsonLogEntity(
      TAlterSentryRoleDeleteGroupsRequest request,
      TAlterSentryRoleDeleteGroupsResponse response, Configuration conf) {
    DBAuditMetadataLogEntity hamle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    String groups = getGroupsStr(request.getGroupsIterator());
    hamle.setOperationText(CommandUtil.createCmdForRoleDeleteGroup(request.getRoleName(), groups));

    return hamle;
  }

  private String getGroupsStr(Iterator<TSentryGroup> iter) {
    StringBuilder groups = new StringBuilder("");
    if (iter != null) {
      boolean commaFlg = false;
      while (iter.hasNext()) {
        if (commaFlg) {
          groups.append(", ");
        } else {
          commaFlg = true;
        }
        groups.append(iter.next().getGroupName());
      }
    }
    return groups.toString();
  }

  public JsonLogEntity createJsonLogEntity(TAlterSentryRoleAddUsersRequest request,
      TAlterSentryRoleAddUsersResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    String users = getUsersStr(request.getUsersIterator());
    amle.setOperationText(CommandUtil.createCmdForRoleAddUser(request.getRoleName(), users));

    return amle;
  }

  public JsonLogEntity createJsonLogEntity(TAlterSentryRoleDeleteUsersRequest request,
      TAlterSentryRoleDeleteUsersResponse response, Configuration conf) {
    AuditMetadataLogEntity amle = createCommonHAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName());
    String users = getUsersStr(request.getUsersIterator());
    amle.setOperationText(CommandUtil.createCmdForRoleDeleteUser(request.getRoleName(), users));

    return amle;
  }

  private String getUsersStr(Iterator<String> iter) {
    StringBuilder users = new StringBuilder("");
    if (iter != null) {
      boolean commaFlg = false;
      while (iter.hasNext()) {
        if (commaFlg) {
          users.append(", ");
        } else {
          commaFlg = true;
        }
        users.append(iter.next());
      }
    }
    return users.toString();
  }

  public String isAllowed(TSentryResponseStatus status) {
    if (status.equals(Status.OK())) {
      return Constants.TRUE;
    }
    return Constants.FALSE;
  }

  // log entity for generic model create role
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.provider.db.generic.service.thrift.TCreateSentryRoleRequest request,
      org.apache.sentry.provider.db.generic.service.thrift.TCreateSentryRoleResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    gmamle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(request.getRoleName(), true));

    return gmamle;
  }

  // log entity for generic model drop role
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.provider.db.generic.service.thrift.TDropSentryRoleRequest request,
      org.apache.sentry.provider.db.generic.service.thrift.TDropSentryRoleResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    gmamle.setOperationText(CommandUtil.createCmdForCreateOrDropRole(request.getRoleName(), false));

    return gmamle;
  }

  // log entity for generic model grant privilege
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleGrantPrivilegeRequest request,
      org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleGrantPrivilegeResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    if (request.getPrivilege() != null) {
      List<TAuthorizable> authorizables = request.getPrivilege().getAuthorizables();
      Map<String, String> privilegesMap = new LinkedHashMap<String, String>();
      if (authorizables != null) {
        for (TAuthorizable authorizable : authorizables) {
          privilegesMap.put(authorizable.getType(), authorizable.getName());
        }
      }
      gmamle.setPrivilegesMap(privilegesMap);
    }
    gmamle.setOperationText(CommandUtil.createCmdForGrantGMPrivilege(request));

    return gmamle;
  }

  // log entity for generic model revoke privilege
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleRevokePrivilegeRequest request,
      org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleRevokePrivilegeResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    if (request.getPrivilege() != null) {
      List<TAuthorizable> authorizables = request.getPrivilege().getAuthorizables();
      Map<String, String> privilegesMap = new LinkedHashMap<String, String>();
      if (authorizables != null) {
        for (TAuthorizable authorizable : authorizables) {
          privilegesMap.put(authorizable.getType(), authorizable.getName());
        }
      }
      gmamle.setPrivilegesMap(privilegesMap);
    }
    gmamle.setOperationText(CommandUtil.createCmdForRevokeGMPrivilege(request));

    return gmamle;
  }

  // log entity for generic model add role to group
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleAddGroupsRequest request,
      org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleAddGroupsResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    Joiner joiner = Joiner.on(",");
    String groups = joiner.join(request.getGroupsIterator());
    gmamle.setOperationText(CommandUtil.createCmdForRoleAddGroup(request.getRoleName(), groups));

    return gmamle;
  }

  // log entity for hive delete role from group
  public JsonLogEntity createJsonLogEntity(
      org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleDeleteGroupsRequest request,
      org.apache.sentry.provider.db.generic.service.thrift.TAlterSentryRoleDeleteGroupsResponse response,
      Configuration conf) {
    GMAuditMetadataLogEntity gmamle = createCommonGMAMLE(conf, response.getStatus(),
        request.getRequestorUserName(), request.getClass().getName(), request.getComponent());
    Joiner joiner = Joiner.on(",");
    String groups = joiner.join(request.getGroupsIterator());
    gmamle.setOperationText(CommandUtil.createCmdForRoleDeleteGroup(request.getRoleName(), groups));

    return gmamle;
  }

  private DBAuditMetadataLogEntity createCommonHAMLE(Configuration conf,
      TSentryResponseStatus responseStatus, String userName, String requestClassName) {
    DBAuditMetadataLogEntity hamle = new DBAuditMetadataLogEntity();
    setCommAttrForAMLE(hamle, conf, responseStatus, userName, requestClassName);
    return hamle;
  }

  private GMAuditMetadataLogEntity createCommonGMAMLE(Configuration conf,
      TSentryResponseStatus responseStatus, String userName, String requestClassName,
      String component) {
    GMAuditMetadataLogEntity gmamle = new GMAuditMetadataLogEntity();
    setCommAttrForAMLE(gmamle, conf, responseStatus, userName, requestClassName);
    gmamle.setComponent(component);
    return gmamle;
  }

  private void setCommAttrForAMLE(AuditMetadataLogEntity amle, Configuration conf,
      TSentryResponseStatus responseStatus, String userName, String requestClassName) {
    amle.setUserName(userName);
    amle.setServiceName(conf.get(ServerConfig.SENTRY_SERVICE_NAME,
        ServerConfig.SENTRY_SERVICE_NAME_DEFAULT).trim());
    amle.setImpersonator(ThriftUtil.getImpersonator());
    amle.setIpAddress(ThriftUtil.getIpAddress());
    amle.setOperation(Constants.requestTypeToOperationMap.get(requestClassName));
    amle.setEventTime(Long.toString(System.currentTimeMillis()));
    amle.setAllowed(isAllowed(responseStatus));
    amle.setObjectType(Constants.requestTypeToObjectTypeMap
        .get(requestClassName));
  }
}
