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
package org.apache.sentry.provider.db.tools;

import java.util.List;
import java.util.Set;

import org.apache.sentry.core.common.exception.SentryUserException;

/**
 * The interface for all admin commands, eg, CreateRoleCmd. It is independent of the underlying mechanism (i.e. Generic or Hive)
 */
public interface ShellCommand {

  void createRole(String requestorName, String roleName) throws SentryUserException;

  void dropRole(String requestorName, String roleName) throws SentryUserException;

  void grantPrivilegeToRole(String requestorName, String roleName, String privilege) throws SentryUserException;

  void grantRoleToGroups(String requestorName, String roleName, Set<String> groups) throws SentryUserException;

  void revokePrivilegeFromRole(String requestorName, String roleName, String privilege) throws SentryUserException;

  void revokeRoleFromGroups(String requestorName, String roleName, Set<String> groups) throws SentryUserException;

  List<String> listRoles(String requestorName, String group) throws SentryUserException;

  List<String> listPrivileges(String requestorName, String roleName) throws SentryUserException;

  List<String> listGroupRoles(String requestorName) throws SentryUserException;
}
