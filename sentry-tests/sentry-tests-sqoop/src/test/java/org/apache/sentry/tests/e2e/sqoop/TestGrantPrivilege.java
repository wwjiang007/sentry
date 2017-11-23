/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.sqoop;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sentry.sqoop.SentrySqoopError;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestGrantPrivilege extends AbstractSqoopSentryTestBase {

  @Test
  public void testNotSupportGrantPrivilegeToUser() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrincipal user1 = new MPrincipal("not_support_grant_user_1", MPrincipal.TYPE.GROUP);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege readPriv = new MPrivilege(hdfsConnector, SqoopActionConstant.READ, false);
    try {
      client.grantPrivilege(Lists.newArrayList(user1), Lists.newArrayList(readPriv));
      fail("expected not support exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SentrySqoopError.GRANT_REVOKE_PRIVILEGE_NOT_SUPPORT_FOR_PRINCIPAL);
    }
  }

  @Test
  public void testNotSupportGrantPrivilegeToGroup() throws Exception {
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrincipal group1 = new MPrincipal("not_support_grant_group_1", MPrincipal.TYPE.GROUP);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege readPriv = new MPrivilege(hdfsConnector, SqoopActionConstant.READ, false);
    try {
      client.grantPrivilege(Lists.newArrayList(group1), Lists.newArrayList(readPriv));
      fail("expected not support exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SentrySqoopError.GRANT_REVOKE_PRIVILEGE_NOT_SUPPORT_FOR_PRINCIPAL);
    }
  }

  @Test
  public void testGrantPrivilege() throws Exception {
    /**
     * user1 belongs to group group1
     * admin user grant role role1 to group group1
     * admin user grant read privilege on connector HDFS_CONNECTOR_NAME to role role1
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role1 = new MRole(ROLE1);
    MPrincipal group1Princ = new MPrincipal(GROUP1, MPrincipal.TYPE.GROUP);
    MPrincipal role1Princ = new MPrincipal(ROLE1, MPrincipal.TYPE.ROLE);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege readPrivilege = new MPrivilege(hdfsConnector, SqoopActionConstant.READ, false);
    client.createRole(role1);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1Princ));
    client.grantPrivilege(Lists.newArrayList(role1Princ), Lists.newArrayList(readPrivilege));

    // check user1 has privilege on role1
    client = sqoopServerRunner.getSqoopClient(USER1);
    assertTrue(client.getPrivilegesByPrincipal(role1Princ, hdfsConnector).size() == 1);
  }

  @Test
  public void testGrantPrivilegeTwice() throws Exception {
    /**
     * user2 belongs to group group2
     * admin user grant role role2 to group group2
     * admin user grant write privilege on connector HDFS_CONNECTOR_NAME to role role2
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role2 = new MRole(ROLE2);
    MPrincipal group2Princ = new MPrincipal(GROUP2, MPrincipal.TYPE.GROUP);
    MPrincipal role2Princ = new MPrincipal(ROLE2, MPrincipal.TYPE.ROLE);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege writePrivilege = new MPrivilege(hdfsConnector, SqoopActionConstant.WRITE, false);
    client.createRole(role2);
    client.grantRole(Lists.newArrayList(role2), Lists.newArrayList(group2Princ));
    client.grantPrivilege(Lists.newArrayList(role2Princ), Lists.newArrayList(writePrivilege));

    // check user2 has one privilege on role2
    client = sqoopServerRunner.getSqoopClient(USER2);
    assertTrue(client.getPrivilegesByPrincipal(role2Princ, hdfsConnector).size() == 1);

    // grant privilege to role role2 again
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.grantPrivilege(Lists.newArrayList(role2Princ), Lists.newArrayList(writePrivilege));

    // check user2 has only one privilege on role2
    client = sqoopServerRunner.getSqoopClient(USER2);
    assertTrue(client.getPrivilegesByPrincipal(role2Princ, hdfsConnector).size() == 1);
  }

  @Test
  public void testGrantPrivilegeWithAllPrivilegeExist() throws Exception {
    /**
     * user3 belongs to group group3
     * admin user grant role role3 to group group3
     * admin user grant all privilege on connector HDFS_CONNECTOR_NAME to role role3
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role3 = new MRole(ROLE3);
    MPrincipal group3Princ = new MPrincipal(GROUP3, MPrincipal.TYPE.GROUP);
    MPrincipal role3Princ = new MPrincipal(ROLE3, MPrincipal.TYPE.ROLE);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege allPrivilege = new MPrivilege(hdfsConnector, SqoopActionConstant.ALL_NAME, false);
    client.createRole(role3);
    client.grantRole(Lists.newArrayList(role3), Lists.newArrayList(group3Princ));
    client.grantPrivilege(Lists.newArrayList(role3Princ), Lists.newArrayList(allPrivilege));

    // check user3 has one privilege on role3
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getPrivilegesByPrincipal(role3Princ, hdfsConnector).size() == 1);
    // user3 has the all action on role3
    MPrivilege user3Privilege = client.getPrivilegesByPrincipal(role3Princ, hdfsConnector).get(0);
    assertEquals(user3Privilege.getAction(), SqoopActionConstant.ALL_NAME);

    /**
     * admin user grant read privilege on connector all to role role3
     * because the role3 has already the all privilege, the read privilege granting has
     * no impact on the role3
     */
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrivilege readPrivilege = new MPrivilege(hdfsConnector, SqoopActionConstant.READ, false);
    client.grantPrivilege(Lists.newArrayList(role3Princ), Lists.newArrayList(readPrivilege));
    // check user3 has only one privilege on role3
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getPrivilegesByPrincipal(role3Princ, hdfsConnector).size() == 1);
    // user3 has the all action on role3
    user3Privilege = client.getPrivilegesByPrincipal(role3Princ, hdfsConnector).get(0);
    assertEquals(user3Privilege.getAction(), SqoopActionConstant.ALL_NAME);
  }

  @Test
  public void testGrantALLPrivilegeWithOtherPrivilegesExist() throws Exception {
    /**
     * user4 belongs to group group4
     * admin user grant role role4 to group group4
     * admin user grant read privilege on connector HDFS_CONNECTOR_NAME to role role4
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role4 = new MRole(ROLE4);
    MPrincipal group4Princ = new MPrincipal(GROUP4, MPrincipal.TYPE.GROUP);
    MPrincipal role4Princ = new MPrincipal(ROLE4, MPrincipal.TYPE.ROLE);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege readPrivilege = new MPrivilege(hdfsConnector, SqoopActionConstant.READ, false);
    client.createRole(role4);
    client.grantRole(Lists.newArrayList(role4), Lists.newArrayList(group4Princ));
    client.grantPrivilege(Lists.newArrayList(role4Princ), Lists.newArrayList(readPrivilege));

    // check user4 has one privilege on role1
    client = sqoopServerRunner.getSqoopClient(USER4);
    assertTrue(client.getPrivilegesByPrincipal(role4Princ, hdfsConnector).size() == 1);
    // user4 has the read action on collector all
    MPrivilege user4Privilege = client.getPrivilegesByPrincipal(role4Princ, hdfsConnector).get(0);
    assertEquals(user4Privilege.getAction().toLowerCase(), SqoopActionConstant.READ);

    /**
     * admin user grant write privilege on connector HDFS_CONNECTOR_NAME to role role4
     */
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrivilege writePrivilege = new MPrivilege(hdfsConnector, SqoopActionConstant.WRITE, false);
    client.grantPrivilege(Lists.newArrayList(role4Princ), Lists.newArrayList(writePrivilege));

    // check user4 has two privileges on role1
    client = sqoopServerRunner.getSqoopClient(USER4);
    assertTrue(client.getPrivilegesByPrincipal(role4Princ, hdfsConnector).size() == 2);
    // user4 has the read and write action on collector HDFS_CONNECTOR_NAME
    List<String> actions = Lists.newArrayList();
    for (MPrivilege privilege : client.getPrivilegesByPrincipal(role4Princ, hdfsConnector)) {
      actions.add(privilege.getAction().toLowerCase());
    }
    assertEquals(2, actions.size());
    assertTrue(actions.contains(SqoopActionConstant.READ));
    assertTrue(actions.contains(SqoopActionConstant.WRITE));

    /**
     * admin user grant all privilege on connector HDFS_CONNECTOR_NAME to role role4
     * because the all privilege includes the read and write privileges, these privileges will
     * be removed
     */
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MPrivilege allPrivilege = new MPrivilege(hdfsConnector, SqoopActionConstant.ALL_NAME, false);
    client.grantPrivilege(Lists.newArrayList(role4Princ), Lists.newArrayList(allPrivilege));

    // check user4 has only privilege on role1
    client = sqoopServerRunner.getSqoopClient(USER4);
    assertTrue(client.getPrivilegesByPrincipal(role4Princ, hdfsConnector).size() == 1);
    // user4 has the all action on role3
    user4Privilege = client.getPrivilegesByPrincipal(role4Princ, hdfsConnector).get(0);
    assertEquals(user4Privilege.getAction(), SqoopActionConstant.ALL_NAME);
  }
}
