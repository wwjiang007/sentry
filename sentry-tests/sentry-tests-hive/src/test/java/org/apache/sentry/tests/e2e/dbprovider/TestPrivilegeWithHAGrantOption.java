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

package org.apache.sentry.tests.e2e.dbprovider;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.junit.Assert;

import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.tests.e2e.hive.DummySentryOnFailureHook;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Disable sentry HA tests for now")
public class TestPrivilegeWithHAGrantOption extends AbstractTestWithDbProvider {

  @BeforeClass
  public static void setup() throws Exception {
    properties = new HashMap<String, String>();
    properties.put(HiveAuthzConf.AuthzConfVars.AUTHZ_ONFAILURE_HOOKS.getVar(),
        DummySentryOnFailureHook.class.getName());
    createContext();
    DummySentryOnFailureHook.invoked = false;

    // Do not run these tests if run with external HiveServer2
    // This test checks for a static member, which will not
    // be set if HiveServer2 and the test run in different JVMs
    String hiveServer2Type = System
        .getProperty(HiveServerFactory.HIVESERVER2_TYPE);
    if(hiveServer2Type != null) {
      Assume.assumeTrue(HiveServerFactory.isInternalServer(
          HiveServerFactory.HiveServer2Type.valueOf(hiveServer2Type.trim())));
    }
  }

  /*
   * Admin grant DB_1 user1 without grant option, grant user3 with grant option,
   * user1 tries to grant it to user2, but failed.
   * user3 can grant it to user2.
   * user1 tries to revoke, but failed.
   * user3 tries to revoke user2, user3 and user1, user3 revoke user1 will failed.
   * permissions for DB_1.
   */
  @Test
  public void testOnGrantPrivilege() throws Exception {

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE admin_role");
    statement.execute("GRANT ALL ON SERVER "
        + HiveServerFactory.DEFAULT_AUTHZ_SERVER_NAME + " TO ROLE admin_role");
    statement.execute("GRANT ROLE admin_role TO GROUP " + ADMINGROUP);
    statement.execute("DROP DATABASE IF EXISTS db_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS db_2 CASCADE");
    statement.execute("CREATE DATABASE db_1");
    shutdownAllSentryService();
    startSentryService(1);
    statement.execute("CREATE ROLE group1_role");
    statement.execute("GRANT ALL ON DATABASE db_1 TO ROLE group1_role");
    statement.execute("GRANT ROLE group1_role TO GROUP " + USERGROUP1);
    statement.execute("CREATE ROLE group3_grant_role");
    shutdownAllSentryService();
    startSentryService(1);
    statement.execute("GRANT ALL ON DATABASE db_1 TO ROLE group3_grant_role WITH GRANT OPTION");
    statement.execute("GRANT ROLE group3_grant_role TO GROUP " + USERGROUP3);
    shutdownAllSentryService();
    startSentryService(1);
    statement.execute("CREATE ROLE group2_role");
    statement.execute("GRANT ROLE group2_role TO GROUP " + USERGROUP2);

    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    statement.execute("USE db_1");
    statement.execute("CREATE TABLE foo (id int)");
    verifyFailureHook(statement,"GRANT ALL ON DATABASE db_1 TO ROLE group2_role",HiveOperation.GRANT_PRIVILEGE,null,null,true);
    verifyFailureHook(statement,"GRANT ALL ON DATABASE db_1 TO ROLE group2_role WITH GRANT OPTION",HiveOperation.GRANT_PRIVILEGE,null,null,true);
    connection.close();

    connection = context.createConnection(USER3_1);
    shutdownAllSentryService();
    startSentryService(1);
    statement = context.createStatement(connection);
    statement.execute("GRANT ALL ON DATABASE db_1 TO ROLE group2_role");
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    verifyFailureHook(statement,"REVOKE ALL ON Database db_1 FROM ROLE admin_role",HiveOperation.REVOKE_PRIVILEGE,null,null,true);
    shutdownAllSentryService();
    startSentryService(1);
    verifyFailureHook(statement,"REVOKE ALL ON Database db_1 FROM ROLE group2_role",HiveOperation.REVOKE_PRIVILEGE,null,null,true);
    verifyFailureHook(statement,"REVOKE ALL ON Database db_1 FROM ROLE group3_grant_role",HiveOperation.REVOKE_PRIVILEGE,null,null,true);
    connection.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("REVOKE ALL ON Database db_1 FROM ROLE group2_role");
    statement.execute("REVOKE ALL ON Database db_1 FROM ROLE group3_grant_role");
    verifyFailureHook(statement,"REVOKE ALL ON Database db_1 FROM ROLE group1_role",HiveOperation.REVOKE_PRIVILEGE,null,null,true);

    connection.close();
    context.close();
  }

  // run the given statement and verify that failure hook is invoked as expected
  private void verifyFailureHook(Statement statement, String sqlStr, HiveOperation expectedOp,
       String dbName, String tableName, boolean checkSentryAccessDeniedException) throws Exception {
    // negative test case: non admin user can't create role
    Assert.assertFalse(DummySentryOnFailureHook.invoked);
    try {
      statement.execute(sqlStr);
      Assert.fail("Expected SQL exception for " + sqlStr);
    } catch (SQLException e) {
      Assert.assertTrue(DummySentryOnFailureHook.invoked);
    } finally {
      DummySentryOnFailureHook.invoked = false;
    }
    if (expectedOp != null) {
      Assert.assertNotNull("Hive op is null for op: " + expectedOp, DummySentryOnFailureHook.hiveOp);
      Assert.assertTrue(expectedOp.equals(DummySentryOnFailureHook.hiveOp));
    }
    if (checkSentryAccessDeniedException) {
      Assert.assertTrue("Expected SentryDeniedException for op: " + expectedOp,
          DummySentryOnFailureHook.exception.getCause() instanceof SentryAccessDeniedException);
    }
    if(tableName != null) {
      Assert.assertNotNull("Table object is null for op: " + expectedOp, DummySentryOnFailureHook.table);
      Assert.assertTrue(tableName.equalsIgnoreCase(DummySentryOnFailureHook.table.getName()));
    }
    if(dbName != null) {
      Assert.assertNotNull("Database object is null for op: " + expectedOp, DummySentryOnFailureHook.db);
      Assert.assertTrue(dbName.equalsIgnoreCase(DummySentryOnFailureHook.db.getName()));
    }
  }
}
