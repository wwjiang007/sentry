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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.tests.e2e.hive.DummySentryOnFailureHook;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDbSentryOnFailureHookLoading extends AbstractTestWithDbProvider {

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

  @After
  public void clearDB() throws Exception {
    Connection connection;
    Statement statement;
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    ResultSet resultSet;
    resultSet = statement.executeQuery("SHOW roles");
    List<String> roles = new ArrayList<String>();
    while ( resultSet.next()) {
      roles.add(resultSet.getString(1));
    }
    for(String role:roles) {
      statement.execute("DROP Role " + role);
    }

    statement.close();
    connection.close();
    if (context != null) {
      context.close();
    }
  }

  /* Admin creates database DB_2
   * user1 tries to drop DB_2, but it has permissions for DB_1.
   */
  @Test
  public void testOnFailureHookLoading() throws Exception {

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    try {
      statement.execute("DROP ROLE admin_role");
    } catch (Exception ex) {
      //It is ok if admin_role already exists
    }
    statement.execute("CREATE ROLE admin_role");
    statement.execute("GRANT ALL ON SERVER "
        + HiveServerFactory.DEFAULT_AUTHZ_SERVER_NAME + " TO ROLE admin_role");
    statement.execute("GRANT ROLE admin_role TO GROUP " + ADMINGROUP);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB_2 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("CREATE DATABASE DB_2");
    statement.execute("CREATE TABLE db_2.tab1(a int )");

    statement.execute("CREATE ROLE all_db1");
    statement.execute("GRANT ALL ON DATABASE DB_1 TO ROLE all_db1");
    statement.execute("GRANT ROLE all_db1 TO GROUP " + USERGROUP1);

    statement.execute("CREATE ROLE lock_db2_tab1");
    statement.execute("GRANT ROLE lock_db2_tab1 TO GROUP " + USERGROUP1);

    statement.execute("USE db_2");
    statement.execute("GRANT LOCK ON TABLE tab1 TO ROLE lock_db2_tab1");// To give user1 privilege to do USE db_2
    statement.close();
    connection.close();

    // test execution
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    // Failure hook for create table when table doesnt exist
    verifyFailureHook(statement, "CREATE TABLE DB_2.TAB2(id INT)", HiveOperation.CREATETABLE, "db_2", null, false);

    // Failure hook for create table when table exist
    verifyFailureHook(statement, "CREATE TABLE DB_2.TAB1(id INT)", HiveOperation.CREATETABLE, "db_2", null, false);

    // Failure hook for select * from table when table exist
    verifyFailureHook(statement, "select * from db_2.tab1", HiveOperation.QUERY,
        null, null, false);

    //Denied alter table invokes failure hook
    statement.execute("USE DB_2");
    verifyFailureHook(statement, "ALTER TABLE tab1 CHANGE id id1 INT", HiveOperation.ALTERTABLE_RENAMECOL,
        "db_2", null, false);

    statement.close();
    connection.close();

    //test cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.execute("DROP DATABASE DB_2 CASCADE");
    statement.close();
    connection.close();
    context.close();
  }

  /*
   * Admin creates database DB_2 user1 tries to drop DB_2, but it has
   * permissions for DB_1.
   */
  @Test
  public void testOnFailureHookForAuthDDL() throws Exception {

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE ROLE admin_role");
    statement.execute("GRANT ALL ON SERVER "
        + HiveServerFactory.DEFAULT_AUTHZ_SERVER_NAME + " TO ROLE admin_role");
    statement.execute("GRANT ROLE admin_role TO GROUP " + ADMINGROUP);
    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("DROP DATABASE IF EXISTS DB_2 CASCADE");
    statement.execute("CREATE DATABASE DB_1");
    statement.execute("CREATE TABLE DB_1.tab1(a int )");
    statement.execute("CREATE ROLE all_db1");
    statement.execute("GRANT ALL ON DATABASE DB_1 TO ROLE all_db1");
    statement.execute("GRANT ROLE all_db1 TO GROUP " + USERGROUP1);
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    statement.execute("USE DB_1");
    statement.execute("CREATE TABLE foo (id int)");

    verifyFailureHook(statement, "CREATE ROLE fooTest",
        HiveOperation.CREATEROLE, null, null, true);

    verifyFailureHook(statement, "DROP ROLE fooTest",
        HiveOperation.DROPROLE, null, null, true);

    verifyFailureHook(statement,
        "GRANT ALL ON SERVER server1 TO ROLE admin_role",
        HiveOperation.GRANT_PRIVILEGE, null, null, true);

    verifyFailureHook(statement,
        "REVOKE ALL ON SERVER server1 FROM ROLE admin_role",
        HiveOperation.REVOKE_PRIVILEGE, null, null, true);

    verifyFailureHook(statement, "GRANT ROLE all_db1 TO GROUP " + USERGROUP1,
        HiveOperation.GRANT_ROLE, null, null, true);

    verifyFailureHook(statement,
        "REVOKE ROLE all_db1 FROM GROUP " + USERGROUP1,
        HiveOperation.REVOKE_ROLE, null, null, true);

    verifyFailureHook(statement, "SHOW ROLES",
        HiveOperation.SHOW_ROLES, null, null, true);

    verifyFailureHook(statement, "SHOW ROLE GRANT group group1",
        HiveOperation.SHOW_ROLE_GRANT, null, null, true);

    verifyFailureHook(statement, "SHOW GRANT role role1",
        HiveOperation.SHOW_GRANT, null, null, true);

    //Should pass as user1_1 is granted role all_db1
    statement.execute("SHOW GRANT role all_db1");

        //Grant privilege on table doesnt expose db and table objects
    verifyFailureHook(statement,
        "GRANT ALL ON TABLE db_1.tab1 TO ROLE admin_role",
        HiveOperation.GRANT_PRIVILEGE, null, null, true);

    //Revoke privilege on table doesnt expose db and table objects
    verifyFailureHook(statement,
        "REVOKE ALL ON TABLE db_1.tab1 FROM ROLE admin_role",
        HiveOperation.REVOKE_PRIVILEGE, null, null, true);

    //Grant privilege on database doesnt expose db and table objects
    verifyFailureHook(statement,
        "GRANT ALL ON Database db_1 TO ROLE admin_role",
        HiveOperation.GRANT_PRIVILEGE, null, null, true);

    //Revoke privilege on database doesnt expose db and table objects
    verifyFailureHook(statement,
        "REVOKE ALL ON Database db_1 FROM ROLE admin_role",
        HiveOperation.REVOKE_PRIVILEGE, null, null, true);

    statement.close();
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
      Assert.assertTrue("FailureHook is not ran : " + e.getMessage(), DummySentryOnFailureHook.invoked);
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
      Assert.assertNotNull("Table name is null for op: " + expectedOp, DummySentryOnFailureHook.table.getName());
      Assert.assertEquals(tableName.toLowerCase(), DummySentryOnFailureHook.table.getName().toLowerCase());
    }
    if(dbName != null) {
      Assert.assertNotNull("Database object is null for op: " + expectedOp, DummySentryOnFailureHook.db);
      Assert.assertNotNull("Database name is null for op: " + expectedOp, DummySentryOnFailureHook.db.getName());
      Assert.assertEquals(dbName.toLowerCase(), DummySentryOnFailureHook.db.getName().toLowerCase());
    }
  }
}
