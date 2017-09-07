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

package org.apache.sentry.tests.e2e.hive;

import org.apache.sentry.provider.file.PolicyFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Tests privileges at table scope with cross database access */

public class TestCrossDbOps extends AbstractTestWithStaticConfiguration {
  private static final Logger LOGGER = LoggerFactory
          .getLogger(TestCrossDbOps.class);

  private File dataFile;
  private PolicyFile policyFile;
  private String loadData;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception{
    LOGGER.info("TestCrossDbOps setupTestStaticConfiguration");
    policyOnHdfs = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();

  }

  @Before
  public void setup() throws Exception {
    LOGGER.info("TestCrossDbOps setup");
    policyFile = super.setupPolicy();
    super.setup();
    File dataDir = context.getDataDir();
    // copy data file to test dir
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    loadData = "server=server1->uri=file://" + dataFile.getPath();
  }

  /*
   * Admin creates DB_1, DB2, tables (tab_1 ) and (tab_2, tab_3) in DB_1 and
   * DB_2 respectively. User user1 has select on DB_1.tab_1, insert on
   * DB2.tab_2 User user2 has select on DB2.tab_3 Test show database and show
   * tables for both user1 and user2
   */
  @Test
  public void testShowDatabasesAndShowTables() throws Exception {
    // admin create two databases
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE TAB1(id int)");
    statement.executeQuery("SHOW TABLES");
    statement.execute("USE " + DB2);
    statement.execute("CREATE TABLE TAB2(id int)");
    statement.execute("CREATE TABLE TAB3(id int)");

    // load policy file and grant role with privileges
    policyFile
            .addRolesToGroup(USERGROUP1, "select_tab1", "insert_tab2")
            .addRolesToGroup(USERGROUP2, "select_tab3")
            .addPermissionsToRole("select_tab1",  "server=server1->db=" + DB1 + "->table=tab1->action=select")
            .addPermissionsToRole("select_tab3", "server=server1->db=" + DB2 + "->table=tab3->action=select")
            .addPermissionsToRole("insert_tab2", "server=server1->db=" + DB2 + "->table=tab2->action=insert")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // show grant to validate roles and privileges
    if(useSentryService) {
      PrivilegeResultSet pRset = new PrivilegeResultSet(statement, "SHOW GRANT ROLE select_tab1 ON DATABASE " + DB1);
      LOGGER.info("SHOW GRANT ROLE select_tab1 ON DATABASE " + DB1 + " : " + pRset.toString());
      pRset.verifyResultSetColumn("database", DB1);
      pRset.verifyResultSetColumn("table", "tab1");

      pRset = new PrivilegeResultSet(statement, "SHOW GRANT ROLE insert_tab2 ON DATABASE " + DB2);
      LOGGER.info("SHOW GRANT ROLE insert_tab2 ON DATABASE " + DB2 + " : " + pRset.toString());
      pRset.verifyResultSetColumn("database", DB2);
      pRset.verifyResultSetColumn("table", "tab2");

      pRset = new PrivilegeResultSet(statement, "SHOW GRANT ROLE select_tab3 ON DATABASE " + DB2);
      LOGGER.info("SHOW GRANT ROLE select_tab3 ON DATABASE " + DB2 + " : " + pRset.toString());
      pRset.verifyResultSetColumn("database", DB2);
      pRset.verifyResultSetColumn("table", "tab3");
    }

    // test show databases
    // show databases shouldn't filter any of the dbs from the resultset
    Connection conn = context.createConnection(USER1_1);
    Statement stmt = context.createStatement(conn);
    PrivilegeResultSet pRset = new PrivilegeResultSet(stmt, "SHOW DATABASES");
    LOGGER.info("found databases :" + pRset.toString());
    pRset.verifyResultSetColumn("database_name", DB1);
    pRset.verifyResultSetColumn("database_name", DB2);

    // test show tables
    stmt.execute("USE " + DB1);
    pRset = new PrivilegeResultSet(stmt, "SHOW TABLES");
    LOGGER.info("found tables :" + pRset.toString());
    pRset.verifyResultSetColumn("tab_name", "tab1");

    stmt.execute("USE " + DB2);
    pRset = new PrivilegeResultSet(stmt, "SHOW TABLES");
    LOGGER.info("found tables :" + pRset.toString());
    pRset.verifyResultSetColumn("tab_name", "tab2");

    try {
      stmt.close();
      conn.close();
    } catch (Exception ex) {
      // nothing to do
    }

    // test show databases and show tables for user2_1
    conn = context.createConnection(USER2_1);
    stmt = context.createStatement(conn);

    pRset = new PrivilegeResultSet(stmt, "SHOW DATABASES");
    pRset.verifyResultSetColumn("database_name", DB2);

    // test show tables
    stmt.execute("USE " + DB2);
    pRset = new PrivilegeResultSet(stmt, "SHOW TABLES");
    pRset.verifyResultSetColumn("tab_name", "tab3");

    try {
      stmt.execute("USE " + DB1);
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    context.close();
  }

  /*
   * Admin creates DB_1, DB2, tables (tab_1 ) and (tab_2, tab_3) in DB_1 and
   * DB_2 respectively. User user1 has select on DB_1.tab_1, insert on
   * DB2.tab_2 User user2 has select on DB2.tab_3 Test show database and show
   * tables for both user1 and user2
   */
  @Test
  public void testJDBCGetSchemasAndGetTables() throws Exception {
    // admin create two databases
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE TAB1(id int)");
    statement.executeQuery("SHOW TABLES");
    statement.execute("USE " + DB2);
    statement.execute("CREATE TABLE TAB2(id int)");
    statement.execute("CREATE TABLE TAB3(id int)");

    // edit policy file
    policyFile.addRolesToGroup(USERGROUP1, "select_tab1", "insert_tab2")
            .addRolesToGroup(USERGROUP2, "select_tab3")
            .addPermissionsToRole("select_tab1", "server=server1->db=" + DB1 + "->table=tab1->action=select")
            .addPermissionsToRole("select_tab3", "server=server1->db=" + DB2 + "->table=tab3->action=select")
            .addPermissionsToRole("insert_tab2", "server=server1->db=" + DB2 + "->table=tab2->action=insert")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test show databases
    // show databases shouldn't filter any of the dbs from the resultset
    Connection conn = context.createConnection(USER1_1);
    Statement stmt = context.createStatement(conn);
    // test direct JDBC metadata API
    ResultSet res = stmt.executeQuery("SHOW DATABASES");
    res = conn.getMetaData().getSchemas();
    ResultSetMetaData resMeta = res.getMetaData();
    assertEquals(2, resMeta.getColumnCount());
    assertEquals("TABLE_SCHEM", resMeta.getColumnName(1));
    assertEquals("TABLE_CATALOG", resMeta.getColumnName(2));

    List<String> expectedResult = new ArrayList<String>();
    List<String> returnedResult = new ArrayList<String>();

    expectedResult.add(DB1);
    expectedResult.add(DB2);
    expectedResult.add("default");
    while (res.next()) {
      returnedResult.add(res.getString(1).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    // test direct JDBC metadata API
    res = conn.getMetaData().getTables(null, DB1, "tab%", null);
    expectedResult.add("tab1");
    while (res.next()) {
      returnedResult.add(res.getString(3).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    // test direct JDBC metadata API
    res = conn.getMetaData().getTables(null, DB2, "tab%", null);
    expectedResult.add("tab2");
    while (res.next()) {
      returnedResult.add(res.getString(3).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    res = conn.getMetaData().getTables(null, "DB%", "tab%", null);
    expectedResult.add("tab2");
    expectedResult.add("tab1");
    while (res.next()) {
      returnedResult.add(res.getString(3).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    //test show columns
    res = conn.getMetaData().getColumns(null, "DB%", "tab%","i%" );
    expectedResult.add("id");

    while (res.next()) {
      returnedResult.add(res.getString(4).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    conn.close();

    // test show databases and show tables for user2
    conn = context.createConnection(USER2_1);

    // test direct JDBC metadata API
    res = conn.getMetaData().getSchemas();
    resMeta = res.getMetaData();
    assertEquals(2, resMeta.getColumnCount());
    assertEquals("TABLE_SCHEM", resMeta.getColumnName(1));
    assertEquals("TABLE_CATALOG", resMeta.getColumnName(2));

    expectedResult.add(DB2);
    expectedResult.add("default");

    while (res.next()) {
      returnedResult.add(res.getString(1).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    // test JDBC direct API
    res = conn.getMetaData().getTables(null, "DB%", "tab%", null);
    expectedResult.add("tab3");

    while (res.next()) {
      returnedResult.add(res.getString(3).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();


    //test show columns
    res = conn.getMetaData().getColumns(null, "DB%", "tab%","i%" );
    expectedResult.add("id");

    while (res.next()) {
      returnedResult.add(res.getString(4).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    //test show columns
    res = conn.getMetaData().getColumns(null, DB1, "tab%","i%" );

    while (res.next()) {
      returnedResult.add(res.getString(4).trim());
    }
    validateReturnedResult(expectedResult, returnedResult);
    returnedResult.clear();
    expectedResult.clear();
    res.close();

    context.close();
  }

  /**
   * 2.8 admin user create two database, DB_1, DB_2 admin grant all to USER1_1,
   * USER1_2 on DB_1, admin grant all to user1's group, user2's group on DB_2
   * positive test case: user1, user2 has ALL privilege on both DB_1 and DB_2
   * negative test case: user1, user2 don't have ALL privilege on SERVER
   */
  @Test
  public void testDbPrivileges() throws Exception {
    createDb(ADMIN1, DB1, DB2);

    // edit policy file
    policyFile.addRolesToGroup(USERGROUP1, "db1_all,db2_all, load_data")
            .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
            .addPermissionsToRole("db2_all", "server=server1->db=" + DB2)
            .addPermissionsToRole("load_data", "server=server1->URI=file://" + dataFile.getPath())
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    for (String user : new String[]{USER1_1, USER1_2}) {
      for (String dbName : new String[]{DB1, DB2}) {
        Connection userConn = context.createConnection(user);
        String tabName = user + "_tab1";
        Statement userStmt = context.createStatement(userConn);
        // Positive case: test user1 and user2 has permissions to access
        // db1 and
        // db2
        userStmt.execute("Use " + dbName);
        userStmt
        .execute("create table " + dbName + "." + tabName + " (id int)");
        userStmt.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath()
            + "' INTO TABLE " + tabName);
        userStmt.execute("select * from " + dbName + "." + tabName);
        context.close();
      }
    }
  }

  /**
   * Test Case 2.11 admin user create a new database DB_1 and grant ALL to
   * himself on DB_1 should work
   */
  @Test
  public void testAdminDbPrivileges() throws Exception {
    createDb(ADMIN1, DB1);

    policyFile
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    Connection adminCon = context.createConnection(ADMIN1);
    Statement adminStmt = context.createStatement(adminCon);
    String tabName = DB1 + "." + "admin_tab1";
    adminStmt.execute("create table " + tabName + "(c1 string)");
    adminStmt.execute("load data local inpath '" + dataFile.getPath() + "' into table "
        + tabName);
    assertTrue(adminStmt.executeQuery("select * from " + tabName).next());
    adminStmt.close();
    adminCon.close();
  }

  /**
   * Test Case 2.14 admin user create a new database DB_1 create TABLE_1 in DB_1
   * admin user grant INSERT to user1's group on TABLE_1 negative test case:
   * user1 try to do following on TABLE_1 will fail: --explain --analyze
   * --describe --describe function --show columns --show table status --show
   * table properties --show create table --show partitions --show indexes
   * --select * from TABLE_1.
   */
  @Test
  public void testNegativeUserPrivileges() throws Exception {
    Connection adminCon = context.createConnection(ADMIN1);
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("CREATE DATABASE " + DB1);
    adminStmt.execute("create table " + DB1 + ".table_1 (id int)");
    adminStmt.execute("create table " + DB1 + ".table_2 (id int)");
    adminStmt.close();
    adminCon.close();

    // edit policy file
    policyFile.addRolesToGroup(USERGROUP1, "db1_tab1_insert", "db1_tab2_all")
            .addPermissionsToRole("db1_tab2_all", "server=server1->db=" + DB1 + "->table=table_2")
            .addPermissionsToRole("db1_tab1_insert", "server=server1->db=" + DB1 + "->table=table_1->action=insert")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    Connection userConn = context.createConnection(USER1_1);
    Statement userStmt = context.createStatement(userConn);
    context.assertAuthzException(userStmt, "select * from " + DB1 + ".table_1");
    userConn.close();
    userStmt.close();
  }

  /**
   * Test Case 2.16 admin user create a new database DB_1 create TABLE_1 and
   * TABLE_2 (same schema) in DB_1 admin user grant SELECT, INSERT to user1's
   * group on TABLE_2 negative test case: user1 try to do following on TABLE_1
   * will fail: --insert overwrite TABLE_2 select * from TABLE_1
   */
  @Test
  public void testNegativeUserDMLPrivileges() throws Exception {
    createDb(ADMIN1, DB1);
    Connection adminCon = context.createConnection(ADMIN1);
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("create table " + DB1 + ".table_1 (id int)");
    adminStmt.execute("create table " + DB1 + ".table_2 (id int)");
    adminStmt.close();
    adminCon.close();

    policyFile
            .addPermissionsToRole("db1_tab2_all", "server=server1->db=" + DB1 + "->table=table_2")
            .addRolesToGroup(USERGROUP1, "db1_tab2_all")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    Connection userConn = context.createConnection(USER1_1);
    Statement userStmt = context.createStatement(userConn);
    context.assertAuthzException(userStmt, "insert overwrite table  " + DB1
        + ".table_2 select * from " + DB1 + ".table_1");
    context.assertAuthzException(userStmt, "insert overwrite directory '" + dataDir.getPath()
        + "' select * from  " + DB1 + ".table_1");
    userStmt.close();
    userConn.close();
  }

  /**
   * Test Case 2.17 Execution steps
   * a) Admin user creates a new database DB_1,
   * b) Admin user grants ALL on DB_1 to group GROUP_1
   * c) User from GROUP_1 creates table TAB_1, TAB_2 in DB_1
   * d) Admin user grants SELECT on TAB_1 to group GROUP_2
   *
   * 1) verify users from GROUP_2 have only SELECT privileges on TAB_1. They
   * shouldn't be able to perform any operation other than those listed as
   * requiring SELECT in the privilege model.
   *
   * 2) verify users from GROUP_2 can't perform queries involving join between
   * TAB_1 and TAB_2.
   *
   * 3) verify users from GROUP_1 can't perform operations requiring ALL @
   * SERVER scope. Refer to list
   */
  @Test
  public void testNegUserPrivilegesAll() throws Exception {
    // create dbs
    Connection adminCon = context.createConnection(ADMIN1);
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("drop table if exists table_def");
    adminStmt.execute("create table table_def (name string)");
    adminStmt
    .execute("load data local inpath '" + dataFile.getPath() + "' into table table_def");

    adminStmt.execute("CREATE DATABASE " + DB1);
    adminStmt.execute("use " + DB1);

    adminStmt.execute("create table table_1 (name string)");
    adminStmt
    .execute("load data local inpath '" + dataFile.getPath() + "' into table table_1");
    adminStmt.execute("create table table_2 (name string)");
    adminStmt
    .execute("load data local inpath '" + dataFile.getPath() + "' into table table_2");
    adminStmt.execute("create view v1 AS select * from table_1");
    adminStmt
    .execute("create table table_part_1 (name string) PARTITIONED BY (year INT)");
    adminStmt.execute("ALTER TABLE table_part_1 ADD PARTITION (year = 2012)");

    adminStmt.close();
    adminCon.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "db1_all")
            .addRolesToGroup(USERGROUP2, "db1_tab1_select")
            .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
            .addPermissionsToRole("db1_tab1_select", "server=server1->db=" + DB1 + "->table=table_1->action=select")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    Connection userConn = context.createConnection(USER2_1);
    Statement userStmt = context.createStatement(userConn);

    context.assertAuthzException(userStmt, "drop database " + DB1);

    // Hive currently doesn't support cross db index DDL

    context.assertAuthzException(userStmt, "CREATE TEMPORARY FUNCTION strip AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFPrintf'");
    context.assertAuthzException(userStmt, "create table  " + DB1
        + ".c_tab_2 as select * from  " + DB1 + ".table_2");
    context.assertAuthzException(userStmt, "select * from  " + DB1 + ".table_2");
    context.assertAuthzException(userStmt, "ALTER DATABASE " + DB1
        + " SET DBPROPERTIES ('foo' = 'bar')");
    context.assertAuthzException(userStmt, "drop table " + DB1 + ".table_1");
    context.assertAuthzException(userStmt, "DROP VIEW IF EXISTS " + DB1 + ".v1");
    context.assertAuthzException(userStmt, "create table " + DB1 + ".table_5 (name string)");
    context.assertAuthzException(userStmt, "ALTER TABLE " + DB1 + ".table_1  RENAME TO "
        + DB1 + ".table_99");
    context.assertAuthzException(userStmt, "insert overwrite table " + DB1
        + ".table_2 select * from " + DB1 + ".table_1");
    context.assertAuthzException(userStmt, "insert overwrite table " + DB1
        + ".table_2 select * from " + "table_def");
    context.assertAuthzException(userStmt, "ALTER TABLE " + DB1
        + ".table_part_1 ADD IF NOT EXISTS PARTITION (year = 2012)");
    context.assertAuthzException(userStmt, "ALTER TABLE " + DB1
        + ".table_part_1 PARTITION (year = 2012) SET LOCATION '/etc'");
    userStmt.close();
    userConn.close();
  }

  /**
   * Steps: 1. admin user create databases, DB_1 and DB_2, no table or other
   * object in database
   * 2. admin grant all to user1's group on DB_1 and DB_2
   * positive test case:
   *  a)user1 has the privilege to create table, load data,
   *   drop table, create view, insert more data on both databases
   * b) user1 can switch between DB_1 and DB_2 without exception
   * negative test case:
   * c) user1 cannot drop database
   */
  @Test
  public void testSandboxOpt9() throws Exception {
    createDb(ADMIN1, DB1, DB2);

    policyFile
            .addPermissionsToRole(GROUP1_ROLE, ALL_DB1, ALL_DB2, loadData)
            .addRolesToGroup(USERGROUP1, GROUP1_ROLE)
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);

    // a
    statement.execute("DROP TABLE IF EXISTS " + DB1 + "." + TBL1);
    statement.execute("create table " + DB1 + "." + TBL1
        + " (under_col int comment 'the under column', value string)");
    statement.execute("load data local inpath '" + dataFile.getPath()
        + "' into table " + DB1 + "." + TBL1);
    statement.execute("DROP VIEW IF EXISTS " + DB1 + "." + VIEW1);
    statement.execute("CREATE VIEW " + DB1 + "." + VIEW1
        + " (value) AS SELECT value from " + DB1 + "." + TBL1
        + " LIMIT 10");
    statement.execute("DROP TABLE IF EXISTS " + DB2 + "." + TBL1);
    statement.execute("CREATE TABLE " + DB2 + "." + TBL1
        + " AS SELECT value from " + DB1 + "." + TBL1
        + " LIMIT 10");

    // b
    statement.execute("DROP TABLE IF EXISTS " + DB2 + "." + TBL2);
    statement.execute("create table " + DB2 + "." + TBL2
        + " (under_col int comment 'the under column', value string)");
    statement.execute("load data local inpath '" + dataFile.getPath()
        + "' into table " + DB2 + "." + TBL2);
    statement.execute("DROP TABLE IF EXISTS " + DB2 + "." + TBL3);
    statement.execute("create table " + DB2 + "." + TBL3
        + " (under_col int comment 'the under column', value string)");
    statement.execute("load data local inpath '" + dataFile.getPath()
        + "' into table " + DB2 + "." + TBL3);

    policyFile.removePermissionsFromRole(GROUP1_ROLE, ALL_DB2);
    writePolicyFile(policyFile);
    // create db1.view1 as select from db2.tbl2
    statement.execute("DROP VIEW IF EXISTS " + DB1 + "." + VIEW2);
    context.assertAuthzException(statement, "CREATE VIEW " + DB1 + "." + VIEW2 +
        " (value) AS SELECT value from " + DB2 + "." + TBL2 + " LIMIT 10");
    // create db1.tbl2 as select from db2.tbl2
    statement.execute("DROP TABLE IF EXISTS " + DB1 + "." + TBL2);
    context.assertAuthzException(statement, "CREATE TABLE " + DB1 + "." + TBL2 +
        " AS SELECT value from " + DB2 + "." + TBL2 + " LIMIT 10");

    statement.close();
    connection.close();
  }

  /**
   * Steps: 1. admin user create databases, DB_1 and DB_2, no table or other
   * object in database positive test case:
   * d) user1 has the privilege to create view on tables in DB_1 negative test case:
   * e) user1 cannot create view in DB_1 that select from tables in DB_2
   *  with no select privilege 2.
   * positive test case:
   * f) user1 has the privilege to create view to select from DB_1.tb_1
   *  and DB_2.tb_2 negative test case:
   * g) user1 cannot create view to select from DB_1.tb_1 and DB_2.tb_3
   */
  @Test
  public void testCrossDbViewOperations() throws Exception {
    // admin create two databases
    createDb(ADMIN1, DB1, DB2);
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement
    .execute("CREATE TABLE " + DB1 + "." + TBL1 + "(id int)");
    statement
    .execute("CREATE TABLE " + DB2 + "." + TBL1 + "(id int)");
    statement
    .execute("CREATE TABLE " + DB2 + "." + TBL2 + "(id int)");
    context.close();

    // edit policy file
    policyFile
            .addRolesToGroup(USERGROUP1, "all_db1", "load_data", "select_tb2")
            .addPermissionsToRole("all_db1", "server=server1->db=" + DB1)
            .addPermissionsToRole("all_db2", "server=server1->db=" + DB2)
            .addPermissionsToRole("select_tb2", "server=server1->db=" + DB2 + "->table=tb_1->action=select")
            .addPermissionsToRole("load_data", "server=server1->URI=file://" + dataFile.getPath())
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);

    // d
    statement.execute("DROP TABLE IF EXISTS " + DB1 + "." + TBL1);
    statement.execute("create table " + DB1 + "." + TBL1
        + " (under_col int comment 'the under column', value string)");

    // e
    statement.execute("DROP VIEW IF EXISTS " + DB1 + "." + VIEW1);
    context.assertAuthzException(statement, "CREATE VIEW " + DB1 + "." + VIEW1
        + " (value) AS SELECT value from " + DB2 + "." + TBL2
        + " LIMIT 10");
    // f
    statement.execute("DROP VIEW IF EXISTS " + DB1 + "." + VIEW2);
    statement.execute("CREATE VIEW " + DB1 + "." + VIEW2
        + " (value) AS SELECT value from " + DB1 + "." + TBL1
        + " LIMIT 10");

    // g
    statement.execute("DROP VIEW IF EXISTS " + DB1 + "." + VIEW3);
    context.assertAuthzException(statement, "CREATE VIEW " + DB1 + "." + VIEW3
        + " (value) AS SELECT value from " + DB2 + "." + TBL2
        + " LIMIT 10");
  }
}
