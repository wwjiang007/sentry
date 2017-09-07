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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

/* Tests privileges at table scope within a single database.
 */

public class TestPrivilegesAtDatabaseScope extends AbstractTestWithStaticConfiguration {
  private PolicyFile policyFile;

  Map <String, String >testProperties;
  private static final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";

  @BeforeClass
  public static void setupTestStaticConfiguration () throws Exception {
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Override
  @Before
  public void setup() throws Exception {
    policyFile = super.setupPolicy();
    super.setup();
    testProperties = new HashMap<String, String>();
  }

  // SENTRY-285 test
  @Test
  public void testAllOnDb() throws Exception {
    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("create database " + DB1);
    statement.execute("create table " + DB1 + ".tab1(a int)");

    policyFile
            .addRolesToGroup(USERGROUP1, "all_db1")
            .addPermissionsToRole("all_db1", "server=server1->db=" + DB1 + "->action=all")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    statement.execute("select * from tab1");

    policyFile
        .addPermissionsToRole("all_db1", "server=server1->db=" + DB1);
    writePolicyFile(policyFile);
    statement.execute("use " + DB1);
    statement.execute("select * from tab1");
  }


  /* Admin creates database DB_1
   * Admin grants ALL to USER_GROUP of which user1 is a member.
   */
  @Test
  public void testAllPrivilege() throws Exception {

    //copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.close();
    connection.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "all_db1", "load_data")
            .addRolesToGroup(USERGROUP2, "all_db2")
            .addPermissionsToRole("all_db1", "server=server1->db=" + DB1)
            .addPermissionsToRole("all_db2", "server=server1->db=" + DB2)
            .addPermissionsToRole("load_data", "server=server1->uri=file://" + dataFile.getPath())
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // test user can create table
    statement.execute("CREATE TABLE " + DB1 + ".TAB_1(A STRING)");
    // test user can execute load
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE " + DB1 + ".TAB_1");
    statement.execute("CREATE TABLE " + DB1 + ".TAB_2(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE " + DB1 + ".TAB_2");

    // test CTAS can reference UDFs
    statement.execute("USE " + DB1);
    statement.execute("create table table2 as select A, count(A) from TAB_1 GROUP BY A");

    // test user can switch db
    statement.execute("USE " + DB1);
    //test user can create view
    statement.execute("CREATE VIEW VIEW_1(A) AS SELECT A FROM TAB_1");

    // test user can insert
    statement.execute("INSERT INTO TABLE TAB_1 SELECT A FROM TAB_2");
    // test user can query table
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(A) FROM TAB_1");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 1000);

    // test user can execute alter table rename
    statement.execute("ALTER TABLE TAB_1 RENAME TO TAB_3");

    // test user can execute create as select
    statement.execute("CREATE TABLE TAB_4 AS SELECT * FROM TAB_2");

    // test user can execute alter table rename cols
    statement.execute("ALTER TABLE TAB_3 ADD COLUMNS (B INT)");

    // test user can drop table
    statement.execute("DROP TABLE TAB_3");

    //negative test case: user can't drop another user's database
    try {
      statement.execute("DROP DATABASE " + DB2 + " CASCADE");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    //negative test case: user can't switch into another user's database
    try {
      statement.execute("USE " + DB2);
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    //User can drop own database
    statement.execute("DROP DATABASE " + DB1 + " CASCADE");

    statement.close();
    connection.close();
  }

  /* Admin creates database DB_1, creates table TAB_1, loads data into it
   * Admin grants ALL to USER_GROUP of which user1 is a member.
   */
  @Test
  public void testAllPrivilegeOnObjectOwnedByAdmin() throws Exception {

    //copy data file to test dir
    File dataDir = context.getDataDir();
    File dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    File externalTblDir = new File(dataDir, "exttab");
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE TABLE PART_TAB_1(A STRING) partitioned by (B INT) STORED AS TEXTFILE");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE PART_TAB_1 PARTITION(B=1)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE PART_TAB_1 PARTITION(B=2)");
    statement.close();
    connection.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "all_db1", "load_data", "exttab")
            .addRolesToGroup(USERGROUP2, "all_db2")
            .addPermissionsToRole("all_db1", "server=server1->db=" + DB1)
            .addPermissionsToRole("all_db2", "server=server1->db=" + DB2)
            .addPermissionsToRole("exttab", "server=server1->uri=file://" + dataDir.getPath())
            .addPermissionsToRole("load_data", "server=server1->uri=file://" + dataFile.getPath())
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // test execution
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // test user can switch db
    statement.execute("USE " + DB1);
    // test user can execute load
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_1");
    statement.execute("CREATE TABLE TAB_2(A STRING)");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE TAB_2");

    //test user can create view
    statement.execute("CREATE VIEW VIEW_1(A) AS SELECT A FROM TAB_1");

    // test user can insert
    statement.execute("INSERT INTO TABLE TAB_1 SELECT A FROM TAB_2");
    // test user can query table
    ResultSet resultSet = statement.executeQuery("SELECT COUNT(A) FROM TAB_1");
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count", countRows == 1);
    assertTrue("Incorrect result", count == 1500);

    // test user can execute alter table rename
    statement.execute("ALTER TABLE TAB_1 RENAME TO TAB_3");

    // test user can drop table
    statement.execute("DROP TABLE TAB_3");

    //positive test case: user can create external tables at given location
    assertTrue("Unable to create directory for external table test" , externalTblDir.mkdir());
    statement.execute("CREATE EXTERNAL TABLE EXT_TAB_1(A STRING) STORED AS TEXTFILE LOCATION 'file:"+
                        externalTblDir.getAbsolutePath() + "'");

    //negative test case: user can't execute alter table set location,
    // as the user does not have privileges on that location
    context.assertSentrySemanticException(statement, "ALTER TABLE TAB_2 SET LOCATION 'file:///tab2'", semanticException);

    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("CREATE EXTERNAL TABLE EXT_TAB_1(A STRING) STORED AS TEXTFILE LOCATION 'file:"+
        externalTblDir.getAbsolutePath() + "'");
      Assert.fail("Expected SQL exception");
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }

    statement.close();
    connection.close();
  }

  /**
   * Test privileges for 'use <db>'
   * Admin should be able to run use <db> with server level access
   * User with db level access should be able to run use <db>
   * User with table level access should be able to run use <db>
   * User with no access to that db objects, should NOT be able run use <db>
   * @throws Exception
   */
  @Test
  public void testUseDbPrivilege() throws Exception {
    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("use " + DB2);
    statement.execute("CREATE TABLE TAB_2(A STRING)");
    context.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "all_db1")
            .addRolesToGroup(USERGROUP2, "select_db2")
            .addRolesToGroup(USERGROUP3, "all_db3")
            .addPermissionsToRole("all_db1", "server=server1->db=" + DB1)
            .addPermissionsToRole("select_db2", "server=server1->db=" + DB2 + "->table=tab_2->action=select")
            .addPermissionsToRole("all_db3", "server=server1->db=DB_3")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // user1 should be able to connect db_1
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("use " + DB1);
    context.close();

    // user2 should not be able to connect db_1
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("use " + DB1);
      assertFalse("user2 shouldn't be able switch to " + DB1, true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    statement.execute("use " + DB2);
    context.close();

    // user3 who is not listed in policy file should not be able to connect db_2
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    try {
      statement.execute("use " + DB2);
      assertFalse("user3 shouldn't be able switch to " + DB2, true);
    } catch (SQLException e) {
      context.verifyAuthzException(e);
    }
    context.close();
  }

  /**
   * Test access to default DB with out of box authz config
   * All users should be able to switch to default, including the users that don't have any
   * privilege on default db objects via policy file
   * @throws Exception
   */
  @Test
  public void testDefaultDbPrivilege() throws Exception {
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("use default");
    statement.execute("create table tab1(a int)");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("use " + DB1);
    statement.execute("CREATE TABLE TAB_1(A STRING)");
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("use " + DB2);
    statement.execute("CREATE TABLE TAB_2(A STRING)");
    context.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "all_db1")
            .addRolesToGroup(USERGROUP2, "select_db2")
            .addRolesToGroup(USERGROUP3, "all_default")
            .addPermissionsToRole("all_db1", "server=server1->db=" + DB1)
            .addPermissionsToRole("select_db2", "server=server1->db=" + DB2 + "->table=tab_2->action=select")
            .addPermissionsToRole("all_default", "server=server1->db=default")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("use default");
    try {
      statement.execute("select * from tab1");
      assertTrue("Should not be allowed !!", false);
    } catch (Exception e) {
      // Ignore
    }
    context.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();

    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    statement.execute("use default");
    context.close();
  }

}
