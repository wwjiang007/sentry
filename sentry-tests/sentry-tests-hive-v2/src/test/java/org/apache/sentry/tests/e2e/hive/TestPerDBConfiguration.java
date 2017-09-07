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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.provider.file.PolicyFile;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

/**
 * Test privileges per database policy files
 */
public class TestPerDBConfiguration extends AbstractTestWithStaticConfiguration {

  private static final String MULTI_TYPE_DATA_FILE_NAME = "emp.dat";
  private static final String DB2_POLICY_FILE = "db2-policy-file.ini";

  private static File dataFile;
  private PolicyFile policyFile;
  private static String prefix;

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Before
  public void setup() throws Exception {
    String hiveServer2 = System.getProperty("sentry.e2etest.hiveServer2Type", "InternalHiveServer2");
    String policyOnHDFS = System.getProperty("sentry.e2etest.hive.policyOnHDFS", "true");
    if(policyOnHDFS.trim().equalsIgnoreCase("true") && (hiveServer2.equals("UnmanagedHiveServer2") )){
      String policyLocation = System.getProperty("sentry.e2etest.hive.policy.location", "/user/hive/sentry");
      prefix = "hdfs://" + policyLocation + "/";
    }else {
      prefix = "file://" + context.getPolicyFile().getParent() + "/";
    }

    policyFile = super.setupPolicy();
    super.setup();
    prepareDBDataForTest();
  }

  protected static void prepareDBDataForTest() throws Exception {
    // copy data file to test dir
    dataDir = context.getDataDir();
    dataFile = new File(dataDir, MULTI_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(MULTI_TYPE_DATA_FILE_NAME), to);
    to.close();

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE tbl1(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl1");

    statement.execute("DROP DATABASE IF EXISTS " + DB2 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB2);
    statement.execute("USE " + DB2);
    statement.execute("CREATE TABLE tbl2(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl2");
    statement.execute("CREATE TABLE tbl5(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");

    statement.execute("DROP DATABASE IF EXISTS db3 CASCADE");
    statement.execute("CREATE DATABASE db3");
    statement.execute("USE db3");
    statement.execute("CREATE TABLE tbl3(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl3");

    statement.execute("DROP DATABASE IF EXISTS db4 CASCADE");
    statement.execute("CREATE DATABASE db4");
    statement.execute("USE db4");
    statement.execute("CREATE TABLE tbl4(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl4");
    statement.close();
    connection.close();
  }

  @After
  public void teardown() throws Exception {
    // one test turns this on so let's disable it in the teardown method
    System.setProperty(SentryConstants.ACCESS_ALLOW_URI_PER_DB_POLICYFILE, "false");
  }

  @Test
  public void testPerDB() throws Exception {
    PolicyFile db2PolicyFile = new PolicyFile();
    File db2PolicyFileHandle = new File(context.getPolicyFile().getParent(), DB2_POLICY_FILE);
    db2PolicyFile
        .addRolesToGroup(USERGROUP2, "select_tbl2")
        .addPermissionsToRole("select_tbl2", "server=server1->db="  + DB2 + "->table=tbl2->action=select")
        .write(db2PolicyFileHandle);

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tbl1")
        .addRolesToGroup(USERGROUP2, "select_tbl2")
        .addPermissionsToRole("select_tbl1", "server=server1->db=" + DB1 +"->table=tbl1->action=select")
        .addDatabase(DB2, prefix + db2PolicyFileHandle.getName())
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE "  + DB1);
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl1", 1, 12);

    // user1 cannot query db2.tbl2
    context.assertAuthzException(statement, "USE "  + DB2);
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM " + DB2 + ".tbl2");
    statement.close();
    connection.close();

    // test per-db file for db2

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE "  + DB2);
    // test user2 can execute query on tbl2
    verifyCount(statement, "SELECT COUNT(*) FROM tbl2", 1, 12);

    // user2 cannot query db1.tbl1
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM " + DB1 + ".tbl1");
    context.assertAuthzException(statement, "USE "  + DB1);

    statement.close();
    connection.close();

  }

  /**
   * Multiple DB files with some containing badly formatted rules
   * The privileges should work for good files
   * No access for bad formatted ones
   * @throws Exception
   */
  @Test
  public void testMultiPerDBwithErrors() throws Exception {
    String DB3_POLICY_FILE = "db3-policy-file.ini";
    String DB4_POLICY_FILE = "db4-policy-file.ini";

    File db2PolicyFileHandle = new File(context.getPolicyFile().getParent(), DB2_POLICY_FILE);
    File db3PolicyFileHandle = new File(context.getPolicyFile().getParent(), DB3_POLICY_FILE);
    File db4PolicyFileHandle = new File(context.getPolicyFile().getParent(), DB4_POLICY_FILE);

    PolicyFile db2PolicyFile = new PolicyFile();
    PolicyFile db3PolicyFile = new PolicyFile();
    PolicyFile db4PolicyFile = new PolicyFile();
    db2PolicyFile
        .addRolesToGroup(USERGROUP2, "select_tbl2")
        .addPermissionsToRole("select_tbl2", "server=server1->db="  + DB2 + "->table=tbl2->action=select")
        .write(db2PolicyFileHandle);
    db3PolicyFile
        .addRolesToGroup(USERGROUP3, "select_tbl3_BAD")
        .addPermissionsToRole("select_tbl3_BAD", "server=server1->db=db3------>table->action=select")
        .write(db3PolicyFileHandle);
    db4PolicyFile
        .addRolesToGroup(USERGROUP4, "select_tbl4")
        .addPermissionsToRole("select_tbl4", "server=server1->db=db4->table=tbl4->action=select")
        .write(db4PolicyFileHandle);
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tbl1")
        .addRolesToGroup(USERGROUP2, "select_tbl2")
        .addPermissionsToRole("select_tbl1", "server=server1->db=" + DB1 +"->table=tbl1->action=select")
        .addDatabase(DB2, prefix + db2PolicyFileHandle.getName())
        .addDatabase("db3", prefix + db3PolicyFileHandle.getName())
        .addDatabase("db4", prefix + db4PolicyFileHandle.getName())
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE "  + DB1);
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl1", 1, 12);
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE "  + DB2);
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl2", 1, 12);
    connection.close();

    // verify no access to db3 due to badly formatted rule in db3 policy file
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    context.assertAuthzException(statement, "USE db3");
    // test user1 can execute query on tbl1
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM db3.tbl3");
    connection.close();

    connection = context.createConnection(USER4_1);
    statement = context.createStatement(connection);
    statement.execute("USE db4");
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl4", 1, 12);
    connection.close();
  }

  @Test
  public void testPerDBPolicyFileWithURI() throws Exception {
    File db2PolicyFileHandle = new File(context.getPolicyFile().getParent(), DB2_POLICY_FILE);
    PolicyFile db2PolicyFile = new PolicyFile();
    db2PolicyFile
        .addRolesToGroup(USERGROUP2, "select_tbl5", "data_read", "insert_tbl5")
        .addPermissionsToRole("select_tbl5",
            "server=server1->db=" + DB2 + "->table=tbl5->action=select")
        .addPermissionsToRole("insert_tbl5",
            "server=server1->db=" + DB2 + "->table=tbl5->action=insert")
        .addPermissionsToRole("data_read", "server=server1->URI=file://" + dataFile)
        .write(db2PolicyFileHandle);

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tbl1")
        .addRolesToGroup(USERGROUP2, "select_tbl5")
        .addPermissionsToRole("select_tbl1", "server=server1->db=" + DB1 +"->table=tbl1->action=select")
        .addDatabase(DB2, prefix + db2PolicyFileHandle.getName())
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    // ugly hack: needs to go away once this becomes a config property. Note that this property
    // will not be set with external HS and this test will fail. Hope is this fix will go away
    // by then.
    System.setProperty(SentryConstants.ACCESS_ALLOW_URI_PER_DB_POLICYFILE, "true");
    // test execution
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE "  + DB1);
    // test user1 can execute query on tbl1
    verifyCount(statement, "SELECT COUNT(*) FROM tbl1", 1, 12);

    // user1 cannot query db2.tbl5
    context.assertAuthzException(statement, "USE "  + DB2);
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM " + DB2 + ".tbl5");
    statement.close();
    connection.close();

    // test per-db file for db2
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE "  + DB2);
    // test user2 can execute query on tbl5
    verifyCount(statement, "SELECT COUNT(*) FROM tbl5", 1, 0);

    // verify user2 can execute LOAD
    statement.execute("LOAD DATA LOCAL INPATH '" + dataFile.getPath() + "' INTO TABLE tbl5");

    // user2 cannot query db1.tbl1
    context.assertAuthzException(statement, "SELECT COUNT(*) FROM " + DB1 + ".tbl1");
    context.assertAuthzException(statement, "USE "  + DB1);

    // once we disable this property all queries should fail
    System.setProperty(SentryConstants.ACCESS_ALLOW_URI_PER_DB_POLICYFILE, "false");
    context.assertAuthzException(statement, "USE "  + DB2);

    // re-enable for clean
    System.setProperty(SentryConstants.ACCESS_ALLOW_URI_PER_DB_POLICYFILE, "true");

    statement.close();
    connection.close();
  }

  /**
   * Test 'use default' statement. It should work as long as the user as privilege to assess any object in system
   * @throws Exception
   */
  @Test
  public void testDefaultDb() throws Exception {
    policyFile
        .addRolesToGroup(USERGROUP1, "select_tbl1")
        .addPermissionsToRole("select_tbl1", "server=server1->db=" + DB1 +"->table=tbl1->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    // user_1 should be able to access default
    Connection connection = context.createConnection(USER1_1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE default");
    statement.close();
    connection.close();

    // user_2 should NOT be able to access default since it does have access to any other object
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    context.assertAuthzException(statement, "USE default");
    statement.close();
    connection.close();

  }

  @Test
  public void testDefaultDBwithDbPolicy() throws Exception {
    File db2PolicyFileHandle = new File(context.getPolicyFile().getParent(), DB2_POLICY_FILE);
    File defaultPolicyFileHandle = new File(context.getPolicyFile().getParent(), "default.ini");

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tbl1")
        .addRolesToGroup(USERGROUP2, "select_tbl2")
        .addPermissionsToRole("select_tbl1", "server=server1->db=" + DB1 +"->table=tbl1->action=select")
        .addDatabase(DB2, prefix + db2PolicyFileHandle.getName())
        .addDatabase("default", prefix + defaultPolicyFileHandle.getName())
        .setUserGroupMapping(StaticUserGroup.getStaticMapping())
        .write(context.getPolicyFile());

    PolicyFile db2PolicyFile = new PolicyFile();
    db2PolicyFile
        .addRolesToGroup(USERGROUP2, "select_tbl2")
        .addPermissionsToRole("select_tbl2", "server=server1->db="  + DB2 + "->table=tbl2->action=select")
        .write(db2PolicyFileHandle);

    PolicyFile defaultPolicyFile = new PolicyFile();
    defaultPolicyFile
        .addRolesToGroup(USERGROUP2, "select_def")
        .addPermissionsToRole("select_def", "server=server1->db=default->table=dtab->action=select")
        .write(defaultPolicyFileHandle);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("USE default");
    statement.execute("DROP TABLE IF EXISTS dtab");
    statement.execute("CREATE TABLE dtab(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    // user_1 should be able to switch to default, but not the tables from default
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE "  + DB1);
    statement.execute("USE default");
    context.assertAuthzException(statement, "SELECT * FROM dtab");
    statement.execute("USE "  + DB1);
    context.assertAuthzException(statement, "SELECT * FROM default.dtab");

    statement.close();
    connection.close();

    // user_2 should be able to access default and select from default's tables
    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    statement.execute("USE "  + DB2);
    statement.execute("USE default");
    statement.execute("SELECT * FROM dtab");
    statement.execute("USE "  + DB2);
    statement.execute("SELECT * FROM default.dtab");
    statement.close();
    connection.close();

    // user_3 should NOT be able to switch to default since it doesn't have access to any objects
    connection = context.createConnection(USER3_1);
    statement = context.createStatement(connection);
    context.assertAuthzException(statement, "USE default");
    statement.close();
    connection.close();
  }

  private void verifyCount(Statement statement, String query, int exceptedCountRows,
      int exceptedCount) throws SQLException {
    ResultSet resultSet = statement.executeQuery(query);
    int count = 0;
    int countRows = 0;

    while (resultSet.next()) {
      count = resultSet.getInt(1);
      countRows++;
    }
    assertTrue("Incorrect row count:" + countRows, countRows == exceptedCountRows);
    assertTrue("Incorrect result:" + count, count == exceptedCount);
  }
}
