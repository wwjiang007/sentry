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

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.google.common.io.Resources;
import org.junit.Assert;

import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.tests.e2e.hive.hiveserver.HiveServerFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUriPermissions extends AbstractTestWithStaticConfiguration {
  private PolicyFile policyFile;
  private File dataFile;
  private String loadData;

  @BeforeClass
  public static void setupTestStaticConfiguration () throws Exception {
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Before
  public void setup() throws Exception {
    policyFile = super.setupPolicy();
    super.setup();
  }

  // test load data into table
  @Test
  public void testLoadPrivileges() throws Exception {
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    loadData = "server=server1->uri=file://" + dataFile.getPath();

    String tabName = "tab1";
    Connection userConn = null;
    Statement userStmt = null;

    // create dbs
    Connection adminCon = context.createConnection(ADMIN1);
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("CREATE DATABASE " + DB1);
    adminStmt.execute("use " + DB1);
    adminStmt.execute("CREATE TABLE " + tabName + "(id int)");
    context.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "db1_read", "db1_write", "data_read")
            .addRolesToGroup(USERGROUP2, "db1_write")
            .addPermissionsToRole("db1_write", "server=server1->db=" + DB1 + "->table=" + tabName + "->action=INSERT")
            .addPermissionsToRole("db1_read", "server=server1->db=" + DB1 + "->table=" + tabName + "->action=SELECT")
            .addPermissionsToRole("data_read", loadData);
    writePolicyFile(policyFile);

    // positive test, user1 has access to file being loaded
    userConn = context.createConnection(USER1_1);
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + DB1);
    userStmt.execute("load data local inpath 'file://" + dataFile.getPath() +
        "' into table " + tabName);
    userStmt.execute("select * from " + tabName + " limit 1");
    ResultSet res = userStmt.getResultSet();
    Assert.assertTrue("Table should have data after load", res.next());
    res.close();
    context.close();

    // Negative test, user2 doesn't have access to the file being loaded
    userConn = context.createConnection(USER2_1);
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + DB1);
    context.assertAuthzException(userStmt, "load data local inpath 'file://" + dataFile.getPath() +
        "' into table " + tabName);
    userStmt.close();
    userConn.close();
  }

  // Test alter partition location
  @Test
  public void testAlterPartitionLocationPrivileges() throws Exception {
    String tabName = "tab1";
    String newPartitionDir = "foo";
    String tabDir = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR) +
      "/" + tabName + "/" + newPartitionDir;
    Connection userConn = null;
    Statement userStmt = null;

    // create dbs
    Connection adminCon = context.createConnection(ADMIN1);
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("CREATE DATABASE " + DB1);
    adminStmt.execute("use " + DB1);
    adminStmt.execute("CREATE TABLE " + tabName + " (id int) PARTITIONED BY (dt string)");
    adminCon.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "db1_all", "data_read")
        .addRolesToGroup(USERGROUP2, "db1_all")
        .addRolesToGroup(USERGROUP3, "db1_tab1_all", "data_read")
        .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
        .addPermissionsToRole("db1_tab1_all", "server=server1->db=" + DB1 + "->table=" + tabName)
        .addPermissionsToRole("data_read", "server=server1->uri=" + tabDir);
    writePolicyFile(policyFile);


    // positive test: user1 has privilege to alter table add partition but not set location
    userConn = context.createConnection(USER1_1);
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + DB1);
    userStmt.execute("ALTER TABLE " + tabName + " ADD IF NOT EXISTS PARTITION (dt = '21-Dec-2012') " +
            " LOCATION '" + tabDir + "'");
    userStmt.execute("ALTER TABLE " + tabName + " DROP PARTITION (dt = '21-Dec-2012')");
    userStmt.execute("ALTER TABLE " + tabName + " ADD PARTITION (dt = '21-Dec-2012') " +
        " LOCATION '" + tabDir + "'");
    userStmt.execute(
        "ALTER TABLE " + tabName + " PARTITION (dt = '21-Dec-2012') " + " SET LOCATION '" + tabDir + "'");
    userConn.close();

    // negative test: user2 doesn't have privilege to alter table add partition
    userConn = context.createConnection(USER2_1);
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + DB1);
    context.assertAuthzException(userStmt,
        "ALTER TABLE " + tabName + " ADD PARTITION (dt = '22-Dec-2012') " +
          " LOCATION '" + tabDir + "/foo'");
    // positive test, user2 can alter managed partitions
    userStmt.execute("ALTER TABLE " + tabName + " ADD PARTITION (dt = '22-Dec-2012')");
    userStmt.execute("ALTER TABLE " + tabName + " DROP PARTITION (dt = '22-Dec-2012')");
    userStmt.execute("ALTER TABLE " + tabName + " ADD IF NOT EXISTS PARTITION (dt = '22-Dec-2012')");
    userStmt.execute("ALTER TABLE " + tabName + " DROP PARTITION (dt = '22-Dec-2012')");
    userConn.close();

    // positive test: user3 has privilege to add/drop partitions
    userConn = context.createConnection(USER3_1);
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + DB1);
    userStmt.execute(
        "ALTER TABLE " + tabName + " ADD PARTITION (dt = '22-Dec-2012') " +
          " LOCATION '" + tabDir + "/foo'");
    userStmt.execute(
        "ALTER TABLE " + tabName + " DROP PARTITION (dt = '21-Dec-2012')");

    userStmt.close();
    userConn.close();
  }

  // test alter table set location
  @Test
  public void testAlterTableLocationPrivileges() throws Exception {
    String tabName = "tab1";
    String tabDir = hiveServer.getProperty(HiveServerFactory.WAREHOUSE_DIR) + "/" + tabName;
    Connection userConn = null;
    Statement userStmt = null;

    // create dbs
    Connection adminCon = context.createConnection(ADMIN1);
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("CREATE DATABASE " + DB1);
    adminStmt.execute("use " + DB1);
    adminStmt.execute("CREATE TABLE " + tabName + " (id int)  PARTITIONED BY (dt string)");
    adminCon.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "server1_all")
            .addRolesToGroup(USERGROUP2, "db1_all, data_read")
            .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
            .addPermissionsToRole("data_read", "server=server1->URI=" + tabDir)
            .addPermissionsToRole("server1_all", "server=server1");
    writePolicyFile(policyFile);

    // positive test: user2 has privilege to alter table set partition
    userConn = context.createConnection(USER2_1);
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + DB1);
    userStmt.execute(
        "ALTER TABLE " + tabName + " SET LOCATION '" + tabDir +  "'");
    userConn.close();

    // positive test: user1 has privilege to alter table set partition
    userConn = context.createConnection(USER1_1);
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + DB1);
    userStmt.execute("ALTER TABLE " + tabName + " SET LOCATION '" + tabDir + "'");
    userConn.close();
  }

  // Test external table
  @Test
  public void testExternalTablePrivileges() throws Exception {
    Connection userConn = null;
    Statement userStmt = null;

    String dataDirPath = "file://" + dataDir;
    String tableDir = dataDirPath + "/" + Math.random();

    //Hive needs write permissions on this local directory
    baseDir.setWritable(true, false);
    dataDir.setWritable(true, false);

    // create dbs
    Connection adminCon = context.createConnection(ADMIN1);
    Statement adminStmt = context.createStatement(adminCon);
    adminStmt.execute("use default");
    adminStmt.execute("CREATE DATABASE " + DB1);
    adminStmt.close();
    adminCon.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "db1_all", "data_read")
            .addRolesToGroup(USERGROUP2, "db1_all")
            .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
            .addPermissionsToRole("data_read", "server=server1->URI=" + dataDirPath);
    writePolicyFile(policyFile);

    // negative test: user2 doesn't have privilege to create external table in given path
    userConn = context.createConnection(USER2_1);
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + DB1);
    context.assertAuthzException(userStmt,
        "CREATE EXTERNAL TABLE extab1(id INT) LOCATION '" + tableDir + "'");
    context.assertAuthzException(userStmt, "CREATE TABLE extab1(id INT) LOCATION '" + tableDir + "'");
    userStmt.close();
    userConn.close();

    // positive test: user1 has privilege to create external table in given path
    userConn = context.createConnection(USER1_1);
    userStmt = context.createStatement(userConn);
    userStmt.execute("use " + DB1);
    userStmt.execute("CREATE EXTERNAL TABLE extab1(id INT) LOCATION '" + tableDir + "'");
    userStmt.execute("DROP TABLE extab1");
    userStmt.execute("CREATE TABLE extab1(id INT) LOCATION '" + tableDir + "'");
    userStmt.close();
    userConn.close();
  }

}
