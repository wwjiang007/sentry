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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

/**
 * Metadata tests for show tables and show databases. * Unlike rest of the
 * access privilege validation which is handled in semantic hooks, these
 * statements are validaed via a runtime fetch hook
 */
public class TestRuntimeMetadataRetrieval extends AbstractTestWithStaticConfiguration {
  private PolicyFile policyFile;
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataDir;
  private File dataFile;

  @BeforeClass
  public static void setupTestStaticConfiguration () throws Exception {
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
  }

  @Before
  public void setup() throws Exception {
    policyFile = super.setupPolicy();
    super.setup();
    dataDir = context.getDataDir();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
  }

  /**
   * Steps: 1. admin create db_1 and db_1.tb_1
   *        2. admin should see all tables
   *        3. user1 should only see the tables it has any level of privilege
   */
  @Test
  public void testShowTables1() throws Exception {
    // tables visible to user1 (not access to tb_4
    String tableNames[] = {"tb_1", "tb_2", "tb_3", "tb_4"};
    List<String> tableNamesValidation = new ArrayList<String>();

    String user1TableNames[] = {"tb_1", "tb_2", "tb_3"};

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    createTabs(statement, DB1, tableNames);
    // Admin should see all tables
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    tableNamesValidation.addAll(Arrays.asList(tableNames));

    validateTables(rs, tableNamesValidation);
    statement.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "tab1_priv,tab2_priv,tab3_priv")
            .addPermissionsToRole("tab1_priv", "server=server1->db=" + DB1 + "->table="
                    + tableNames[0] + "->action=select")
            .addPermissionsToRole("tab2_priv", "server=server1->db=" + DB1 + "->table="
                    + tableNames[1] + "->action=insert")
            .addPermissionsToRole("tab3_priv", "server=server1->db=" + DB1 + "->table="
                    + tableNames[2] + "->action=select")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // User1 should see tables with any level of access
    rs = statement.executeQuery("SHOW TABLES");
    tableNamesValidation.addAll(Arrays.asList(user1TableNames));
    validateTables(rs, tableNamesValidation);
    statement.close();
  }

  /**
   * Steps: 1. admin create db_1 and tables
   * 2. admin should see all tables
   * 3. user1 should only see the all tables with db level privilege
   */
  @Test
  public void testShowTables2() throws Exception {
    // tables visible to user1 (not access to tb_4
    String tableNames[] = {"tb_1", "tb_2", "tb_3", "tb_4"};
    List<String> tableNamesValidation = new ArrayList<String>();

    String user1TableNames[] = {"tb_1", "tb_2", "tb_3", "tb_4"};

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    createTabs(statement, DB1, tableNames);
    // Admin should see all tables
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    tableNamesValidation.addAll(Arrays.asList(tableNames));
    validateTables(rs, tableNamesValidation);
    statement.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "db_priv")
            .addPermissionsToRole("db_priv", "server=server1->db=" + DB1)
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // User1 should see tables with any level of access
    rs = statement.executeQuery("SHOW TABLES");
    tableNamesValidation.addAll(Arrays.asList(user1TableNames));
    validateTables(rs, tableNamesValidation);
    statement.close();
  }

  /**
   * Steps: 1. admin create db_1 and db_1.tb_1
   *        2. admin should see all tables
   *        3. user1 should only see the tables he/she has any level of privilege
   */
  @Test
  public void testShowTables3() throws Exception {
    // tables visible to user1 (not access to tb_4
    String tableNames[] = {"tb_1", "tb_2", "tb_3", "newtab_3"};
    List<String> tableNamesValidation = new ArrayList<String>();

    String adminTableNames[] = {"tb_3", "newtab_3", "tb_2", "tb_1"};
    String user1TableNames[] = {"newtab_3"};

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    createTabs(statement, DB1, tableNames);
    // Admin should see all tables
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    tableNamesValidation.addAll(Arrays.asList(adminTableNames));
    validateTables(rs, tableNamesValidation);
    statement.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "tab_priv")
            .addPermissionsToRole("tab_priv", "server=server1->db=" + DB1 + "->table="
                    + tableNames[3] + "->action=insert")
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // User1 should see tables with any level of access
    rs = statement.executeQuery("SHOW TABLES");
    tableNamesValidation.addAll(Arrays.asList(user1TableNames));
    validateTables(rs, tableNamesValidation);
    statement.close();
  }

  /**
   * Steps: 1. admin create db_1 and db_1.tb_1
   *        2. admin should see all tables
   *        3. user1 should only see the tables with db level privilege
   */
  @Test
  public void testShowTables4() throws Exception {
    String tableNames[] = {"tb_1", "tb_2", "tb_3", "newtab_3"};
    List<String> tableNamesValidation = new ArrayList<String>();

    String adminTableNames[] = {"tb_3", "newtab_3", "tb_1", "tb_2"};
    String user1TableNames[] = {"tb_3", "newtab_3", "tb_1", "tb_2"};

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    createTabs(statement, DB1, tableNames);
    // Admin should be able to see all tables
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    tableNamesValidation.addAll(Arrays.asList(adminTableNames));
    validateTables(rs, tableNamesValidation);
    statement.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "tab_priv")
            .addPermissionsToRole("tab_priv", "server=server1->db=" + DB1)
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // User1 should see tables with any level of access
    rs = statement.executeQuery("SHOW TABLES");
    tableNamesValidation.addAll(Arrays.asList(user1TableNames));
    validateTables(rs, tableNamesValidation);
    statement.close();
  }

  /**
   * Steps: 1. admin creates tables in default db
   *        2. user1 shouldn't see any table when he/she doesn't have any privilege on default
   */
  @Test
  public void testShowTables5() throws Exception {
    String tableNames[] = {"tb_1", "tb_2", "tb_3", "tb_4"};

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    createTabs(statement, "default", tableNames);

    policyFile
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    // User1 should see tables with any level of access
    ResultSet rs = statement.executeQuery("SHOW TABLES");
    // user1 doesn't have access to any tables in default db
    Assert.assertFalse(rs.next());
    statement.close();
  }

  /**
   * Steps: 1. admin create db_1 and tb_1, tb_2, tb_3, tb_4 and table_5
   *        2. admin should see all tables except table_5 which does not match tb*
   *        3. user1 should only see the matched tables it has any level of privilege
   */
  @Test
  public void testShowTablesExtended() throws Exception {
    // tables visible to user1 (not access to tb_4
    String tableNames[] = {"tb_1", "tb_2", "tb_3", "tb_4", "table_5"};
    List<String> tableNamesValidation = new ArrayList<String>();

    String user1TableNames[] = {"tb_1", "tb_2", "tb_3"};

    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    createTabs(statement, DB1, tableNames);

    policyFile
        .addRolesToGroup(USERGROUP1, "tab1_priv,tab2_priv,tab3_priv")
        .addPermissionsToRole("tab1_priv", "server=server1->db=" + DB1 + "->table="
            + tableNames[0] + "->action=select")
        .addPermissionsToRole("tab2_priv", "server=server1->db=" + DB1 + "->table="
            + tableNames[1] + "->action=insert")
        .addPermissionsToRole("tab3_priv", "server=server1->db=" + DB1 + "->table="
            + tableNames[2] + "->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // Admin should see all tables except table_5, the one does not match the pattern
    ResultSet rs = statement.executeQuery("SHOW TABLE EXTENDED IN " + DB1 + " LIKE 'tb*'");
    tableNamesValidation.addAll(Arrays.asList(tableNames).subList(0, 4));
    validateTablesInRs(rs, tableNamesValidation);
    statement.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("USE " + DB1);
    // User1 should see tables with any level of access
    rs = statement.executeQuery("SHOW TABLE EXTENDED IN " + DB1 + " LIKE 'tb*'");
    tableNamesValidation.addAll(Arrays.asList(user1TableNames));
    validateTablesInRs(rs, tableNamesValidation);
    statement.close();
  }

  /**
   * Steps: 1. admin create few dbs
   *        2. admin can do show databases
   *        3. users with db level permissions should only those dbs on 'show database'
   */
  @Test
  public void testShowDatabases1() throws Exception {
    List<String> dbNamesValidation = new ArrayList<String>();
    String[] dbNames = {DB1, DB2, DB3};
    String[] user1DbNames = {DB1};

    createDb(ADMIN1, dbNames);
    dbNamesValidation.addAll(Arrays.asList(dbNames));
    dbNamesValidation.add("default");
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    validateDBs(rs, dbNamesValidation); // admin should see all dbs
    rs.close();

    policyFile
            .addRolesToGroup(USERGROUP1, "db1_all")
            .addPermissionsToRole("db1_all", "server=server1->db=" + DB1)
            .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    rs = statement.executeQuery("SHOW DATABASES");
    dbNamesValidation.addAll(Arrays.asList(user1DbNames));
    dbNamesValidation.add("default");
    // user should see only dbs with access
    validateDBs(rs, dbNamesValidation);
    rs.close();
  }

  /**
   * Steps: 1. admin create few dbs
   *        2. admin can do show databases
   *        3. users with table level permissions should should only those parent dbs on 'show
   *           database'
   */
  @Test
  public void testShowDatabases2() throws Exception {
    String[] dbNames = {DB1, DB2, DB3};
    List<String> dbNamesValidation = new ArrayList<String>();
    String[] user1DbNames = {DB1, DB2};
    String tableNames[] = {"tb_1"};

    // verify by SQL
    // 1, 2
    createDb(ADMIN1, dbNames);
    dbNamesValidation.addAll(Arrays.asList(dbNames));
    dbNamesValidation.add("default");
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    createTabs(statement, DB1, tableNames);
    createTabs(statement, DB2, tableNames);
    ResultSet rs = statement.executeQuery("SHOW DATABASES");
    validateDBs(rs, dbNamesValidation); // admin should see all dbs
    rs.close();

    policyFile
        .addRolesToGroup(USERGROUP1, "db1_tab,db2_tab")
        .addPermissionsToRole("db1_tab", "server=server1->db=" + DB1 + "->table=tb_1->action=select")
        .addPermissionsToRole("db2_tab", "server=server1->db=" + DB2 + "->table=tb_1->action=insert")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    rs = statement.executeQuery("SHOW DATABASES");
    dbNamesValidation.addAll(Arrays.asList(user1DbNames));
    dbNamesValidation.add("default");
    // user should see only dbs with access
    validateDBs(rs, dbNamesValidation);
    rs.close();
  }

  // compare the table resultset with given array of table names
  private void validateDBs(ResultSet rs, List<String> dbNames)
      throws SQLException {
    while (rs.next()) {
      String dbName = rs.getString(1);
      //There might be non test related dbs in the system
      if(dbNames.contains(dbName.toLowerCase())) {
        dbNames.remove(dbName.toLowerCase());
      }
    }
    Assert.assertTrue(dbNames.toString(), dbNames.isEmpty());
  }

  // Create the give tables
  private void createTabs(Statement statement, String dbName,
      String tableNames[]) throws SQLException {
    for (String tabName : tableNames) {
      statement.execute("DROP TABLE IF EXISTS " + dbName + "." + tabName);
      statement.execute("create table " + dbName + "." + tabName
          + " (under_col int comment 'the under column', value string)");
    }
  }

  // compare the table resultset with given array of table names
  private void validateTables(ResultSet rs, List<String> tableNames) throws SQLException {
    while (rs.next()) {
      String tableName = rs.getString(1);
      Assert.assertTrue(tableName, tableNames.remove(tableName.toLowerCase()));
    }
    Assert.assertTrue(tableNames.toString(), tableNames.isEmpty());
    rs.close();
  }

  // compare the tables in resultset with given array of table names
  // for some hive query like 'show table extended ...', the resultset does
  // not only contains tableName (See HIVE-8109)
  private void validateTablesInRs(ResultSet rs, List<String> tableNames) throws SQLException {
    while (rs.next()) {
      String tableName = rs.getString(1);
      if (tableName.startsWith("tableName:")) {
        Assert.assertTrue("Expected table " + tableName.substring(10),
            tableNames.remove(tableName.substring(10).toLowerCase()));
      }
    }
    Assert.assertTrue(tableNames.toString(), tableNames.isEmpty());
    rs.close();
  }
}
