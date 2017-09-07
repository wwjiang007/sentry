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
import java.sql.Statement;

import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestPrivilegeAtTransform extends AbstractTestWithStaticConfiguration {
  private final String SINGLE_TYPE_DATA_FILE_NAME = "kv1.dat";
  private File dataDir;
  private File dataFile;
  private PolicyFile policyFile;

  @Override
  @Before
  public void setup() throws Exception {
    dataDir = context.getDataDir();
    dataFile = new File(dataDir, SINGLE_TYPE_DATA_FILE_NAME);
    FileOutputStream to = new FileOutputStream(dataFile);
    Resources.copy(Resources.getResource(SINGLE_TYPE_DATA_FILE_NAME), to);
    to.close();
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
  }

  /**
   * Steps:
   * 1. admin create database, create table, load data into it
   * 2. all@server can issue transforms command
   * 3. all@database cannot issue transform command
   * 4. insert@table select@table cannot issue transform command
   * 5. select@view cannot issue transform command
   * 6. transform@server can issue the transform command
   * 7. non-admin user with URI privilege on transform can execute query
   */
  @Test
  public void testTransform1() throws Exception {
    policyFile
      .addPermissionsToRole("all_db1", "server=server1->db=" + DB1)
      .addPermissionsToRole("transform_uri", "server=server1->uri=file:///bin/cat")
      .addRolesToGroup(USERGROUP1, "all_db1")
      .addRolesToGroup(USERGROUP2, "all_db1", "transform_uri")
      .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // verify by SQL
    // 1, 2
    String tableName1 = "tb_1";
    String query = "select TRANSFORM(a.under_col, a.value) "
        + "USING '/bin/cat' AS (tunder_col, tvalue) FROM " + DB1 + "." + tableName1 + " a";
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    statement.execute("DROP DATABASE IF EXISTS " + DB1 + " CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("DROP TABLE IF EXISTS " + DB1 + "." + tableName1);
    statement.execute("create table " + DB1 + "." + tableName1
        + " (under_col int, value string)");
    statement.execute("load data local inpath '" + dataFile.getPath()
            + "' into table " + DB1 + "." + tableName1);
    assertTrue(query, statement.execute(query));

    statement.close();
    connection.close();

    // 3
    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    context.assertAuthzException(statement, query);

    // 4
    policyFile
      .addPermissionsToRole("select_tb1", "server=server1->db=" + DB1 + "->table=tb_1->action=select")
      .addPermissionsToRole("insert_tb1", "server=server1->db=" + DB1 + "->table=tb_1->action=insert")
      .addRolesToGroup(USERGROUP1, "select_tb1", "insert_tb1");
    writePolicyFile(policyFile);
    context.assertAuthzException(statement, query);

    // 5
    policyFile
      .addPermissionsToRole("all_server1", "server=server1")
      .addRolesToGroup(USERGROUP1, "all_server1");
    writePolicyFile(policyFile);
    assertTrue(query, statement.execute(query));
    statement.close();
    connection.close();

    connection = context.createConnection(USER2_1);
    statement = context.createStatement(connection);
    assertTrue(query, statement.execute(query));
    statement.close();
    connection.close();

  }
}
