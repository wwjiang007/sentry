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

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.sentry.tests.e2e.hdfs.TestHDFSIntegrationBase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class TestHmsNotificationProcessingWithOutSyncOnDrop extends TestHmsNotificationProcessingBase {

  @BeforeClass
  public static void setup() throws Exception {
    hiveSyncOnDrop = false;
    hiveSyncOnCreate = true;
    hdfsSyncEnabled = true;
    TestHDFSIntegrationBase.setup();
  }

  /*
  Tests basic sanity of Hms notification processing by verifying below when new Hive objects are created
  1. Making sure that HDFS ACL rules for an new Hive objects created.
  2. Making sure that stale permissions are deleted for the new Hive object that are created.
  3. Making sure that permissions are not deleted when hive objects are deleted
  */

  @Test
  public void testHmsNotificationProcessingSanity() throws Throwable {
    dbNames = new String[]{DB1};
    roles = new String[]{"admin_role", "read_db1", "select_tbl1"};
    admin = "hive";

    Connection connection = hiveServer2.createConnection(admin, admin);
    Statement statement = connection.createStatement();
    statement.execute("create role admin_role");
    statement.execute("grant role admin_role to group hive");
    statement.execute("grant all on server server1 to role admin_role");

    // Add privileges for an objects that do not exist yet
    statement.execute("create role read_db1");
    statement.execute("create role select_tbl1");
    statement.execute("grant role read_db1 to group hbase");
    statement.execute("grant role select_tbl1 to group hbase");

    //Add object
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("use " + DB1);
    statement.execute("create table " + DB1 + "." + tableName1
            + " (under_col int comment 'the under column', value string)");

    Thread.sleep(WAIT_FOR_NOTIFICATION_PROCESSING);
    //Make sure that the privileges for that object are removed.
    verifyPrivilegesCount(statement, 0);

    //add "select" sentry permission for the object's
    statement.execute("GRANT select ON DATABASE " + DB1 + " TO ROLE read_db1");
    statement.execute("USE " + DB1);
    statement.execute("GRANT SELECT ON TABLE " + tableName1
            + " TO ROLE select_tbl1");

    verifyPrivilegesCount(statement, 2);

    // Make sure that an ACL is added for that
    verifyOnAllSubDirs("/user/hive/warehouse/db_1.db", FsAction.READ_EXECUTE, "hbase", true);
    verifyOnAllSubDirs("/user/hive/warehouse/db_1.db/tb_1", FsAction.READ_EXECUTE, "hbase", true);

    //Drop the object
    statement.execute("DROP DATABASE " + DB1 + " CASCADE");

    Thread.sleep(WAIT_FOR_NOTIFICATION_PROCESSING);
    //Make sure that the privileges added for that object are removed.
    verifyPrivilegesCount(statement, 2);
  }
}
