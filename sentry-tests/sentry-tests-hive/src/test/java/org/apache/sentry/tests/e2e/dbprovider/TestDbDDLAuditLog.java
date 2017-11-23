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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sentry.provider.db.log.appender.AuditLoggerTestAppender;
import org.apache.sentry.provider.db.log.util.CommandUtil;
import org.apache.sentry.provider.db.log.util.Constants;
import org.apache.sentry.tests.e2e.hive.AbstractTestWithStaticConfiguration;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDbDDLAuditLog extends AbstractTestWithStaticConfiguration {

  @BeforeClass
  public static void setupTestStaticConfiguration() throws Exception {
    useSentryService = true;
    AbstractTestWithStaticConfiguration.setupTestStaticConfiguration();
    AuditLoggerTestAppender testAppender = new AuditLoggerTestAppender();
    Logger logger = Logger.getLogger(Constants.AUDIT_LOGGER_NAME);
    logger.addAppender(testAppender);
    logger.setLevel(Level.INFO);
  }

  @Override
  @Before
  public void setup() throws Exception {
    super.setupAdmin();
    super.setup();
  }

  @Test
  public void testBasic() throws Exception {
    String roleName = "testRole";
    String groupName = "testGroup";
    String userName = "testUser";
    String dbName = "dbTest";
    String tableName = "tableTest";
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);
    Map<String, String> fieldValueMap = new HashMap<String, String>();

    // for success audit log
    statement.execute("CREATE ROLE " + roleName);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_CREATE_ROLE);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "CREATE ROLE " + roleName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
    assertAuditLog(fieldValueMap);

    statement.execute("GRANT ROLE " + roleName + " TO GROUP " + groupName);
    fieldValueMap.clear();
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_ADD_ROLE);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ROLE " + roleName + " TO GROUP "
        + groupName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
    assertAuditLog(fieldValueMap);

    statement.execute("GRANT ROLE " + roleName + " TO USER " + userName);
    fieldValueMap.clear();
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_ADD_ROLE_USER);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ROLE " + roleName + " TO USER "
        + userName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    assertAuditLog(fieldValueMap);

    statement.execute("create database " + dbName);
    statement.execute("use " + dbName);
    statement.execute("CREATE TABLE " + tableName + " (c1 string)");

    statement.execute("GRANT ALL ON DATABASE " + dbName + " TO ROLE " + roleName);
    fieldValueMap.clear();
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ALL ON DATABASE " + dbName
        + " TO ROLE " + roleName);
    fieldValueMap.put(Constants.LOG_FIELD_DATABASE_NAME, dbName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
    assertAuditLog(fieldValueMap);

    statement.execute("GRANT SELECT ON TABLE " + tableName + " TO ROLE " + roleName
        + " WITH GRANT OPTION");
    fieldValueMap.clear();
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT SELECT ON TABLE " + tableName
        + " TO ROLE " + roleName + " WITH GRANT OPTION");
    fieldValueMap.put(Constants.LOG_FIELD_TABLE_NAME, tableName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
    assertAuditLog(fieldValueMap);

    // for error audit log
    try {
      statement.execute("CREATE ROLE " + roleName);
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_CREATE_ROLE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "CREATE ROLE " + roleName);
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }
    try {
      statement.execute("GRANT ROLE errorROLE TO GROUP " + groupName);
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_ADD_ROLE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ROLE errorROLE TO GROUP "
          + groupName);
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }
    try {
      statement.execute("GRANT ALL ON DATABASE " + dbName + " TO ROLE errorRole");
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT ALL ON DATABASE " + dbName
          + " TO ROLE errorRole");
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }
    try {
      statement.execute("GRANT INSERT ON DATABASE " + dbName + " TO ROLE errorRole");
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT INSERT ON DATABASE " + dbName
          + " TO ROLE errorRole");
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }
    try {
      statement.execute("GRANT SELECT ON DATABASE " + dbName + " TO ROLE errorRole");
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT SELECT ON DATABASE " + dbName
          + " TO ROLE errorRole");
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }
    try {
      statement.execute("GRANT SELECT ON TABLE " + tableName + " TO ROLE errorRole");
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_GRANT_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "GRANT SELECT ON TABLE " + tableName
          + " TO ROLE errorRole");
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }

    statement.execute("REVOKE SELECT ON TABLE " + tableName + " FROM ROLE " + roleName);
    fieldValueMap.clear();
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE SELECT ON TABLE " + tableName
        + " FROM ROLE " + roleName);
    fieldValueMap.put(Constants.LOG_FIELD_TABLE_NAME, tableName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
    assertAuditLog(fieldValueMap);

    statement.execute("REVOKE ALL ON DATABASE " + dbName + " FROM ROLE " + roleName);
    fieldValueMap.clear();
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ALL ON DATABASE " + dbName
        + " FROM ROLE " + roleName);
    fieldValueMap.put(Constants.LOG_FIELD_DATABASE_NAME, dbName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
    assertAuditLog(fieldValueMap);

    statement.execute("REVOKE ROLE " + roleName + " FROM GROUP " + groupName);
    fieldValueMap.clear();
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DELETE_ROLE);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ROLE " + roleName
        + " FROM GROUP " + groupName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
    assertAuditLog(fieldValueMap);

    statement.execute("REVOKE ROLE " + roleName + " FROM USER " + userName);
    fieldValueMap.clear();
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DELETE_ROLE_USER);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ROLE " + roleName + " FROM USER "
        + userName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    assertAuditLog(fieldValueMap);

    statement.execute("DROP ROLE " + roleName);
    fieldValueMap.clear();
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DROP_ROLE);
    fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "DROP ROLE " + roleName);
    fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.TRUE);
    fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
    assertAuditLog(fieldValueMap);

    // for error audit log
    try {
      statement.execute("REVOKE SELECT ON TABLE " + tableName + " FROM ROLE errorRole");
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE SELECT ON TABLE " + tableName
          + " FROM ROLE errorRole");
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }

    try {
      statement.execute("REVOKE ALL ON DATABASE " + dbName + " FROM ROLE errorRole");
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_REVOKE_PRIVILEGE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ALL ON DATABASE " + dbName
          + " FROM ROLE errorRole");
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }

    try {
      statement.execute("REVOKE ROLE errorRole FROM GROUP " + groupName);
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DELETE_ROLE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "REVOKE ROLE errorRole FROM GROUP "
          + groupName);
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }

    try {
      statement.execute("DROP ROLE errorRole");
      fail("Exception should have been thrown");
    } catch (Exception e) {
      fieldValueMap.clear();
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION, Constants.OPERATION_DROP_ROLE);
      fieldValueMap.put(Constants.LOG_FIELD_OPERATION_TEXT, "DROP ROLE errorRole");
      fieldValueMap.put(Constants.LOG_FIELD_ALLOWED, Constants.FALSE);
      fieldValueMap.put(Constants.LOG_FIELD_IP_ADDRESS, null);
      assertAuditLog(fieldValueMap);
    }

    statement.close();
    connection.close();
  }

  private void assertAuditLog(Map<String, String> fieldValueMap) throws Exception {
    assertThat(AuditLoggerTestAppender.getLastLogLevel(), is(Level.INFO));
    JSONObject jsonObject = new JSONObject(AuditLoggerTestAppender.getLastLogEvent());
    if (fieldValueMap != null) {
      for (Map.Entry<String, String> entry : fieldValueMap.entrySet()) {
        String entryKey = entry.getKey();
        if (Constants.LOG_FIELD_IP_ADDRESS.equals(entryKey)) {
          assertTrue(CommandUtil.assertIPInAuditLog(jsonObject.get(entryKey).toString()));
        } else {
          assertTrue(entry.getValue().equalsIgnoreCase(jsonObject.get(entryKey).toString()));
        }
      }
    }
  }
}