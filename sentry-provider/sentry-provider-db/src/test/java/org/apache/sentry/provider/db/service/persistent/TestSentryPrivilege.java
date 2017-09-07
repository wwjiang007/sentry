/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.persistent;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.junit.Test;

public class TestSentryPrivilege {
  @Test
  public void testImpliesPrivilegePositive() throws Exception {
    // 1.test server+database+table+action
    MSentryPrivilege my = new MSentryPrivilege();
    MSentryPrivilege your = new MSentryPrivilege();
    my.setServerName("server1");
    my.setDbName("db1");
    my.setTableName("tb1");
    my.setAction(AccessConstants.SELECT);
    your.setServerName("server1");
    your.setDbName("db1");
    your.setTableName("tb1");
    your.setAction(AccessConstants.SELECT);
    assertTrue(my.implies(your));

    my.setAction(AccessConstants.ALL);
    assertTrue(my.implies(your));

    my.setTableName("");
    assertTrue(my.implies(your));

    my.setDbName("");
    assertTrue(my.implies(your));

    my.setAction(AccessConstants.ACTION_ALL);
    assertTrue(my.implies(your));

    my.setTableName("");
    assertTrue(my.implies(your));

    my.setDbName("");
    assertTrue(my.implies(your));

    // 2.test server+URI+action using all combinations of * and ALL for action
    String[][] actionMap = new String[][] {
        { AccessConstants.ALL, AccessConstants.ALL },
        { AccessConstants.ALL, AccessConstants.ACTION_ALL },
        { AccessConstants.ACTION_ALL, AccessConstants.ALL },
        { AccessConstants.ACTION_ALL, AccessConstants.ACTION_ALL } };

    for (int actions = 0; actions < actionMap.length; actions++) {
      my = new MSentryPrivilege();
      your = new MSentryPrivilege();
      my.setServerName("server1");
      my.setAction(actionMap[actions][0]);
      your.setServerName("server1");
      your.setAction(actionMap[actions][1]);
      my.setURI("hdfs://namenode:9000/path");
      your.setURI("hdfs://namenode:9000/path");
      assertTrue(my.implies(your));

      my.setURI("hdfs://namenode:9000/path");
      your.setURI("hdfs://namenode:9000/path/to/some/dir");
      assertTrue(my.implies(your));

      my.setURI("file:///path");
      your.setURI("file:///path");
      assertTrue(my.implies(your));

      my.setURI("file:///path");
      your.setURI("file:///path/to/some/dir");
      assertTrue(my.implies(your));

      // my is SERVER level privilege, your is URI level privilege
      my.setURI("");
      your.setURI("file:///path");
      assertTrue(my.implies(your));
    }
  }

  @Test
  public void testImpliesPrivilegeNegative() throws Exception {
    // 1.test server+database+table+action
    MSentryPrivilege my = new MSentryPrivilege();
    MSentryPrivilege your = new MSentryPrivilege();
    // bad action
    my.setServerName("server1");
    my.setDbName("db1");
    my.setTableName("tb1");
    my.setAction(AccessConstants.SELECT);
    your.setServerName("server1");
    your.setDbName("db1");
    your.setTableName("tb1");
    your.setAction(AccessConstants.INSERT);
    assertFalse(my.implies(your));

    // bad action
    your.setAction(AccessConstants.ALL);
    assertFalse(my.implies(your));


    // bad table
    your.setTableName("tb2");
    assertFalse(my.implies(your));

    // bad database
    your.setTableName("tb1");
    your.setDbName("db2");
    assertFalse(my.implies(your));

    // bad server
    your.setTableName("tb1");
    your.setDbName("db1");
    your.setServerName("server2");
    assertFalse(my.implies(your));

    // 2.test server+URI+action
    my = new MSentryPrivilege();
    your = new MSentryPrivilege();
    my.setServerName("server1");
    my.setAction(AccessConstants.ALL);
    your.setServerName("server2");
    your.setAction(AccessConstants.ALL);

    // relative path
    my.setURI("hdfs://namenode:9000/path");
    your.setURI("hdfs://namenode:9000/path/to/../../other");
    assertFalse(my.implies(your));
    my.setURI("file:///path");
    your.setURI("file:///path/to/../../other");
    assertFalse(my.implies(your));

    // bad uri
    my.setURI("blah");
    your.setURI("hdfs://namenode:9000/path/to/some/dir");
    assertFalse(my.implies(your));
    my.setURI("hdfs://namenode:9000/path/to/some/dir");
    your.setURI("blah");
    assertFalse(my.implies(your));

    // bad scheme
    my.setURI("hdfs://namenode:9000/path");
    your.setURI("file:///path/to/some/dir");
    assertFalse(my.implies(your));
    my.setURI("hdfs://namenode:9000/path");
    your.setURI("file://namenode:9000/path/to/some/dir");
    assertFalse(my.implies(your));

    // bad hostname
    my.setURI("hdfs://namenode1:9000/path");
    your.setURI("hdfs://namenode2:9000/path");
    assertFalse(my.implies(your));

    // bad port
    my.setURI("hdfs://namenode:9000/path");
    your.setURI("hdfs://namenode:9001/path");
    assertFalse(my.implies(your));

    // bad path
    my.setURI("hdfs://namenode:9000/path1");
    your.setURI("hdfs://namenode:9000/path2");
    assertFalse(my.implies(your));
    my.setURI("file:///path1");
    your.setURI("file:///path2");
    assertFalse(my.implies(your));

    // bad server
    your.setServerName("server2");
    my.setURI("hdfs://namenode:9000/path1");
    your.setURI("hdfs://namenode:9000/path1");
    assertFalse(my.implies(your));

    // bad implies
    my.setServerName("server1");
    my.setURI("hdfs://namenode:9000/path1");
    your.setServerName("server1");
    your.setURI("");
    assertFalse(my.implies(your));
  }

  @Test
  public void testImpliesPrivilegePositiveWithColumn() throws Exception {
    // 1.test server+database+table+column+action
    MSentryPrivilege my = new MSentryPrivilege();
    MSentryPrivilege your = new MSentryPrivilege();
    my.setServerName("server1");
    my.setAction(AccessConstants.SELECT);
    your.setServerName("server1");
    your.setDbName("db1");
    your.setTableName("tb1");
    your.setColumnName("c1");
    your.setAction(AccessConstants.SELECT);
    assertTrue(my.implies(your));

    my.setDbName("db1");
    assertTrue(my.implies(your));

    my.setTableName("tb1");
    assertTrue(my.implies(your));

    my.setColumnName("c1");
    assertTrue(my.implies(your));
  }

  @Test
  public void testImpliesPrivilegeNegativeWithColumn() throws Exception {
    // 1.test server+database+table+column+action
    MSentryPrivilege my = new MSentryPrivilege();
    MSentryPrivilege your = new MSentryPrivilege();
    // bad column
    my.setServerName("server1");
    my.setDbName("db1");
    my.setTableName("tb1");
    my.setColumnName("c1");
    my.setAction(AccessConstants.SELECT);
    your.setServerName("server1");
    your.setDbName("db1");
    your.setTableName("tb1");
    your.setColumnName("c2");
    your.setAction(AccessConstants.SELECT);
    assertFalse(my.implies(your));

    // bad scope
    your.setColumnName("");
    assertFalse(my.implies(your));
  }
}
