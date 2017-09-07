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
package org.apache.sentry.policy.hive;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAction;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.HivePrivilegeModel;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.sentry.provider.common.ResourceAuthorizationProvider;
import org.apache.sentry.provider.file.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;


public class TestResourceAuthorizationProviderGeneralCases {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TestResourceAuthorizationProviderGeneralCases.class);

  private static final Multimap<String, String> USER_TO_GROUP_MAP = HashMultimap
      .create();

  private static final Subject SUB_ADMIN = new Subject("admin1");
  private static final Subject SUB_MANAGER = new Subject("manager1");
  private static final Subject SUB_ANALYST = new Subject("analyst1");
  private static final Subject SUB_JUNIOR_ANALYST = new Subject("jranalyst1");

  private static final Server SVR_SERVER1 = new Server("server1");
  private static final Server SVR_ALL = new Server(AccessConstants.ALL);

  private static final Database DB_CUSTOMERS = new Database("customers");
  private static final Database DB_ANALYST = new Database("analyst1");
  private static final Database DB_JR_ANALYST = new Database("jranalyst1");

  private static final Table TBL_PURCHASES = new Table("purchases");

  private static final Set<? extends Action> ALL = EnumSet.of(DBModelAction.ALL);
  private static final Set<? extends Action> SELECT = EnumSet.of(DBModelAction.SELECT);
  private static final Set<? extends Action> INSERT = EnumSet.of(DBModelAction.INSERT);

  static {
    USER_TO_GROUP_MAP.putAll(SUB_ADMIN.getName(), Arrays.asList("admin"));
    USER_TO_GROUP_MAP.putAll(SUB_MANAGER.getName(), Arrays.asList("manager"));
    USER_TO_GROUP_MAP.putAll(SUB_ANALYST.getName(), Arrays.asList("analyst"));
    USER_TO_GROUP_MAP.putAll(SUB_JUNIOR_ANALYST.getName(),
        Arrays.asList("jranalyst"));
  }

  private final ResourceAuthorizationProvider authzProvider;
  private File baseDir;

  public TestResourceAuthorizationProviderGeneralCases() throws IOException {
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, "hive-policy-test-authz-provider.ini", "hive-policy-test-authz-provider-other-group.ini");
    authzProvider = new HadoopGroupResourceAuthorizationProvider(
            DBPolicyTestUtil.createPolicyEngineForTest("server1",
        new File(baseDir, "hive-policy-test-authz-provider.ini").getPath()),
        new MockGroupMappingServiceProvider(USER_TO_GROUP_MAP), HivePrivilegeModel.getInstance());

  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  private void doTestAuthorizables(
      Subject subject, Set<? extends Action> privileges, boolean expected,
      Authorizable... authorizables) throws Exception {
    List<Authorizable> authzHierarchy = Arrays.asList(authorizables);
    Objects.ToStringHelper helper = Objects.toStringHelper("TestParameters");
      helper.add("authorizables", authzHierarchy).add("Privileges", privileges);
    LOGGER.info("Running with " + helper.toString());
    Assert.assertEquals(helper.toString(), expected,
        authzProvider.hasAccess(subject, authzHierarchy, privileges, ActiveRoleSet.ALL));
    LOGGER.info("Passed " + helper.toString());
  }

  private void doTestResourceAuthorizationProvider(Subject subject,
      Server server, Database database, Table table,
      Set<? extends Action> privileges, boolean expected) throws Exception {
    List<Authorizable> authzHierarchy = Arrays.asList(new Authorizable[] {
        server, database, table
    });
    Objects.ToStringHelper helper = Objects.toStringHelper("TestParameters");
    helper.add("Subject", subject).add("Server", server).add("DB", database)
    .add("Table", table).add("Privileges", privileges).add("authzHierarchy", authzHierarchy);
    LOGGER.info("Running with " + helper.toString());
    Assert.assertEquals(helper.toString(), expected,
        authzProvider.hasAccess(subject, authzHierarchy, privileges, ActiveRoleSet.ALL));
    LOGGER.info("Passed " + helper.toString());
  }

  @Test
  public void testAdmin() throws Exception {
    doTestResourceAuthorizationProvider(SUB_ADMIN, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, ALL, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, SELECT, true);
    doTestResourceAuthorizationProvider(SUB_ADMIN, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, INSERT, true);
    doTestAuthorizables(SUB_ADMIN, SELECT, true, SVR_ALL, DB_CUSTOMERS, TBL_PURCHASES);

  }
  @Test
  public void testManager() throws Exception {
    doTestResourceAuthorizationProvider(SUB_MANAGER, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, ALL, false);
    doTestResourceAuthorizationProvider(SUB_MANAGER, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, SELECT, true);
    doTestResourceAuthorizationProvider(SUB_MANAGER, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, INSERT, false);
    doTestResourceAuthorizationProvider(SUB_MANAGER, SVR_ALL, DB_CUSTOMERS, TBL_PURCHASES, SELECT, true);
  }
  @Test
  public void testAnalyst() throws Exception {
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, ALL, false);
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, SELECT, true);
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, INSERT, false);
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_ALL, DB_CUSTOMERS, TBL_PURCHASES, SELECT, true);

    // analyst sandbox
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_SERVER1, DB_ANALYST, TBL_PURCHASES, ALL, true);
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_SERVER1, DB_ANALYST, TBL_PURCHASES, SELECT, true);
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_SERVER1, DB_ANALYST, TBL_PURCHASES, INSERT, true);
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_ALL, DB_ANALYST, TBL_PURCHASES, SELECT, true);

    // jr analyst sandbox
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_SERVER1, DB_JR_ANALYST, TBL_PURCHASES, ALL, false);
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_SERVER1, DB_JR_ANALYST, TBL_PURCHASES, SELECT, true);
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_SERVER1, DB_JR_ANALYST, TBL_PURCHASES, INSERT, false);
    doTestResourceAuthorizationProvider(SUB_ANALYST, SVR_ALL, DB_JR_ANALYST, TBL_PURCHASES, SELECT, true);
  }
  @Test
  public void testJuniorAnalyst() throws Exception {
    doTestResourceAuthorizationProvider(SUB_JUNIOR_ANALYST, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, ALL, false);
    doTestResourceAuthorizationProvider(SUB_JUNIOR_ANALYST, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, SELECT, false);
    doTestResourceAuthorizationProvider(SUB_JUNIOR_ANALYST, SVR_SERVER1, DB_CUSTOMERS, TBL_PURCHASES, INSERT, false);
    doTestResourceAuthorizationProvider(SUB_JUNIOR_ANALYST, SVR_ALL, DB_CUSTOMERS, TBL_PURCHASES, SELECT, false);
    // jr analyst sandbox
    doTestResourceAuthorizationProvider(SUB_JUNIOR_ANALYST, SVR_SERVER1, DB_JR_ANALYST, TBL_PURCHASES, ALL, true);
    doTestResourceAuthorizationProvider(SUB_JUNIOR_ANALYST, SVR_SERVER1, DB_JR_ANALYST, TBL_PURCHASES, SELECT, true);
    doTestResourceAuthorizationProvider(SUB_JUNIOR_ANALYST, SVR_SERVER1, DB_JR_ANALYST, TBL_PURCHASES, INSERT, true);
    doTestResourceAuthorizationProvider(SUB_JUNIOR_ANALYST, SVR_ALL, DB_JR_ANALYST, TBL_PURCHASES, SELECT, true);
  }

  public class MockGroupMappingServiceProvider implements GroupMappingService {
    private final Multimap<String, String> userToGroupMap;

    public MockGroupMappingServiceProvider(Multimap<String, String> userToGroupMap) {
      this.userToGroupMap = userToGroupMap;
    }

    @Override
    public Set<String> getGroups(String user) {
      return Sets.newHashSet(userToGroupMap.get(user));
    }
  }
}
