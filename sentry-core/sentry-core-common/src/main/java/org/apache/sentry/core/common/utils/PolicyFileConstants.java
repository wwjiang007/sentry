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
package org.apache.sentry.core.common.utils;

import java.util.Arrays;
import java.util.List;

public class PolicyFileConstants {
  public static final String DATABASES = "databases";
  public static final String GROUPS = "groups";
  public static final String ROLES = "roles";
  public static final String USERS = "users";
  public static final String USER_ROLES = "userroles";
  public static final String PRIVILEGE_SERVER_NAME = "server";
  public static final String PRIVILEGE_DATABASE_NAME = "db";
  public static final String PRIVILEGE_TABLE_NAME = "table";
  public static final String PRIVILEGE_COLUMN_NAME = "column";
  public static final String PRIVILEGE_URI_NAME = "uri";
  public static final String PRIVILEGE_ACTION_NAME = "action";
  public static final String PRIVILEGE_GRANT_OPTION_NAME = "grantoption";

  /**
   * This constant defines all possible section names in sentry ini file in the expected order
   */
  public static final List<String> SECTION_NAMES = Arrays.asList(DATABASES, USERS, GROUPS, ROLES);

  private PolicyFileConstants() {
    // Make constructor private to avoid instantiation
  }
}
