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
package org.apache.sentry.policy.common;

import java.util.Collection;
import java.util.Set;

import org.apache.shiro.util.PermissionUtils;
import org.apache.shiro.util.StringUtils;

public class PrivilegeUtils {
  public static Set<String> toPrivilegeStrings(String s) {
    return PermissionUtils.toPermissionStrings(s);
  }

  /**
   * Transform the specified {@linkplain Set} of privileges to a {@linkplain String} value.
   */
  public static String fromPrivilegeStrings (Collection<String> s) {
    return StringUtils.toDelimitedString(s, String.valueOf(StringUtils.DEFAULT_DELIMITER_CHAR));
  }

  private PrivilegeUtils() {
    // Make constructor private to avoid instantiation
  }
}
