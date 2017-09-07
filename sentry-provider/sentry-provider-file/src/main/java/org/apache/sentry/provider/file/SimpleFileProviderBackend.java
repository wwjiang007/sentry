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
package org.apache.sentry.provider.file;

import static org.apache.sentry.core.common.utils.SentryConstants.ROLE_SPLITTER;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.exception.SentryConfigurationException;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.apache.sentry.policy.common.PrivilegeUtils;
import org.apache.sentry.core.common.validator.PrivilegeValidator;
import org.apache.sentry.core.common.validator.PrivilegeValidatorContext;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.provider.common.CacheProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.common.TableCache;
import org.apache.shiro.config.Ini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

public class SimpleFileProviderBackend extends CacheProvider implements ProviderBackend {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleFileProviderBackend.class);

  private final FileSystem fileSystem;
  private final Path resourcePath;
  private final Configuration conf;
  private final List<String> configErrors;
  private final List<String> configWarnings;
  private TableCache cache;
  /**
   * Each group, role, and privilege in groupRolePrivilegeTable is
   * interned using a weak interner so that we only store each string
   * once.
   */
  private final Interner<String> stringInterner;

  private ImmutableList<PrivilegeValidator> validators;
  private boolean allowPerDatabaseSection;
  private volatile boolean initialized;

  public SimpleFileProviderBackend(Configuration conf, String resourcePath) throws IOException {
    this(conf, new Path(resourcePath));
  }

  public SimpleFileProviderBackend(Configuration conf, Path resourcePath) throws IOException {
    this.resourcePath = resourcePath;
    this.fileSystem = resourcePath.getFileSystem(conf);
    this.conf = conf;
    this.configErrors = Lists.newArrayList();
    this.configWarnings = Lists.newArrayList();
    this.validators = ImmutableList.of();
    this.allowPerDatabaseSection = true;
    this.initialized = false;
    this.stringInterner = Interners.newWeakInterner();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(ProviderBackendContext context) {
    if (initialized) {
      throw new IllegalStateException("Backend has already been initialized, cannot be initialized twice");
    }
    this.validators = context.getValidators();
    this.allowPerDatabaseSection = context.isAllowPerDatabase();
    final Table<String, String, Set<String>> table = parse();
    this.cache = new TableCache() {
      @Override
      public Table<String, String, Set<String>> getCache() {
        return table;
      }
    };
    super.initialize(cache);
    this.initialized = true;
  }

  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups, Set<String> users,
      ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    // SimpleFileProviderBackend doesn't support getPrivileges for user now.
    return getPrivileges(groups, roleSet, authorizableHierarchy);
  }

  @Override
  public void close() {
    // SENTRY-847 will use HiveAuthBinding again, so groupRolePrivilegeTable shouldn't clear itself
  }

  @Override
  public void validatePolicy(boolean strictValidation) throws SentryConfigurationException {
    if (!initialized) {
      throw new IllegalStateException("Backend has not been properly initialized");
    }
    List<String> localConfigErrors = Lists.newArrayList(configErrors);
    List<String> localConfigWarnings = Lists.newArrayList(configWarnings);
    if (strictValidation && !localConfigWarnings.isEmpty() || !localConfigErrors.isEmpty()) {
      localConfigErrors.add("Failed to process global policy file " + resourcePath);
      SentryConfigurationException e = new SentryConfigurationException("");
      e.setConfigErrors(localConfigErrors);
      e.setConfigWarnings(localConfigWarnings);
      throw e;
    }
  }

  private Table<String, String, Set<String>> parse() {
    configErrors.clear();
    configWarnings.clear();
    Table<String, String, Set<String>> groupRolePrivilegeTable = HashBasedTable.create();
    Table<String, String, Set<String>> groupRolePrivilegeTableTemp = HashBasedTable.create();
    Ini ini;
    LOGGER.info("Parsing " + resourcePath);
    LOGGER.info("Filesystem: " + fileSystem.getUri());
    try {
      try {
        ini = PolicyFiles.loadFromPath(fileSystem, resourcePath);
      } catch (IOException e) {
        configErrors.add("Failed to read policy file " + resourcePath +
          " Error: " + e.getMessage());
        throw new SentryConfigurationException("Error loading policy file " + resourcePath, e);
      } catch (IllegalArgumentException e) {
        configErrors.add("Failed to read policy file " + resourcePath +
          " Error: " + e.getMessage());
        throw new SentryConfigurationException("Error loading policy file " + resourcePath, e);
      }

      if(LOGGER.isDebugEnabled()) {
        for(String sectionName : ini.getSectionNames()) {
          LOGGER.debug("Section: " + sectionName);
          Ini.Section section = ini.get(sectionName);
          for(String key : section.keySet()) {
            String value = section.get(key);
            LOGGER.debug(key + " = " + value);
          }
        }
      }
      parseIni(null, ini, validators, resourcePath, groupRolePrivilegeTableTemp);
      mergeResult(groupRolePrivilegeTable, groupRolePrivilegeTableTemp);
      groupRolePrivilegeTableTemp.clear();
      Ini.Section filesSection = ini.getSection(PolicyFileConstants.DATABASES);
      if(filesSection == null) {
        LOGGER.info("Section " + PolicyFileConstants.DATABASES + " needs no further processing");
      } else if (!allowPerDatabaseSection) {
        String msg = "Per-db policy file is not expected in this configuration.";
        throw new SentryConfigurationException(msg);
      } else {
        for(Map.Entry<String, String> entry : filesSection.entrySet()) {
          String database = Strings.nullToEmpty(entry.getKey()).trim().toLowerCase();
          Path perDbPolicy = new Path(Strings.nullToEmpty(entry.getValue()).trim());
          if(isRelative(perDbPolicy)) {
            perDbPolicy = new Path(resourcePath.getParent(), perDbPolicy);
          }
          try {
            LOGGER.debug("Parsing " + perDbPolicy);
            Ini perDbIni = PolicyFiles.loadFromPath(perDbPolicy.getFileSystem(conf), perDbPolicy);
            if(perDbIni.containsKey(PolicyFileConstants.USERS)) {
              configErrors.add("Per-db policy file cannot contain " + PolicyFileConstants.USERS + " section in " +  perDbPolicy);
              throw new SentryConfigurationException("Per-db policy files cannot contain " + PolicyFileConstants.USERS + " section");
            }
            if(perDbIni.containsKey(PolicyFileConstants.DATABASES)) {
              configErrors.add("Per-db policy files cannot contain " + PolicyFileConstants.DATABASES
                  + " section in " + perDbPolicy);
              throw new SentryConfigurationException("Per-db policy files cannot contain " + PolicyFileConstants.DATABASES + " section");
            }
            parseIni(database, perDbIni, validators, perDbPolicy, groupRolePrivilegeTableTemp);
            mergeResult(groupRolePrivilegeTable, groupRolePrivilegeTableTemp);
            groupRolePrivilegeTableTemp.clear();
          } catch (Exception e) {
            configErrors.add("Failed to read per-DB policy file " + perDbPolicy +
               " Error: " + e.getMessage());
            LOGGER.error("Error processing key " + entry.getKey() + ", skipping " + entry.getValue(), e);
          }
        }
      }
    } catch (Exception e) {
      configErrors.add("Error processing file " + resourcePath + ".  Message: " + e.getMessage());
      LOGGER.error("Error processing file, ignoring " + resourcePath, e);
    }

    return groupRolePrivilegeTable;
  }

  /**
   * Relative for our purposes is no scheme, no authority
   * and a non-absolute path portion.
   */
  private boolean isRelative(Path path) {
    URI uri = path.toUri();
    return uri.getAuthority() == null && uri.getScheme() == null && !path.isUriPathAbsolute();
  }

  private void mergeResult(Table<String, String, Set<String>> groupRolePrivilegeTable,
                           Table<String, String, Set<String>> groupRolePrivilegeTableTemp) {
    for (Cell<String, String, Set<String>> cell : groupRolePrivilegeTableTemp.cellSet()) {
      String groupName = cell.getRowKey();
      String roleName = cell.getColumnKey();
      Set<String> privileges = groupRolePrivilegeTable.get(groupName, roleName);
      if (privileges == null) {
        privileges = new HashSet<String>();
        groupRolePrivilegeTable.put(groupName, roleName, privileges);
      }
      privileges.addAll(cell.getValue());
    }
  }

  private void parseIni(String database, Ini ini,
      List<? extends PrivilegeValidator> validators, Path policyPath,
      Table<String, String, Set<String>> groupRolePrivilegeTable) {
    Ini.Section privilegesSection = ini.getSection(PolicyFileConstants.ROLES);
    boolean invalidConfiguration = false;
    if (privilegesSection == null) {
      String errMsg = String.format("Section %s empty for %s", PolicyFileConstants.ROLES, policyPath);
      LOGGER.warn(errMsg);
      configErrors.add(errMsg);
      invalidConfiguration = true;
    }
    Ini.Section groupsSection = ini.getSection(PolicyFileConstants.GROUPS);
    if (groupsSection == null) {
      String warnMsg = String.format("Section %s empty for %s", PolicyFileConstants.GROUPS, policyPath);
      LOGGER.warn(warnMsg);
      configErrors.add(warnMsg);
      invalidConfiguration = true;
    }
    if (!invalidConfiguration) {
      parsePrivileges(database, privilegesSection, groupsSection, validators, policyPath,
          groupRolePrivilegeTable);
    }
  }

  private void parsePrivileges(@Nullable String database, Ini.Section rolesSection,
      Ini.Section groupsSection, List<? extends PrivilegeValidator> validators, Path policyPath,
      Table<String, String, Set<String>> groupRolePrivilegeTable) {
    Multimap<String, String> roleNameToPrivilegeMap = HashMultimap
        .create();
    for (Map.Entry<String, String> entry : rolesSection.entrySet()) {
      String roleName = stringInterner.intern(Strings.nullToEmpty(entry.getKey()).trim());
      String roleValue = Strings.nullToEmpty(entry.getValue()).trim();
      boolean invalidConfiguration = false;
      if (roleName.isEmpty()) {
        String errMsg = String.format("Empty role name encountered in %s", policyPath);
        LOGGER.warn(errMsg);
        configErrors.add(errMsg);
        invalidConfiguration = true;
      }
      if (roleValue.isEmpty()) {
        String errMsg = String.format("Empty role value encountered in %s", policyPath);
        LOGGER.warn(errMsg);
        configErrors.add(errMsg);
        invalidConfiguration = true;
      }
      if (roleNameToPrivilegeMap.containsKey(roleName)) {
        String warnMsg = String.format("Role %s defined twice in %s", roleName, policyPath);
        LOGGER.warn(warnMsg);
        configWarnings.add(warnMsg);
      }
      Set<String> privileges = PrivilegeUtils.toPrivilegeStrings(roleValue);
      if (!invalidConfiguration && privileges != null) {
        Set<String> internedPrivileges = Sets.newHashSet();
        for(String privilege : privileges) {
          for(PrivilegeValidator validator : validators) {
            validator.validate(new PrivilegeValidatorContext(database, privilege.trim()));
          }
          internedPrivileges.add(stringInterner.intern(privilege));
        }
        roleNameToPrivilegeMap.putAll(roleName, internedPrivileges);
      }
    }
    Splitter roleSplitter = ROLE_SPLITTER.omitEmptyStrings().trimResults();
    for (Map.Entry<String, String> entry : groupsSection.entrySet()) {
      String groupName = stringInterner.intern(Strings.nullToEmpty(entry.getKey()).trim());
      String groupPrivileges = Strings.nullToEmpty(entry.getValue()).trim();
      for (String roleName : roleSplitter.split(groupPrivileges)) {
        roleName = stringInterner.intern(roleName);
        if (roleNameToPrivilegeMap.containsKey(roleName)) {
          Set<String> privileges = groupRolePrivilegeTable.get(groupName, roleName);
          if (privileges == null) {
            privileges = new HashSet<String>();
            groupRolePrivilegeTable.put(groupName, roleName, privileges);
          }
          privileges.addAll(roleNameToPrivilegeMap.get(roleName));
        } else {
          String warnMsg = String.format("Role %s for group %s does not exist in privileges section in %s",
                  roleName, groupName, policyPath);
          LOGGER.warn(warnMsg);
          configWarnings.add(warnMsg);
        }
      }
    }
  }

  /**
   * Returns backing table of group-role-privileges cache.
   * Caller must not modify the returned table.
   * @return backing table of cache.
   */
  public Table<String, String, Set<String>> getGroupRolePrivilegeTable() {
    return this.cache.getCache();
  }
}
