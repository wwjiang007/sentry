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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.sentry.core.common.exception.SentryUserException;

public class SentryStoreSchemaInfo {
  private static final String SQL_FILE_EXTENSION = ".sql";
  private static final String UPGRADE_FILE_PREFIX = "upgrade-";
  private static final String INIT_FILE_PREFIX = "sentry-";
  private static final String VERSION_UPGRADE_LIST = "upgrade.order";
  private final String dbType;
  private final String sentrySchemaVersions[];
  private final String sentryScriptDir;

  private static final String SENTRY_VERSION = "2.0.0";

  public SentryStoreSchemaInfo(String sentryScriptDir, String dbType)
      throws SentryUserException {
    this.sentryScriptDir = sentryScriptDir;
    this.dbType = dbType;
    // load upgrade order for the given dbType
    List<String> upgradeOrderList = new ArrayList<String>();
    String upgradeListFile = getSentryStoreScriptDir() + File.separator
        + VERSION_UPGRADE_LIST + "." + dbType;
    try (BufferedReader bfReader = new BufferedReader(new FileReader(upgradeListFile))) {
      String currSchemaVersion;
      while ((currSchemaVersion = bfReader.readLine()) != null) {
        upgradeOrderList.add(currSchemaVersion.trim());
      }
    } catch (FileNotFoundException e) {
      throw new SentryUserException("File " + upgradeListFile + " not found ", e);
    } catch (IOException e) {
      throw new SentryUserException("Error reading " + upgradeListFile, e);
    }
    sentrySchemaVersions = upgradeOrderList.toArray(new String[0]);
  }

  public String getSentrySchemaVersion() {
    return SENTRY_VERSION;
  }

  public List<String> getUpgradeScripts(String fromSchemaVer)
      throws SentryUserException {
    List<String> upgradeScriptList = new ArrayList<String>();

    // check if we are already at current schema level
    if (getSentryVersion().equals(fromSchemaVer)) {
      return upgradeScriptList;
    }

    // Find the list of scripts to execute for this upgrade
    int firstScript = sentrySchemaVersions.length;
    for (int i = 0; i < sentrySchemaVersions.length; i++) {
      String fromVersion = sentrySchemaVersions[i].split("-to-")[0];
      if (fromVersion.equals(fromSchemaVer)) {
        firstScript = i;
        break;
      }
    }
    if (firstScript == sentrySchemaVersions.length) {
      throw new SentryUserException("Unknown version specified for upgrade "
          + fromSchemaVer + " Metastore schema may be too old or newer");
    }

    for (int i = firstScript; i < sentrySchemaVersions.length; i++) {
      String scriptFile = generateUpgradeFileName(sentrySchemaVersions[i]);
      upgradeScriptList.add(scriptFile);
    }
    return upgradeScriptList;
  }

  /***
   * Get the name of the script to initialize the schema for given version
   *
   * @param toVersion
   *          Target version. If it's null, then the current server version is
   *          used
   * @return
   * @throws SentryUserException
   */
  public String generateInitFileName(String toVersion)
      throws SentryUserException {
    String version = toVersion;
    if (version == null) {
      version = getSentryVersion();
    }
    String initScriptName = INIT_FILE_PREFIX + dbType + "-" + version
        + SQL_FILE_EXTENSION;
    // check if the file exists
    if (!(new File(getSentryStoreScriptDir() + File.separatorChar
        + initScriptName).exists())) {
      throw new SentryUserException(
          "Unknown version specified for initialization: " + version);
    }
    return initScriptName;
  }

  /**
   * Find the directory of sentry store scripts
   *
   * @return
   */
  public String getSentryStoreScriptDir() {
    return sentryScriptDir;
  }

  // format the upgrade script name eg upgrade-x-y-dbType.sql
  private String generateUpgradeFileName(String fileVersion) {
    return INIT_FILE_PREFIX + UPGRADE_FILE_PREFIX + dbType + "-"
        + fileVersion + SQL_FILE_EXTENSION;
  }

  // Current hive version, in majorVersion.minorVersion.changeVersion format
  // TODO: store the version using the build script
  public static String getSentryVersion() {
    return SENTRY_VERSION;
  }
}
