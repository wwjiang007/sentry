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

package org.apache.sentry.binding.hive.authz;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.sentry.Command;
import org.apache.sentry.binding.hive.SentryPolicyFileFormatFactory;
import org.apache.sentry.binding.hive.SentryPolicyFileFormatter;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.exception.SentryConfigurationException;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;

import java.security.CodeSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

/**
 * set the required system property to be read by HiveConf and AuthzConf
 *
 * @throws Exception
 */
// Hack, hiveConf doesn't provide a reliable way check if it found a valid
// hive-site
// load auth provider
// get the configured sentry provider
// validate policy files
// import policy files
public class SentryConfigTool {
  private String sentrySiteFile = null;
  private String policyFile = null;
  private String query = null;
  private String jdbcURL = null;
  private String user = null;
  private String passWord = null;
  private String importPolicyFilePath = null;
  private String exportPolicyFilePath = null;
  private String objectPath = null;
  private boolean listPrivs = false;
  private boolean validate = false;
  private boolean importOverwriteRole = false;
  private HiveConf hiveConf = null;
  private HiveAuthzConf authzConf = null;
  private AuthorizationProvider sentryProvider = null;

  public SentryConfigTool() {

  }

  public AuthorizationProvider getSentryProvider() {
    return sentryProvider;
  }

  public void setSentryProvider(AuthorizationProvider sentryProvider) {
    this.sentryProvider = sentryProvider;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public void setHiveConf(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public HiveAuthzConf getAuthzConf() {
    return authzConf;
  }

  public void setAuthzConf(HiveAuthzConf authzConf) {
    this.authzConf = authzConf;
  }

  public boolean isValidate() {
    return validate;
  }

  public void setValidate(boolean validate) {
    this.validate = validate;
  }

  public String getImportPolicyFilePath() {
    return importPolicyFilePath;
  }

  public void setImportPolicyFilePath(String importPolicyFilePath) {
    this.importPolicyFilePath = importPolicyFilePath;
  }

  public String getObjectPath() {
    return objectPath;
  }

  public void setObjectPath(String objectPath) {
    this.objectPath = objectPath;
  }

  public String getExportPolicyFilePath() {
    return exportPolicyFilePath;
  }

  public void setExportPolicyFilePath(String exportPolicyFilePath) {
    this.exportPolicyFilePath = exportPolicyFilePath;
  }

  public String getSentrySiteFile() {
    return sentrySiteFile;
  }

  public void setSentrySiteFile(String sentrySiteFile) {
    this.sentrySiteFile = sentrySiteFile;
  }

  public String getPolicyFile() {
    return policyFile;
  }

  public void setPolicyFile(String policyFile) {
    this.policyFile = policyFile;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getJdbcURL() {
    return jdbcURL;
  }

  public void setJdbcURL(String jdbcURL) {
    this.jdbcURL = jdbcURL;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassWord() {
    return passWord;
  }

  public void setPassWord(String passWord) {
    this.passWord = passWord;
  }

  public boolean isListPrivs() {
    return listPrivs;
  }

  public void setListPrivs(boolean listPrivs) {
    this.listPrivs = listPrivs;
  }

  public boolean isImportOverwriteRole() {
    return importOverwriteRole;
  }

  public void setImportOverwriteRole(boolean importOverwriteRole) {
    this.importOverwriteRole = importOverwriteRole;
  }

  /**
   * set the required system property to be read by HiveConf and AuthzConf
   * @throws Exception
   */
  public void setupConfig() throws Exception {
    System.out.println("Configuration: ");
    CodeSource src = SentryConfigTool.class.getProtectionDomain()
        .getCodeSource();
    if (src != null) {
      System.out.println("Sentry package jar: " + src.getLocation());
    }

    if (getPolicyFile() != null) {
      System.setProperty(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(),
          getPolicyFile());
    }
    System.setProperty(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "true");
    setHiveConf(new HiveConf(SessionState.class));
    getHiveConf().setVar(ConfVars.SEMANTIC_ANALYZER_HOOK,
        HiveAuthzBindingHookBase.class.getName());
    try {
      System.out.println("Hive config: " + HiveConf.getHiveSiteLocation());
    } catch (NullPointerException e) {
      // Hack, hiveConf doesn't provide a reliable way check if it found a valid
      // hive-site
      throw new SentryConfigurationException("Didn't find a hive-site.xml");

    }

    if (getSentrySiteFile() != null) {
      getHiveConf()
          .set(HiveAuthzConf.HIVE_SENTRY_CONF_URL, getSentrySiteFile());
    }

    setAuthzConf(HiveAuthzConf.getAuthzConf(getHiveConf()));
    System.out.println("Sentry config: "
        + getAuthzConf().getHiveAuthzSiteFile());
    System.out.println("Sentry Policy: "
        + getAuthzConf().get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar()));
    System.out.println("Sentry server: "
        + getAuthzConf().get(AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));

    setSentryProvider(getAuthorizationProvider());
  }

  // load auth provider
  private AuthorizationProvider getAuthorizationProvider()
      throws IllegalStateException, SentryConfigurationException {
    String serverName = new Server(getAuthzConf().get(
        AuthzConfVars.AUTHZ_SERVER_NAME.getVar())).getName();
    // get the configured sentry provider
    try {
      return HiveAuthzBinding.getAuthProvider(getHiveConf(),
          authzConf, serverName);
    } catch (SentryConfigurationException eC) {
      printConfigErrors(eC);
      throw eC;
    } catch (Exception e) {
      throw new IllegalStateException("Couldn't load sentry provider ", e);
    }
  }

  // validate policy files
  public void validatePolicy() throws Exception {
    try {
      getSentryProvider().validateResource(true);
    } catch (SentryConfigurationException e) {
      printConfigErrors(e);
      throw e;
    }
    System.out.println("No errors found in the policy file");
  }

  // import the sentry mapping data to database
  public void importPolicy() throws Exception {
    String requestorUserName = System.getProperty("user.name", "");
    // get the FileFormatter according to the configuration
    SentryPolicyFileFormatter sentryPolicyFileFormatter = SentryPolicyFileFormatFactory
        .createFileFormatter(authzConf);
    // parse the input file, get the mapping data in map structure
    Map<String, Map<String, Set<String>>> policyFileMappingData = sentryPolicyFileFormatter.parse(
        importPolicyFilePath, authzConf);
    // todo: here should be an validator to check the data's value, format, hierarchy
    try(SentryPolicyServiceClient client =
                SentryServiceClientFactory.create(getAuthzConf())) {
      // import the mapping data to database
      client.importPolicy(policyFileMappingData, requestorUserName, importOverwriteRole);
    }
  }

  // export the sentry mapping data to file
  public void exportPolicy() throws Exception {
    String requestorUserName = System.getProperty("user.name", "");
    try (SentryPolicyServiceClient client =
                SentryServiceClientFactory.create(getAuthzConf())) {
      // export the sentry mapping data from database to map structure
      Map<String, Map<String, Set<String>>> policyFileMappingData = client
              .exportPolicy(requestorUserName, objectPath);
      // get the FileFormatter according to the configuration
      SentryPolicyFileFormatter sentryPolicyFileFormatter = SentryPolicyFileFormatFactory
              .createFileFormatter(authzConf);
      // write the sentry mapping data to exportPolicyFilePath with the data in map structure
      sentryPolicyFileFormatter.write(exportPolicyFilePath, policyFileMappingData);
    }
  }

  // list permissions for given user
  public void listPrivs() throws Exception {
    getSentryProvider().validateResource(true);
    System.out.println("Available privileges for user " + getUser() + ":");
    Set<String> permList = getSentryProvider().listPrivilegesForSubject(
        new Subject(getUser()));
    for (String perms : permList) {
      System.out.println("\t" + perms);
    }
    if (permList.isEmpty()) {
      System.out.println("\t*** No permissions available ***");
    }
  }

  // Verify the given query
  public void verifyLocalQuery(String queryStr) throws Exception {
    // setup Hive driver
    SessionState session = new SessionState(getHiveConf());
    SessionState.start(session);
    Driver driver = new Driver(session.getConf(), getUser());

    // compile the query
    CommandProcessorResponse compilerStatus = driver
        .compileAndRespond(queryStr);
    if (compilerStatus.getResponseCode() != 0) {
      String errMsg = compilerStatus.getErrorMessage();
      if (errMsg.contains(HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE)) {
        printMissingPerms(getHiveConf().get(
            HiveAuthzConf.HIVE_SENTRY_AUTH_ERRORS));
      }
      throw new SemanticException("Compilation error: "
          + compilerStatus.getErrorMessage());
    }
    driver.close();
    System.out
        .println("User " + getUser() + " has privileges to run the query");
  }

  // connect to remote HS2 and run mock query
  public void verifyRemoteQuery(String queryStr) throws Exception {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    Connection conn = DriverManager.getConnection(getJdbcURL(), getUser(),
        getPassWord());
    Statement stmt = conn.createStatement();
    if (!isSentryEnabledOnHiveServer(stmt)) {
      throw new IllegalStateException("Sentry is not enabled on HiveServer2");
    }
    stmt.execute("set " + HiveAuthzConf.HIVE_SENTRY_MOCK_COMPILATION + "=true");
    try {
      stmt.execute(queryStr);
    } catch (SQLException e) {
      String errMsg = e.getMessage();
      if (errMsg.contains(HiveAuthzConf.HIVE_SENTRY_MOCK_ERROR)) {
        System.out.println("User "
            + readConfig(stmt, HiveAuthzConf.HIVE_SENTRY_SUBJECT_NAME)
            + " has privileges to run the query");
        return;
      } else if (errMsg
          .contains(HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE)) {
        printMissingPerms(readConfig(stmt,
            HiveAuthzConf.HIVE_SENTRY_AUTH_ERRORS));
        throw e;
      } else {
        throw e;
      }
    } finally {
      if (!stmt.isClosed()) {
        stmt.close();
      }
      conn.close();
    }

  }

  // verify senty session hook is set
  private boolean isSentryEnabledOnHiveServer(Statement stmt)
      throws SQLException {
    String bindingString = readConfig(stmt, ConfVars.HIVE_SERVER2_SESSION_HOOK.varname).toUpperCase();
    return bindingString.contains("org.apache.sentry.binding.hive".toUpperCase())
        && bindingString.contains("HiveAuthzBindingSessionHook".toUpperCase());
  }

  // read a config value using 'set' statement
  private String readConfig(Statement stmt, String configKey)
      throws SQLException {
    try (ResultSet res = stmt.executeQuery("set " + configKey)) {
      if (!res.next()) {
        return null;
      }
      // parse key=value result format
      String result = res.getString(1);
      res.close();
      return result.substring(result.indexOf("=") + 1);
    }
  }

  // print configuration/policy file errors and warnings
  private void printConfigErrors(SentryConfigurationException configException)
      throws SentryConfigurationException {
    System.out.println(" *** Found configuration problems *** ");
    for (String errMsg : configException.getConfigErrors()) {
      System.out.println("ERROR: " + errMsg);
    }
    for (String warnMsg : configException.getConfigWarnings()) {
      System.out.println("Warning: " + warnMsg);
    }
  }

  // extract the authorization errors from config property and print
  private void printMissingPerms(String errMsg) {
    if (errMsg == null || errMsg.isEmpty()) {
      return;
    }
    System.out.println("*** Query compilation failed ***");
    String perms[] = errMsg.replaceFirst(
        ".*" + HiveAuthzConf.HIVE_SENTRY_PRIVILEGE_ERROR_MESSAGE, "")
        .split(";");
    System.out.println("Required privileges for given query:");
    for (int count = 0; count < perms.length; count++) {
      System.out.println(" \t " + perms[count]);
    }
  }

  // print usage
  private void usage(Options sentryOptions) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("sentry --command config-tool", sentryOptions);
    System.exit(-1);
  }

  /**
   * parse arguments
   *
   * <pre>
   *   -d,--debug                  Enable debug output
   *   -e,--query <arg>            Query privilege verification, requires -u
   *   -h,--help                   Print usage
   *   -i,--policyIni <arg>        Policy file path
   *   -j,--jdbcURL <arg>          JDBC URL
   *   -l,--listPrivs,--listPerms  List privilges for given user, requires -u
   *   -p,--password <arg>         Password
   *   -s,--sentry-site <arg>      sentry-site file path
   *   -u,--user <arg>             user name
   *   -v,--validate               Validate policy file
   *   -I,--import                 Import policy file
   *   -E,--export                 Export policy file
   *   -o,--overwrite              Overwrite the exist role data when do the import
   *   -b,--objectPath             The path of the object whose privileges will be exported
   * </pre>
   *
   * @param args
   */
  private void parseArgs(String[] args) {
    boolean enableDebug = false;

    Options sentryOptions = new Options();

    Option helpOpt = new Option("h", "help", false, "Print usage");
    helpOpt.setRequired(false);

    Option validateOpt = new Option("v", "validate", false,
        "Validate policy file");
    validateOpt.setRequired(false);

    Option queryOpt = new Option("e", "query", true,
        "Query privilege verification, requires -u");
    queryOpt.setRequired(false);

    Option listPermsOpt = new Option("l", "listPerms", false,
        "list permissions for given user, requires -u");
    listPermsOpt.setRequired(false);
    Option listPrivsOpt = new Option("listPrivs", false,
        "list privileges for given user, requires -u");
    listPrivsOpt.setRequired(false);

    Option importOpt = new Option("I", "import", true,
        "Import policy file");
    importOpt.setRequired(false);

    Option exportOpt = new Option("E", "export", true, "Export policy file");
    exportOpt.setRequired(false);
    // required args
    OptionGroup sentryOptGroup = new OptionGroup();
    sentryOptGroup.addOption(helpOpt);
    sentryOptGroup.addOption(validateOpt);
    sentryOptGroup.addOption(queryOpt);
    sentryOptGroup.addOption(listPermsOpt);
    sentryOptGroup.addOption(listPrivsOpt);
    sentryOptGroup.addOption(importOpt);
    sentryOptGroup.addOption(exportOpt);
    sentryOptGroup.setRequired(true);
    sentryOptions.addOptionGroup(sentryOptGroup);

    // optional args
    Option jdbcArg = new Option("j", "jdbcURL", true, "JDBC URL");
    jdbcArg.setRequired(false);
    sentryOptions.addOption(jdbcArg);

    Option sentrySitePath = new Option("s", "sentry-site", true,
        "sentry-site file path");
    sentrySitePath.setRequired(false);
    sentryOptions.addOption(sentrySitePath);

    Option globalPolicyPath = new Option("i", "policyIni", true,
        "Policy file path");
    globalPolicyPath.setRequired(false);
    sentryOptions.addOption(globalPolicyPath);

    Option userOpt = new Option("u", "user", true, "user name");
    userOpt.setRequired(false);
    sentryOptions.addOption(userOpt);

    Option passWordOpt = new Option("p", "password", true, "Password");
    userOpt.setRequired(false);
    sentryOptions.addOption(passWordOpt);

    Option debugOpt = new Option("d", "debug", false, "enable debug output");
    debugOpt.setRequired(false);
    sentryOptions.addOption(debugOpt);

    Option overwriteOpt = new Option("o", "overwrite", false, "enable import overwrite");
    overwriteOpt.setRequired(false);
    sentryOptions.addOption(overwriteOpt);

    Option objectPathOpt = new Option("b", "objectPath",
        false, "The path of the object whose privileges will be exported");
    objectPathOpt.setRequired(false);
    sentryOptions.addOption(objectPathOpt);

    try {
      Parser parser = new GnuParser();
      CommandLine cmd = parser.parse(sentryOptions, args);

      for (Option opt : cmd.getOptions()) {
        if (opt.getOpt().equals("s")) {
          setSentrySiteFile(opt.getValue());
        } else if (opt.getOpt().equals("i")) {
          setPolicyFile(opt.getValue());
        } else if (opt.getOpt().equals("e")) {
          setQuery(opt.getValue());
        } else if (opt.getOpt().equals("j")) {
          setJdbcURL(opt.getValue());
        } else if (opt.getOpt().equals("u")) {
          setUser(opt.getValue());
        } else if (opt.getOpt().equals("p")) {
          setPassWord(opt.getValue());
        } else if (opt.getOpt().equals("l") || opt.getOpt().equals("listPrivs")) {
          setListPrivs(true);
        } else if (opt.getOpt().equals("v")) {
          setValidate(true);
        } else if (opt.getOpt().equals("I")) {
          setImportPolicyFilePath(opt.getValue());
        } else if (opt.getOpt().equals("E")) {
          setExportPolicyFilePath(opt.getValue());
        } else if (opt.getOpt().equals("h")) {
          usage(sentryOptions);
        } else if (opt.getOpt().equals("d")) {
          enableDebug = true;
        } else if (opt.getOpt().equals("o")) {
          setImportOverwriteRole(true);
        } else if (opt.getOpt().equals("b")) {
          setObjectPath(opt.getValue());
        }
      }

      if (isListPrivs() && getUser() == null) {
        throw new ParseException("Can't use -l without -u ");
      }
      if (getQuery() != null && getUser() == null) {
        throw new ParseException("Must use -u with -e ");
      }
    } catch (ParseException e1) {
      usage(sentryOptions);
    }

    if (!enableDebug) {
      // turn off log
      LogManager.getRootLogger().setLevel(Level.OFF);
    }
  }

  public static class CommandImpl implements Command {
    @Override
    public void run(String[] args) throws Exception {
      SentryConfigTool sentryTool = new SentryConfigTool();

      try {
        // parse arguments
        sentryTool.parseArgs(args);

        // load configuration
        sentryTool.setupConfig();

        // validate configuration
        if (sentryTool.isValidate()) {
          sentryTool.validatePolicy();
        }

        if (!StringUtils.isEmpty(sentryTool.getImportPolicyFilePath())) {
          sentryTool.importPolicy();
        }

        if (!StringUtils.isEmpty(sentryTool.getExportPolicyFilePath())) {
          sentryTool.exportPolicy();
        }

        // list permissions for give user
        if (sentryTool.isListPrivs()) {
          sentryTool.listPrivs();
        }

        // verify given query
        if (sentryTool.getQuery() != null) {
          if (sentryTool.getJdbcURL() != null) {
            sentryTool.verifyRemoteQuery(sentryTool.getQuery());
          } else {
            sentryTool.verifyLocalQuery(sentryTool.getQuery());
          }
        }
      } catch (Exception e) {
        System.out.println("Sentry tool reported Errors: " + e.getMessage());
        e.printStackTrace(System.out);
        System.exit(1);
      }
    }
  }
}
