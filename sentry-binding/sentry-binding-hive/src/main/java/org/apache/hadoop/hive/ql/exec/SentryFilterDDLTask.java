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
package org.apache.hadoop.hive.ql.exec;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.ShowColumnsDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.IOUtils;
import org.apache.sentry.binding.hive.authz.HiveAuthzBindingHookBase;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.core.common.Subject;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.util.StringUtils.stringifyException;

public class SentryFilterDDLTask extends DDLTask {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(SentryFilterDDLTask.class);

  private HiveAuthzBinding hiveAuthzBinding;
  private Subject subject;
  private HiveOperation stmtOperation;

  public SentryFilterDDLTask(HiveAuthzBinding hiveAuthzBinding, Subject subject,
      HiveOperation stmtOperation) {
    Preconditions.checkNotNull(hiveAuthzBinding);
    Preconditions.checkNotNull(subject);
    Preconditions.checkNotNull(stmtOperation);

    this.hiveAuthzBinding = hiveAuthzBinding;
    this.subject = subject;
    this.stmtOperation = stmtOperation;
  }

  public HiveAuthzBinding getHiveAuthzBinding() {
    return hiveAuthzBinding;
  }

  public Subject getSubject() {
    return subject;
  }

  public HiveOperation getStmtOperation() {
    return stmtOperation;
  }

  @Override
  public int execute(DriverContext driverContext) {
    // Currently the SentryFilterDDLTask only supports filter the "show columns in table " command.
    ShowColumnsDesc showCols = work.getShowColumnsDesc();
    try {
      if (showCols != null) {
        return showFilterColumns(showCols);
      }
    } catch (Throwable e) {
      failed(e);
      return 1;
    }

    return super.execute(driverContext);
  }

  private void failed(Throwable e) {
    // Get the cause of the exception if available
    Throwable error = e;
    while (error.getCause() != null && error.getClass() == RuntimeException.class) {
      error = error.getCause();
    }
    setException(error);
    LOG.error(stringifyException(error));
  }

  /**
   * Filter the command "show columns in table"
   *
   */
  private int showFilterColumns(ShowColumnsDesc showCols) throws HiveException {
    Table table = Hive.get(conf).getTable(showCols.getTableName());

    // write the results in the file
    DataOutputStream outStream = null;
    try {
      Path resFile = new Path(showCols.getResFile());
      FileSystem fs = resFile.getFileSystem(conf);
      outStream = fs.create(resFile);

      List<FieldSchema> cols = table.getCols();
      cols.addAll(table.getPartCols());
      // In case the query is served by HiveServer2, don't pad it with spaces,
      // as HiveServer2 output is consumed by JDBC/ODBC clients.
      boolean isOutputPadded = !SessionState.get().isHiveServerQuery();
      outStream.writeBytes(MetaDataFormatUtils.getAllColumnsInformation(
          fiterColumns(cols, table), false, isOutputPadded, null));
      outStream.close();
      outStream = null;
    } catch (IOException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    } finally {
      IOUtils.closeStream(outStream);
    }
    return 0;
  }

  private List<FieldSchema> fiterColumns(List<FieldSchema> cols, Table table) throws HiveException {
    // filter some columns that the subject has privilege on
    return HiveAuthzBindingHookBase.filterShowColumns(getHiveAuthzBinding(),
        cols, getStmtOperation(), getSubject().getName(), table.getTableName(), table.getDbName());
  }

  public void copyDDLTask(DDLTask ddlTask) {
    work = ddlTask.getWork();
    rootTask = ddlTask.isRootTask();
    childTasks = ddlTask.getChildTasks();
    parentTasks = ddlTask.getParentTasks();
    backupTask = ddlTask.getBackupTask();
    backupChildrenTasks = ddlTask.getBackupChildrenTasks();
    id = ddlTask.getId();
    taskCounters = ddlTask.getCounters();
    feedSubscribers = ddlTask.getFeedSubscribers();
    taskTag = ddlTask.getTaskTag();
    setLocalMode(ddlTask.isLocalMode());
    setRetryCmdWhenFail(ddlTask.ifRetryCmdWhenFail());
    queryPlan = ddlTask.getQueryPlan();
    jobID = ddlTask.getJobID();
    setException(ddlTask.getException());
    console = ddlTask.console;
    setFetchSource(ddlTask.isFetchSource());
    taskHandle = ddlTask.getTaskHandle();
    conf = ddlTask.conf;
    queryState = ddlTask.queryState;
    driverContext = ddlTask.getDriverContext();
    clonedConf = ddlTask.clonedConf;
    queryDisplay = ddlTask.queryDisplay;
  }
}
