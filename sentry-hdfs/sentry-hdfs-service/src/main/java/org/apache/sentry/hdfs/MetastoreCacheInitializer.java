/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class MetastoreCacheInitializer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger
          (MetastoreCacheInitializer.class);

  final static class CallResult {
    final private Exception failure;
    final private boolean successStatus;

    CallResult(Exception ex, boolean successStatus) {
      failure = ex;
      this.successStatus = successStatus;
    }

    public boolean getSuccessStatus() {
      return successStatus;
    }

    public Exception getFailure() {
      return failure;
    }
  }

  abstract class BaseTask implements Callable<CallResult> {

    /**
     *  Class represents retry strategy for BaseTask.
     */
    private final class RetryStrategy {
      private int retryStrategyMaxRetries = 0;
      private int retryStrategyWaitDurationMillis;
      private int retries;
      private Exception exception;

      private RetryStrategy(int retryStrategyMaxRetries, int retryStrategyWaitDurationMillis) {
        this.retryStrategyMaxRetries = retryStrategyMaxRetries;
        retries = 0;

        // Assign default wait duration if negative value is provided.
        if (retryStrategyWaitDurationMillis > 0) {
          this.retryStrategyWaitDurationMillis = retryStrategyWaitDurationMillis;
        } else {
          this.retryStrategyWaitDurationMillis = 1000;
        }
      }

      public CallResult exec()  {

        // Retry logic is happening inside callable/task to avoid
        // synchronous waiting on getting the result.
        // Retry the failure task until reach the max retry number.
        // Wait configurable duration for next retry.
        for (int i = 0; i < retryStrategyMaxRetries; i++) {
          try {
            doTask();

            // Task succeeds, reset the exception and return
            // the successful flag.
            exception = null;
            return new CallResult(exception, true);
          } catch (Exception ex) {
            LOGGER.debug("Failed to execute task on " + (i + 1) + " attempts." +
                    " Sleeping for " + retryStrategyWaitDurationMillis + " ms. Exception: " + ex.toString(), ex);
            exception = ex;

            try {
              Thread.sleep(retryStrategyWaitDurationMillis);
            } catch (InterruptedException exception) {
              // Skip the rest retries if get InterruptedException.
              // And set the corresponding retries number.
              retries = i;
              i = retryStrategyMaxRetries;
            }
          }

          retries = i;
        }

        // Task fails, return the failure flag.
        LOGGER.error("Task did not complete successfully after " + retries
                + " tries. Exception got: " + exception.toString());
        return new CallResult(exception, false);
      }
    }

    private RetryStrategy retryStrategy;

    BaseTask() {
      taskCounter.incrementAndGet();
      retryStrategy = new RetryStrategy(maxRetries, waitDurationMillis);
    }

    @Override
    public CallResult call() throws Exception {
      CallResult callResult = retryStrategy.exec();
      taskCounter.decrementAndGet();
      return callResult;
    }

    abstract void doTask() throws Exception;
  }

  class PartitionTask extends BaseTask {
    private final String dbName;
    private final String tblName;
    private final List<String> partNames;
    private final TPathChanges tblPathChange;

    PartitionTask(String dbName, String tblName, List<String> partNames,
                  TPathChanges tblPathChange) {
      super();
      Preconditions.checkNotNull(dbName, "Null database name");
      Preconditions.checkNotNull(tblName, "database \"%s\": Null table name", dbName);
      Preconditions.checkNotNull(partNames, "database \"%s\", table \"%s\": Null partNames", dbName, tblName);
      Preconditions.checkNotNull(tblPathChange, "database \"%s\", table \"%s\": Null tblPathChange", dbName, tblName);
      this.dbName = dbName;
      this.tblName = tblName;
      this.partNames = partNames;
      this.tblPathChange = tblPathChange;
    }

    @Override
    public void doTask() throws TException, SentryMalformedPathException {
      List<Partition> tblParts =
              hmsHandler.get_partitions_by_names(dbName, tblName, partNames);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("#### Fetching partitions " +
                "[" + dbName + "." + tblName + "]" + "[" + partNames + "]");
      }
      for (Partition part : tblParts) {
        List<String> partPath = null;
        Preconditions.checkNotNull(part.getSd(),
          "database \"%s\", table \"%s\", partition with Null SD",
          dbName, tblName);
        try {
          partPath = PathsUpdate.parsePath(part.getSd().getLocation());
        } catch (SentryMalformedPathException e) {
          String msg = String.format("Unexpected path in partitionTask: dbName=\"%s\", tblName=\"%s\", path=\"%s\"",
            dbName, tblName, part.getSd().getLocation());
          throw new SentryMalformedPathException(msg, e);
        }

        if (partPath != null) {
          synchronized (tblPathChange) {
            tblPathChange.addToAddPaths(partPath);
          }
        }
      }
    }
  }

  class TableTask extends BaseTask {
    private final Database db;
    private final List<String> tableNames;
    private final PathsUpdate update;

    TableTask(Database db, List<String> tableNames, PathsUpdate update) {
      super();
      Preconditions.checkNotNull(db, "Null database");
      Preconditions.checkNotNull(db.getName(), "Null database name");
      Preconditions.checkNotNull(tableNames, "database \"%s\": Null tableNames", db.getName());
      Preconditions.checkNotNull(update, "database \"%s\": Null update object", db.getName());
      this.db = db;
      this.tableNames = tableNames;
      this.update = update;
    }

    @Override
    public void doTask() throws Exception {
      List<Table> tables =
              hmsHandler.get_table_objects_by_name(db.getName(), tableNames);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("#### Fetching tables [" + db.getName() + "][" +
                tableNames + "]");
      }
      for (Table tbl : tables) {
        TPathChanges tblPathChange;
        // Table names are case insensitive
        Preconditions.checkNotNull(tbl.getTableName(),
          "database \"%s\": Null table name", db.getName());
        Preconditions.checkNotNull(tbl.getDbName(),
          "database \"%s\", table \"%s\": Null database name", db.getName(), tbl.getTableName());
        Preconditions.checkNotNull(tbl.getSd(),
          "database \"%s\", table \"%s\": Null SD", db.getName(), tbl.getTableName());
        Preconditions.checkArgument(tbl.getDbName().equalsIgnoreCase(db.getName()),
          "database \"%s\", table \"%s\": inconsistent database name \"%s\"", tbl.getDbName(), tbl.getTableName(), db.getName());
        String tableName = tbl.getTableName().toLowerCase();
        synchronized (update) {
          tblPathChange = update.newPathChange(db.getName() + "." + tableName);
        }
        if (tbl.getSd().getLocation() != null) {
          List<String> tblPath = null;
          try {
            tblPath = PathsUpdate.parsePath(tbl.getSd().getLocation());
          } catch (SentryMalformedPathException e) {
            String msg = String.format("Unexpected path in TableTask: dbName=\"%s\", tblName=\"%s\", path=\"%s\"",
              tbl.getDbName(), tbl.getTableName(), tbl.getSd().getLocation());
            throw new SentryMalformedPathException(msg, e);
          }
          if (tblPath != null) {
            tblPathChange.addToAddPaths(tblPath);
          }
          List<String> tblPartNames =
                  hmsHandler.get_partition_names(db.getName(), tableName, (short) -1);
          for (int i = 0; i < tblPartNames.size(); i += maxPartitionsPerCall) {
            List<String> partsToFetch =
                    tblPartNames.subList(i, Math.min(
                            i + maxPartitionsPerCall, tblPartNames.size()));
            Callable<CallResult> partTask =
                    new PartitionTask(db.getName(), tableName,
                            partsToFetch, tblPathChange);
            results.add(threadPool.submit(partTask));
          }
        }
      }
    }
  }

  class DbTask extends BaseTask {

    private final PathsUpdate update;
    private final String dbName;

    DbTask(PathsUpdate update, String dbName) {
      super();
      Preconditions.checkNotNull(dbName, "Null database name");
      Preconditions.checkNotNull(update, "database \"%s\": Null update object", dbName);
      this.update = update;
      //Database names are case insensitive
      this.dbName = dbName.toLowerCase();
    }

    @Override
    public void doTask() throws TException, SentryMalformedPathException {
      Database db = hmsHandler.get_database(dbName);
      Preconditions.checkNotNull(db, "Cannot find database \"%s\"", dbName);
      List<String> dbPath = null;
      try {
        dbPath = PathsUpdate.parsePath(db.getLocationUri());
      } catch (SentryMalformedPathException e) {
        String msg = String.format("Unexpected path in DbTask: DB=\"%s\", Path = \"%s\"",
          db.getName(), db.getLocationUri());
        throw new SentryMalformedPathException(msg, e);
      }
      if (dbPath != null) {
        Preconditions.checkArgument(dbName.equalsIgnoreCase(db.getName()),
                "Inconsistent database names \"%s\" vs \"%s\"", dbName, db.getName());
        synchronized (update) {
          update.newPathChange(dbName).addToAddPaths(dbPath);
        }
      }
      List<String> allTblStr = hmsHandler.get_all_tables(dbName);
      Preconditions.checkNotNull(allTblStr, "Cannot fetch tables for database \"%s\"", dbName);
      for (int i = 0; i < allTblStr.size(); i += maxTablesPerCall) {
        List<String> tablesToFetch =
                allTblStr.subList(i, Math.min(
                        i + maxTablesPerCall, allTblStr.size()));
        Callable<CallResult> tableTask =
                new TableTask(db, tablesToFetch, update);
          results.add(threadPool.submit(tableTask));
      }
    }
  }

  private final ExecutorService threadPool;
  private final IHMSHandler hmsHandler;
  private final int maxPartitionsPerCall;
  private final int maxTablesPerCall;
  // We use Vector because it is thread-safe
  private final Collection<Future<CallResult>> results = new Vector<>();
  private final AtomicInteger taskCounter = new AtomicInteger(0);
  private final int maxRetries;
  private final int waitDurationMillis;
  private final boolean failOnRetry;

  MetastoreCacheInitializer(IHMSHandler hmsHandler, Configuration conf) {
    this.hmsHandler = hmsHandler;
    this.maxPartitionsPerCall = conf.getInt(
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC,
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_PART_PER_RPC_DEFAULT);
    this.maxTablesPerCall = conf.getInt(
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC,
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_MAX_TABLES_PER_RPC_DEFAULT);
    threadPool = Executors.newFixedThreadPool(conf.getInt(
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS,
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_INIT_THREADS_DEFAULT));
    maxRetries = conf.getInt(
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_MAX_NUM,
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_MAX_NUM_DEFAULT);
    waitDurationMillis = conf.getInt(
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_WAIT_DURAION_IN_MILLIS,
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_RETRY_WAIT_DURAION_IN_MILLIS_DEFAULT);
    failOnRetry = conf.getBoolean(
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_FAIL_ON_PARTIAL_UPDATE,
            ServiceConstants.ServerConfig
                    .SENTRY_HDFS_SYNC_METASTORE_CACHE_FAIL_ON_PARTIAL_UPDATE_DEFAULT);
  }

  UpdateableAuthzPaths createInitialUpdate() throws
          Exception {
    UpdateableAuthzPaths authzPaths = new UpdateableAuthzPaths(new
            String[]{"/"});
    PathsUpdate tempUpdate = new PathsUpdate(-1, false);
    List<String> allDbStr = hmsHandler.get_all_databases();
    for (String dbName : allDbStr) {
      Callable<CallResult> dbTask = new DbTask(tempUpdate, dbName);
      results.add(threadPool.submit(dbTask));
    }

    while (taskCounter.get() != 0) {
      Thread.sleep(250);
      // Wait until no more tasks remain
    }

    for (Future<CallResult> result : results) {
      CallResult callResult = result.get();

      // Fail the HMS startup if tasks are not all successful and
      // fail on partial updates flag is set in the config.
      if (!callResult.getSuccessStatus() && failOnRetry) {
        throw callResult.getFailure();
      }
    }

    authzPaths.updatePartial(Lists.newArrayList(tempUpdate),
            new ReentrantReadWriteLock());
    return authzPaths;
  }


  @Override
  public void close() throws IOException {
    if (threadPool != null) {
      threadPool.shutdownNow();
    }
  }
}
