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
package org.apache.sentry.hdfs;

import com.codahale.metrics.Timer;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import java.util.Map;
import java.util.Set;

public class PathImageRetriever implements ExternalImageRetriever<PathsUpdate> {

  private final SentryStore sentryStore;

  public PathImageRetriever(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  @Override
  public PathsUpdate retrieveFullImage(long currSeqNum) {
    final Timer.Context timerContext = SentryHdfsMetricsUtil.getRetrievePathFullImageTimer.time();
    Map<String, Set<String>> pathImage = sentryStore.retrieveFullPathsImage();
    PathsUpdate pathsUpdate = new PathsUpdate(currSeqNum, true);

    for (Map.Entry<String, Set<String>> pathEnt : pathImage.entrySet()) {
      TPathChanges pathChange = pathsUpdate.newPathChange(pathEnt.getKey());

      for (String path : pathEnt.getValue()) {
        pathChange.addToAddPaths(PathsUpdate.splitPath(path));
      }
    }

    timerContext.stop();
    SentryHdfsMetricsUtil.getPathChangesHistogram.update(pathsUpdate.getPathChanges().size());

    return pathsUpdate;
    }
  }