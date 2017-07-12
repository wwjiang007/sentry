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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.hdfs.service.thrift.TPathsDump;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateableAuthzPaths implements AuthzPaths, Updateable<PathsUpdate> {
  private static final int MAX_UPDATES_PER_LOCK_USE = 99;
  private static final String UPDATABLE_TYPE_NAME = "path_update";
  private static final Logger LOG = LoggerFactory.getLogger(UpdateableAuthzPaths.class);
  private volatile HMSPaths paths;
  private final AtomicLong seqNum = new AtomicLong(0);

  public UpdateableAuthzPaths(String[] pathPrefixes) {
    this.paths = new HMSPaths(pathPrefixes);
  }

  UpdateableAuthzPaths(HMSPaths paths) {
    this.paths = paths;
  }

  @Override
  public boolean isUnderPrefix(String[] pathElements) {
    return paths.isUnderPrefix(pathElements);
  }

  @Override
  public Set<String> findAuthzObject(String[] pathElements) {
    return paths.findAuthzObject(pathElements);
  }

  @Override
  public Set<String> findAuthzObjectExactMatches(String[] pathElements) {
    return paths.findAuthzObjectExactMatches(pathElements);
  }

  @Override
  public UpdateableAuthzPaths updateFull(PathsUpdate update) {
    UpdateableAuthzPaths other = getPathsDump().initializeFromDump(
            update.toThrift().getPathsDump());
    other.seqNum.set(update.getSeqNum());
    return other;
  }

  @Override
  public void updatePartial(Iterable<PathsUpdate> updates, ReadWriteLock lock) {
    lock.writeLock().lock();
    try {
      int counter = 0;
      for (PathsUpdate update : updates) {
        applyPartialUpdate(update);
        if (++counter > MAX_UPDATES_PER_LOCK_USE) {
          counter = 0;
          lock.writeLock().unlock();
          lock.writeLock().lock();
        }
        seqNum.set(update.getSeqNum());
        LOG.debug("##### Updated paths seq Num [" + seqNum.get() + "]");
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void applyPartialUpdate(PathsUpdate update) {
    // Handle alter table rename : will have exactly 2 path changes
    // 1 is add path and the other is del path and oldName != newName
    if (update.getPathChanges().size() == 2) {
      List<TPathChanges> pathChanges = update.getPathChanges();
      TPathChanges newPathInfo = null;
      TPathChanges oldPathInfo = null;
      if (pathChanges.get(0).getAddPathsSize() == 1
          && pathChanges.get(1).getDelPathsSize() == 1) {
        newPathInfo = pathChanges.get(0);
        oldPathInfo = pathChanges.get(1);
      } else if (pathChanges.get(1).getAddPathsSize() == 1
          && pathChanges.get(0).getDelPathsSize() == 1) {
        newPathInfo = pathChanges.get(1);
        oldPathInfo = pathChanges.get(0);
      }
      if (newPathInfo != null && oldPathInfo != null &&
              !newPathInfo.getAuthzObj().equalsIgnoreCase(oldPathInfo.getAuthzObj())) {
        paths.renameAuthzObject(
            oldPathInfo.getAuthzObj(), oldPathInfo.getDelPaths(),
            newPathInfo.getAuthzObj(), newPathInfo.getAddPaths());
        return;
      }
    }

    List<TPathChanges> deletePathChanges = new ArrayList<TPathChanges>();
    List<TPathChanges> addPathChanges = new ArrayList<TPathChanges>();

    for (TPathChanges pathChanges : update.getPathChanges()) {
      if (pathChanges.getDelPaths() != null && pathChanges.getDelPaths().size() != 0) {
        deletePathChanges.add(pathChanges);
      }
      if (pathChanges.getAddPaths() != null && pathChanges.getAddPaths().size() != 0) {
        addPathChanges.add(pathChanges);
      }
    }
    for (TPathChanges pathChanges : deletePathChanges) {
      List<List<String>> delPaths = pathChanges.getDelPaths();
      if (delPaths.size() == 1 && delPaths.get(0).size() == 1
              && delPaths.get(0).get(0).equals(PathsUpdate.ALL_PATHS)) {
        // Remove all paths.. eg. drop table
        paths.deleteAuthzObject(pathChanges.getAuthzObj());
      } else {
        paths.deletePathsFromAuthzObject(pathChanges.getAuthzObj(), pathChanges
                .getDelPaths());
      }
    }
    for (TPathChanges pathChanges : addPathChanges) {
      paths.addPathsToAuthzObject(pathChanges.getAuthzObj(), pathChanges
          .getAddPaths(), true);
    }
  }

  @Override
  public long getLastUpdatedSeqNum() {
    return seqNum.get();
  }

  @Override
  public PathsUpdate createFullImageUpdate(long currSeqNum) {
    PathsUpdate pathsUpdate = new PathsUpdate(currSeqNum, true);
    pathsUpdate.toThrift().setPathsDump(getPathsDump().createPathsDump(true));
    return pathsUpdate;
  }

  @Override
  public AuthzPathsDumper<UpdateableAuthzPaths> getPathsDump() {
    return new AuthzPathsDumper<UpdateableAuthzPaths>() {

      @Override
      public TPathsDump createPathsDump(boolean minimizeSize) {
        return UpdateableAuthzPaths.this.paths.getPathsDump().
            createPathsDump(minimizeSize);
      }

      @Override
      public UpdateableAuthzPaths initializeFromDump(TPathsDump pathsDump) {
        return new UpdateableAuthzPaths(UpdateableAuthzPaths.this.paths
            .getPathsDump().initializeFromDump(pathsDump));
      }
    };
  }

  @Override
  public String getUpdateableTypeName() {
    return UPDATABLE_TYPE_NAME;
  }
}
