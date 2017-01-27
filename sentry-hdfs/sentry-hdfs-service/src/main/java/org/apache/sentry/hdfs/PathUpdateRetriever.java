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


import com.google.common.collect.Lists;
import org.apache.sentry.provider.db.service.model.MSentryPathChange;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import java.util.List;

public class PathUpdateRetriever implements UpdateRetriever<PathsUpdate> {

  private final SentryStore sentryStore;

  public PathUpdateRetriever(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  @Override
  public List<PathsUpdate> retrievePartialUpdate(long seqNum) throws Exception {
    List<MSentryPathChange> mSentryPathChanges =
            sentryStore.getMSentryPathChanges(seqNum);
    List<PathsUpdate> updates = Lists.newArrayList();
    for (MSentryPathChange mSentryPathChange : mSentryPathChanges) {
      // get changeID from stored MSentryPathChange
      long changeID = mSentryPathChange.getChangeID();
      // Create a corresponding PathsUpdate and deserialize the stored
      // JSON string to TPathsUpdate. Then set the corresponding
      // changeID.
      PathsUpdate pathsUpdate = new PathsUpdate();
      pathsUpdate.JSONDeserialize(mSentryPathChange.getPathChange());
      pathsUpdate.setSeqNum(changeID);
      updates.add(pathsUpdate);
    }
    return updates;
  }

  @Override
  public boolean isPartialUpdateAvailable(long seqNum) throws Exception {
    return sentryStore.findMSentryPathChangeByID(seqNum);
  }
}