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
import org.apache.sentry.provider.db.service.model.MSentryPermChange;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import java.util.List;

public class PermUpdateRetriever implements UpdateRetriever<PermissionsUpdate> {

  private final SentryStore sentryStore;

  public PermUpdateRetriever(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  @Override
  public List<PermissionsUpdate> retrievePartialUpdate(long seqNum) throws Exception {
    List<MSentryPermChange> mSentryPermChanges =
            sentryStore.getMSentryPermChanges(seqNum);
    List<PermissionsUpdate> updates = Lists.newArrayList();
    for (MSentryPermChange mSentryPermChange : mSentryPermChanges) {
      // get changeID from stored MSentryPermChange
      long changeID = mSentryPermChange.getChangeID();
      // Create a corresponding PermissionsUpdate and deserialize the stored
      // JSON string to TPermissionsUpdate. Then set the corresponding
      // changeID.
      PermissionsUpdate permsUpdate = new PermissionsUpdate();
      permsUpdate.JSONDeserialize(mSentryPermChange.getPermChange());
      permsUpdate.setSeqNum(changeID);
      updates.add(permsUpdate);
    }
    return updates;
  }

  @Override
  public boolean isPartialUpdateAvailable(long seqNum) throws Exception {
    return sentryStore.findMSentryPermChangeByID(seqNum);
  }
}