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
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.apache.sentry.provider.db.service.persistent.PermissionsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class PermImageRetriever implements ImageRetriever<PermissionsUpdate> {

  private final SentryStore sentryStore;

  public PermImageRetriever(SentryStore sentryStore) {
    this.sentryStore = sentryStore;
  }

  @Override
  public PermissionsUpdate retrieveFullImage() throws Exception {
    try(Timer.Context timerContext =
        SentryHdfsMetricsUtil.getRetrievePermFullImageTimer.time()) {

      SentryHdfsMetricsUtil.getRetrievePermFullImageTimer.time();

      // Read the full permission snapshot from Sentry DB which
      // associates with a up-to-date/corresponding sequence number.
      PermissionsImage permImage = sentryStore.retrieveFullPermssionsImage();
      long curSeqNum = permImage.getCurSeqNum();
      Map<String, HashMap<String, String>> privilegeImage =
          permImage.getPrivilegeImage();
      Map<String, LinkedList<String>> roleImage =
          permImage.getRoleImage();

      // Generate a corresponding PermissionsUpdate.
      TPermissionsUpdate tPermUpdate = new TPermissionsUpdate(true, curSeqNum,
          new HashMap<String, TPrivilegeChanges>(),
          new HashMap<String, TRoleChanges>());

      for (Map.Entry<String, HashMap<String, String>> privEnt : privilegeImage.entrySet()) {
        String authzObj = privEnt.getKey();
        HashMap<String,String> privs = privEnt.getValue();
        tPermUpdate.putToPrivilegeChanges(authzObj, new TPrivilegeChanges(
        authzObj, privs, new HashMap<String, String>()));
      }

      for (Map.Entry<String, LinkedList<String>> privEnt : roleImage.entrySet()) {
        String role = privEnt.getKey();
        LinkedList<String> groups = privEnt.getValue();
        tPermUpdate.putToRoleChanges(role, new TRoleChanges(role, groups, new LinkedList<String>()));
      }

      PermissionsUpdate permissionsUpdate = new PermissionsUpdate(tPermUpdate);
      // TODO: use curSeqNum from DB instead of seqNum when doing SENTRY-1567
      permissionsUpdate.setSeqNum(curSeqNum);
      SentryHdfsMetricsUtil.getPrivilegeChangesHistogram.update(
          tPermUpdate.getPrivilegeChangesSize());
      SentryHdfsMetricsUtil.getRoleChangesHistogram.update(
          tPermUpdate.getRoleChangesSize());
      return permissionsUpdate;
    }
  }

}
