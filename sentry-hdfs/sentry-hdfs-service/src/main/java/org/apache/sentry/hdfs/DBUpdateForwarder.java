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

import java.util.LinkedList;
import java.util.List;

import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  DBUpdateForwarder is thread safe class
 */
public class DBUpdateForwarder<K extends Updateable.Update> {

  private final ImageRetriever<K> imageRetreiver;
  private final UpdateRetriever<K> updateRetriever;

  private static final Logger LOGGER = LoggerFactory.getLogger(DBUpdateForwarder.class);
  private static final String UPDATABLE_TYPE_NAME = "update_forwarder";

  protected DBUpdateForwarder(final ImageRetriever<K> imageRetreiver,
      final UpdateRetriever<K> updateRetriever) {
    this.imageRetreiver = imageRetreiver;
    this.updateRetriever = updateRetriever;
  }

  /**
   * Return all updates from requested seqNum (inclusive)
   *
   * @param seqNum
   * @return the list of updates
   */
  public List<K> getAllUpdatesFrom(long seqNum) throws Exception {
    List<K> retVal = new LinkedList<>();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("#### GetAllUpdatesFrom ["
      + "reqSeqNum=" + seqNum + " ]");
    }

    if (seqNum == SentryStore.INIT_CHANGE_ID ||
          !updateRetriever.isPartialUpdateAvailable(seqNum)) {
      retVal.add(imageRetreiver.retrieveFullImage());
    } else {
      retVal.addAll(updateRetriever.retrievePartialUpdate(seqNum));
    }

    return retVal;
  }
}
