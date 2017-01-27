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

import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.hdfs.PermissionsUpdate;
import org.apache.sentry.provider.db.service.model.MSentryPathChange;
import org.apache.sentry.provider.db.service.model.MSentryPermChange;
import static org.apache.sentry.hdfs.Updateable.Update;

import javax.jdo.PersistenceManager;

/**
 * A subclass of {@link TransactionBlock} to manage the code should be executed for
 * each delta update. The update could be {@link PathsUpdate} or {@link PermissionsUpdate}.
 * Based on update type, the update would be persisted into corresponding update table,
 * e.g {@link MSentryPathChange} {@link MSentryPermChange}.
 * <p>
 * Delta update should not have full image, hence update contains full image would not
 * be executed.
 */
public class DeltaTransactionBlock implements TransactionBlock {
  private final Update update;

  public DeltaTransactionBlock(Update update) {
    this.update = update;
  }

  @Override
  public Object execute(PersistenceManager pm) throws Exception {
    persistUpdate(pm, update);
    return null;
  }

  /**
   * Persist the delta change into corresponding type based on its type.
   * Atomic increasing primary key changeID by 1. Return without any
   * operation if update is null or update is a fullImage.
   *
   * @param pm PersistenceManager
   * @param update update
   * @throws Exception
   */
  private void persistUpdate(PersistenceManager pm, Update update)
      throws Exception {

    // persistUpdate cannot handle full image update, instead
    // it only handles delta updates.
    if (update == null || update.hasFullImage()) {
      return;
    }

    // Persist the update into corresponding tables based on its type.
    // changeID is the primary key in MSentryPXXXChange table. If same
    // changeID is trying to be persisted twice, the transaction would
    // fail.
    if (update instanceof PermissionsUpdate) {
      pm.makePersistent(new MSentryPermChange(update.JSONSerialize()));
    } else if (update instanceof PathsUpdate) {
      pm.makePersistent(new MSentryPathChange(update.JSONSerialize()));
    }
  }

  @Override
  public int hashCode() {
    return (update == null) ? 0 : update.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (this == obj) {
      return true;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    DeltaTransactionBlock other = (DeltaTransactionBlock) obj;
    if (update == null) {
      return other.update == null;
    }
    return update.equals(other.update);
  }
}
