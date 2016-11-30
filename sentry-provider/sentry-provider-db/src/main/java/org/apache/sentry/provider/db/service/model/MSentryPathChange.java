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

package org.apache.sentry.provider.db.service.model;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.PrimaryKey;

/**
 * Database backed Sentry Changes. Any changes to this object
 * require re-running the maven build so DN can re-enhance.
 */

@PersistenceCapable
public class MSentryPathChange {

  @PrimaryKey
  private long changeID;

  /**
   * Change in Json format
   */
  private String pathChange;
  private long pathID;
  private long createTimeMs;

  public MSentryPathChange(long changeID, long pathID, String pathChange, long createTime) {
    this.changeID = changeID;
    this.pathID = pathID;
    this.pathChange = pathChange;
    this.createTimeMs = createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTimeMs = createTime;
  }

  @Override
  public String toString() {
    return "MSentryChange [changeID=" + changeID + ", pathID= " + pathID + ", pathChange= " + pathChange + ", createTime=" + createTimeMs +  "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((pathChange == null) ? 0 : pathChange.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    MSentryPathChange other = (MSentryPathChange) obj;
    if (changeID != other.changeID) {
      return false;
    }

    if (pathID != other.pathID) {
      return false;
    }

    if (!pathChange.equals(other.pathChange)) {
      return false;
    }

    if (createTimeMs != other.createTimeMs) {
      return false;
    }

    return true;
  }
}