/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.binding.metastore.messaging.json;

import java.util.Iterator;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.PartitionFiles;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAddPartitionMessage;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class SentryJSONAddPartitionMessage extends JSONAddPartitionMessage {
  @JsonProperty
  private List<String> locations;

  public SentryJSONAddPartitionMessage() {
  }

  public SentryJSONAddPartitionMessage(String server, String servicePrincipal, Table tableObj,
      Iterator<Partition> partitionsIterator, Iterator<PartitionFiles> partitionFileIter, Long timestamp) {
    super(server, servicePrincipal, tableObj, partitionsIterator, partitionFileIter, timestamp);
  }

  public SentryJSONAddPartitionMessage(String server, String servicePrincipal, Table tableObj,
                                       Iterator<Partition> partitionsIterator, Iterator<PartitionFiles> partitionFileIter, Long timestamp, List<String> locations) {
    super(server, servicePrincipal, tableObj, partitionsIterator, partitionFileIter, timestamp);
    this.locations = locations;
  }

  public List<String> getLocations() {
    return locations;
  }

  @Override
  public String toString() {
    return SentryJSONMessageDeserializer.serialize(this);
  }
}
