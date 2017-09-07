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
package org.apache.sentry.core.common.exception;

/**
 * An exception which indicates that the current server is standby.
 */
public class SentryStandbyException extends SentryUserException {
  private static final long serialVersionUID = 2162010615815L;

  public SentryStandbyException(String msg, Exception e) {
    super(msg, e);
  }

  public SentryStandbyException(String msg) {
    super(msg);
  }

  public SentryStandbyException(String msg, String reason) {
    super(msg, reason);
  }

  public SentryStandbyException(String msg, Throwable t) {
    super(msg, t);
  }
}
