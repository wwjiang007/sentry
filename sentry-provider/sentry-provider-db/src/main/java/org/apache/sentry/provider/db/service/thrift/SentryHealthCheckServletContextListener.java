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
package org.apache.sentry.provider.db.service.thrift;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;

/**
 * Use this class's registry to register health checks: Can be some tests which make sure Sentry service is healthy
 */
public class SentryHealthCheckServletContextListener extends HealthCheckServlet.ContextListener {

  //This is just a place holder for health check registry, with out this AdminServlet throws out an error
  public static final HealthCheckRegistry HEALTH_CHECK_REGISTRY = new HealthCheckRegistry();

  @Override
  protected HealthCheckRegistry getHealthCheckRegistry() {
    return HEALTH_CHECK_REGISTRY;
  }
}