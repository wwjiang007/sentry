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

package org.apache.sentry.provider.db.generic.service.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.RetryClientInvocationHandler;
import org.apache.sentry.service.thrift.ServiceConstants;

import java.lang.reflect.Proxy;

public final class SentryGenericServiceClientFactory {

    private SentryGenericServiceClientFactory() {
    }

    public static SentryGenericServiceClient create(Configuration conf) throws Exception {
        boolean pooled = conf.getBoolean(
          ServiceConstants.ClientConfig.SENTRY_POOL_ENABLED, ServiceConstants.ClientConfig.SENTRY_POOL_ENABLED_DEFAULT);
        if (pooled) {
            //SentryGenericServiceClient doesn't have pool implementation
            // TODO Implement pool for SentryGenericServiceClient
            return null;
        } else {
            RetryClientInvocationHandler clientHandler = new RetryClientInvocationHandler(conf,
              new SentryGenericServiceClientDefaultImpl(conf));
            return (SentryGenericServiceClient) Proxy
              .newProxyInstance(SentryGenericServiceClientDefaultImpl.class.getClassLoader(),
                SentryGenericServiceClientDefaultImpl.class.getInterfaces(),
                clientHandler);

        }
    }
}
