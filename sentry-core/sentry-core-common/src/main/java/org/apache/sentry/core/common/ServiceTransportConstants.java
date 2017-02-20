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
package org.apache.sentry.core.common;

import com.google.common.collect.ImmutableMap;
import org.apache.sentry.SentryServiceClientTransportDefaultImpl;
//import java.lang.reflect.Method;

/**
 * This class acts as base class for transport constants needed for sentry clients/servers.
 */
public abstract class ServiceTransportConstants {

    public enum sentryService{
        DB_POLICY_SERVICE,
        GENERIC_POLICY_SERVICE,
        HDFS_SERVICE
    }

    public static final int RPC_PORT_DEFAULT = 8038;

    // connection pool configuration
    public static final String SENTRY_POOL_ENABLED = "sentry.service.client.connection.pool.enabled";
    public static final boolean SENTRY_POOL_ENABLED_DEFAULT = false;

    // commons-pool configuration for pool size
    public final String SENTRY_POOL_MAX_TOTAL = "sentry.service.client.connection.pool.max-total";
    public final int SENTRY_POOL_MAX_TOTAL_DEFAULT = 8;
    public final String SENTRY_POOL_MAX_IDLE = "sentry.service.client.connection.pool.max-idle";
    public final int SENTRY_POOL_MAX_IDLE_DEFAULT = 8;
    public final String SENTRY_POOL_MIN_IDLE = "sentry.service.client.connection.pool.min-idle";
    public final int SENTRY_POOL_MIN_IDLE_DEFAULT = 0;

    // retry num for getting the connection from connection pool
    public final String SENTRY_POOL_RETRY_TOTAL = "sentry.service.client.connection.pool.retry-total";
    public final int SENTRY_POOL_RETRY_TOTAL_DEFAULT = 3;

    /**
     * full retry num for getting the connection in non-pool model
     * In a full retry, it will cycle through all available sentry servers
     * {@link SentryServiceClientTransportDefaultImpl#connectWithRetry(boolean)}
     */
    public final String SENTRY_FULL_RETRY_TOTAL = "sentry.service.client.connection.full.retry-total";
    public final int SENTRY_FULL_RETRY_TOTAL_DEFAULT = 2;

    /**
     * max retry num for client rpc
     * {link RetryClientInvocationHandler#invokeImpl(Object, Method, Object[])}
     */
    public static final String SENTRY_RPC_RETRY_TOTAL = "sentry.service.client.rpc.retry-total";
    public static final int SENTRY_RPC_RETRY_TOTAL_DEFAULT = 3;


    public final String SECURITY_MODE_KERBEROS = "kerberos";
    public final String SECURITY_MODE_NONE = "none";

    public String getSecurityModeKerberos() {
        return SECURITY_MODE_KERBEROS;
    }

    public String getSecurityModeNone() {
        return SECURITY_MODE_NONE;
    }

    public String getSentryFullRetryTotal() { return SENTRY_FULL_RETRY_TOTAL; }
    public int getSentryFullRetryTotalDefault()  { return SENTRY_FULL_RETRY_TOTAL_DEFAULT;}

    public String getSentryRpcRetryTotal() { return SENTRY_RPC_RETRY_TOTAL; }
    public int getSentryRpcRetryTotalDefault()  { return SENTRY_RPC_RETRY_TOTAL_DEFAULT;}

    public abstract String getSecurityMode();
    public abstract String getSecurityUseUgiTransport();
    public abstract String getPrincipal();
    public abstract String getRpcAddress();
    public abstract String getRpcAddressDefault();
    public abstract String getServerRpcPort();
    public abstract String getServerRpcAddress();
    public abstract String getServerRpcConnTimeout();
    public abstract int getServerRpcPortDefault();
    public abstract int getServerRpcConnTimeoutDefault();

    abstract public ImmutableMap<String, String> getSaslProperties();

}
