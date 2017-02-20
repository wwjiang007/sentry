/*
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

import org.apache.hadoop.conf.Configuration;

import javax.security.sasl.Sasl;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.sentry.core.common.SentryClientConstants.KERBEROS_MODE;
import static org.apache.sentry.core.common.SentryClientConstants.HDFSClientConstants.*;
import static org.apache.sentry.core.common.SentryClientConstants.sentryClientSecurityMode.SECURITY_MODE_KERBEROS;
import static org.apache.sentry.core.common.SentryClientConstants.sentryClientSecurityMode.SECURITY_MODE_NONE;

public class SentryHDFSClientTransportConfig implements SentryClientTransportConfigInterface {
  private static final Map<String, String> SASL_PROPERTIES;

  static {
    Map<String, String> saslProps = new HashMap<>();
    saslProps.put(Sasl.SERVER_AUTH, "true");
    saslProps.put(Sasl.QOP, "auth-conf");
    SASL_PROPERTIES = Collections.unmodifiableMap(saslProps);
  }

  @Override
  public SentryClientConstants.sentryClientSecurityMode getSecurityMode(Configuration conf) {
    return (conf.get(SECURITY_MODE, KERBEROS_MODE)
            .equalsIgnoreCase((KERBEROS_MODE)) ?
            SECURITY_MODE_KERBEROS : SECURITY_MODE_NONE);
  }

  @Override
  public int getClientRetryTotal(Configuration conf) {
    return conf.getInt(SENTRY_RPC_RETRY_TOTAL, SENTRY_RPC_RETRY_TOTAL_DEFAULT);
  }

  @Override
  public int getClientFullRetryTotal(Configuration conf) {
    return conf.getInt(SENTRY_FULL_RETRY_TOTAL, SENTRY_FULL_RETRY_TOTAL_DEFAULT);
  }

  @Override
  public boolean useUgiTransport(Configuration conf) {
    return "true".equalsIgnoreCase(conf.get(SECURITY_USE_UGI_TRANSPORT, "false"));
  }

  @Override
  public String getPrincipal(Configuration conf) {
    return conf.get(PRINCIPAL);
  }

  @Override
  public String getServerRpcAddress(Configuration conf) {
    return conf.get(SERVER_RPC_ADDRESS);
  }

  @Override
  public int getServerRpcPort(Configuration conf) {
    return conf.getInt(SERVER_RPC_PORT, RPC_PORT_DEFAULT);
  }

  @Override
  public int getServerRpcConnTimeout(Configuration conf) {
    return conf.getInt(SERVER_RPC_CONN_TIMEOUT, SERVER_RPC_CONN_TIMEOUT_DEFAULT);
  }

  @Override
  public Map<String, String> getSaslProperties() {
    return SASL_PROPERTIES;
  }

}
