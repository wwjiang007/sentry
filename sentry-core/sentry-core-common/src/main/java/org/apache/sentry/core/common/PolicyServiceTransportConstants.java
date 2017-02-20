/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.core.common;

import com.google.common.collect.ImmutableMap;

import javax.security.sasl.Sasl;

import java.util.HashMap;
import java.util.Map;

/**
 * This class holds all the transport constants needed for Policy service clients
 */
public final class PolicyServiceTransportConstants extends ServiceTransportConstants {

  private static final ImmutableMap<String, String> SASL_PROPERTIES;

  static {
    Map<String, String> saslProps = new HashMap<String, String>();
    saslProps.put(Sasl.SERVER_AUTH, "true");
    saslProps.put(Sasl.QOP, "auth-conf");
    SASL_PROPERTIES = ImmutableMap.copyOf(saslProps);
  }

  public PolicyServiceTransportConstants() {
    super();
  }

  /**
   * This configuration parameter is only meant to be used for testing purposes.
   */
  private final String SECURITY_MODE = "sentry.service.security.mode";

  private final String SECURITY_USE_UGI_TRANSPORT = "sentry.service.security.use.ugi";
  private final String PRINCIPAL = "sentry.service.server.principal";
  private final String RPC_ADDRESS = "sentry.service.server.rpc-address";
  private final String RPC_ADDRESS_DEFAULT = "0.0.0.0"; //NOPMD

  private final String SERVER_RPC_PORT = "sentry.service.client.server.rpc-port";
  private final int SERVER_RPC_PORT_DEFAULT = ServiceTransportConstants.RPC_PORT_DEFAULT;
  private final String SERVER_RPC_ADDRESS = "sentry.service.client.server.rpc-address";
  private final String SERVER_RPC_CONN_TIMEOUT = "sentry.service.client.server.rpc-connection-timeout";
  private final int SERVER_RPC_CONN_TIMEOUT_DEFAULT = 200000;

  public String getSecurityMode() {
    return SECURITY_MODE;
  }

  public String getSecurityUseUgiTransport() {
    return SECURITY_USE_UGI_TRANSPORT;
  }

  public String getPrincipal() {
    return PRINCIPAL;
  }

  public String getRpcAddress() {
    return RPC_ADDRESS;
  }

  public String getRpcAddressDefault() {
    return RPC_ADDRESS_DEFAULT;
  }

  public String getServerRpcPort() {
    return SERVER_RPC_PORT;
  }

  public String getServerRpcAddress() {
    return SERVER_RPC_ADDRESS;
  }

  public String getServerRpcConnTimeout() {
    return SERVER_RPC_CONN_TIMEOUT;
  }

  public int getServerRpcPortDefault() {
    return SERVER_RPC_PORT_DEFAULT;
  }

  public int getServerRpcConnTimeoutDefault() {
    return SERVER_RPC_CONN_TIMEOUT_DEFAULT;
  }

  public ImmutableMap<String, String> getSaslProperties() {
    return SASL_PROPERTIES;
  }

}