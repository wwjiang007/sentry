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

package org.apache.sentry;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.HdfsServiceTransportConstants;
import org.apache.sentry.core.common.PolicyServiceTransportConstants;
import org.apache.sentry.core.common.ServiceTransportConstants;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

/**
 * This class has the transport implementation for sentry clients.
 * All the sentry clients should extend this class for transport implementation.
 */

public abstract class SentryServiceClientTransportDefaultImpl {
  protected final Configuration conf;
  private final boolean kerberos;
  private String[] serverPrincipalParts;

  protected TTransport transport;
  private final int connectionTimeout;
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServiceClientTransportDefaultImpl.class);
  // configs for connection retry
  private final int connectionFullRetryTotal;
  private final int rpcRetryTotal;
  private final List<InetSocketAddress> endpoints;
  protected InetSocketAddress serverAddress;
  private final ServiceTransportConstants serviceConstants;

  /**
   * This transport wraps the Sasl transports to set up the right UGI context for open().
   */
  public static class UgiSaslClientTransport extends TSaslClientTransport {
    protected UserGroupInformation ugi = null;

    public UgiSaslClientTransport(String mechanism, String authorizationId,
                                  String protocol, String serverName, Map<String, String> props,
                                  CallbackHandler cbh, TTransport transport, boolean wrapUgi, Configuration conf)
      throws IOException, SaslException {
      super(mechanism, authorizationId, protocol, serverName, props, cbh,
        transport);
      if (wrapUgi) {
        // If we don't set the configuration, the UGI will be created based on
        // what's on the classpath, which may lack the kerberos changes we require
        UserGroupInformation.setConfiguration(conf);
        ugi = UserGroupInformation.getLoginUser();
      }
    }

    // open the SASL transport with using the current UserGroupInformation
    // This is needed to get the current login context stored
    @Override
    public void open() throws TTransportException {
      if (ugi == null) {
        baseOpen();
      } else {
        try {
          if (ugi.isFromKeytab()) {
            ugi.checkTGTAndReloginFromKeytab();
          }
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws TTransportException {
              baseOpen();
              return null;
            }
          });
        } catch (IOException e) {
          throw new TTransportException("Failed to open SASL transport: " + e.getMessage(), e);
        } catch (InterruptedException e) {
          throw new TTransportException(
            "Interrupted while opening underlying transport: " + e.getMessage(), e);
        }
      }
    }

    private void baseOpen() throws TTransportException {
      super.open();
    }
  }

  /**
   * Initialize the object based on the sentry configuration provided.
   * @param conf Sentry configuration
   * @param type Type indicates the service type
   */
  public SentryServiceClientTransportDefaultImpl(Configuration conf, ServiceTransportConstants.sentryService type)
    throws IOException {
    String hostsAndPortsStr;
    String[] hostsAndPortsStrArr;
    HostAndPort[] hostsAndPorts;
    int defaultPort;
    this.conf = conf;
    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");

    if (type == ServiceTransportConstants.sentryService.HDFS_SERVICE) {
      serviceConstants = new HdfsServiceTransportConstants();
    } else {
      serviceConstants = new PolicyServiceTransportConstants();
    }

    this.connectionTimeout = conf.getInt(serviceConstants.getServerRpcConnTimeout(),
      serviceConstants.getServerRpcConnTimeoutDefault());
    this.rpcRetryTotal = conf.getInt(serviceConstants.getSentryRpcRetryTotal(),
      serviceConstants.getSentryRpcRetryTotalDefault());
    this.connectionFullRetryTotal = conf.getInt(serviceConstants.getSentryFullRetryTotal(),
      serviceConstants.getSentryFullRetryTotalDefault());
    this.kerberos = serviceConstants.getSecurityModeKerberos().equalsIgnoreCase(
      conf.get(serviceConstants.getSecurityMode(), serviceConstants.getSecurityModeKerberos()).trim());

    hostsAndPortsStr = conf.get(serviceConstants.getServerRpcAddress());
    if (hostsAndPortsStr == null) {
      throw new RuntimeException("Config key " +
        serviceConstants.getServerRpcAddress() + " is required");
    }
    defaultPort = conf.getInt(serviceConstants.getServerRpcPort(), serviceConstants.getServerRpcPortDefault());

    hostsAndPortsStrArr = hostsAndPortsStr.split(",");
    hostsAndPorts = ThriftUtil.parseHostPortStrings(hostsAndPortsStrArr, defaultPort);

    this.endpoints = new ArrayList(hostsAndPortsStrArr.length);
    for( HostAndPort endpoint : hostsAndPorts) {
      this.endpoints.add(
        new InetSocketAddress(endpoint.getHostText(), endpoint.getPort()));
      LOGGER.debug("Added server endpoint: " + endpoint.toString());
    }
    serverAddress = null;
  }
  /**
   * Initialize the object based on the parameters provided provided.
   * @param addr Host address which the client needs to connect
   * @param port Host Port which the client needs to connect
   * @param conf Sentry configuration
   * @param type Type indicates the service type
   */
  public SentryServiceClientTransportDefaultImpl(String addr, int port, Configuration conf, ServiceTransportConstants.sentryService type)
    throws IOException {
    // copy the configuration because we may make modifications to it.
    this.conf = new Configuration(conf);

    Preconditions.checkNotNull(this.conf, "Configuration object cannot be null");
    if (type == ServiceTransportConstants.sentryService.HDFS_SERVICE) {
      serviceConstants = new HdfsServiceTransportConstants();
    } else {
      serviceConstants = new PolicyServiceTransportConstants();
    }

    this.serverAddress = NetUtils.createSocketAddr(Preconditions.checkNotNull(
      addr, "Config key " + serviceConstants.getServerRpcAddress() + " is required"), port);
    this.connectionTimeout = conf.getInt(serviceConstants.getServerRpcConnTimeout(),
      serviceConstants.getServerRpcConnTimeoutDefault());
    this.rpcRetryTotal = conf.getInt(serviceConstants.getSentryRpcRetryTotal(),
      serviceConstants.getSentryRpcRetryTotalDefault());
    this.connectionFullRetryTotal = conf.getInt(serviceConstants.getSentryFullRetryTotal(),
      serviceConstants.getSentryFullRetryTotalDefault());
    this.kerberos = serviceConstants.getSecurityModeKerberos().equalsIgnoreCase(
      conf.get(serviceConstants.getSecurityMode(), serviceConstants.getSecurityModeKerberos()).trim());
    endpoints = null;
  }


  /**
   * This is a no-op when already connected.
   * When there is a connection error, it will retry with another sentry server. It will
   * first cycle through all the available sentry servers, and then retry the whole server
   * list no more than connectionFullRetryTotal times. In this case, it won't introduce
   * more latency when some server fails. Also to prevent all clients connecting to the
   * same server, it will reorder the endpoints randomly after a full retry.
   * <p>
   * TODO: Have a small random sleep after a full retry to prevent all clients connecting to the same server.
   * <p>
   * TODO: Add metrics for the number of successful connects and errors per client, and total number of retries.
   */
  public synchronized void connectWithRetry(boolean tryAlternateServer) throws IOException {
    if (isConnected() && (!tryAlternateServer)) {
      return;
    }
    IOException currentException = null;
    // Here for each full connectWithRetry it will cycle through all available sentry
    // servers. Before each full connectWithRetry, it will shuffle the server list.
    for (int retryCount = 0; retryCount < connectionFullRetryTotal; retryCount++) {
      // Reorder endpoints randomly to prevent all clients connecting to the same endpoint
      // at the same time after a node failure.
      Collections.shuffle(endpoints);
      for (InetSocketAddress addr : endpoints) {
        try {
          if (serverAddress != null && serverAddress.equals(addr)) {
            continue;
          }
          serverAddress = addr;
          connect(addr);
          LOGGER.info(String.format("Connected to SentryServer: %s", addr.toString()));
          return;
        } catch (IOException e) {
          LOGGER.debug(String.format("Failed connection to %s: %s",
            addr.toString(), e.getMessage()), e);
          currentException = e;
        }
      }
    }

    // Throw exception as reaching the max full connectWithRetry number.
    LOGGER.error(
      String.format("Reach the max connection retry num %d ", connectionFullRetryTotal),
      currentException);
    throw currentException;
  }

  /**
   * Connect to the specified socket address and throw IOException if failed.
   */
  protected void connect(InetSocketAddress serverAddress) throws IOException {
    if (kerberos) {
      String serverPrincipal = Preconditions.checkNotNull(conf.get(serviceConstants.getPrincipal()), serviceConstants.getPrincipal() + " is required");
      // since the client uses hadoop-auth, we need to set kerberos in
      // hadoop-auth if we plan to use kerberos
      conf.set(HADOOP_SECURITY_AUTHENTICATION, serviceConstants.getSecurityModeKerberos());

      // Resolve server host in the same way as we are doing on server side
      serverPrincipal = SecurityUtil.getServerPrincipal(serverPrincipal, serverAddress.getAddress());
      LOGGER.debug("Using server kerberos principal: " + serverPrincipal);
      if (serverPrincipalParts == null) {
        serverPrincipalParts = SaslRpcServer.splitKerberosName(serverPrincipal);
        Preconditions.checkArgument(serverPrincipalParts.length == 3,
          "Kerberos principal should have 3 parts: " + serverPrincipal);
      }
      boolean wrapUgi = "true".equalsIgnoreCase(conf
        .get(serviceConstants.getSecurityUseUgiTransport(), "true"));
      transport = new UgiSaslClientTransport(SaslRpcServer.AuthMethod.KERBEROS.getMechanismName(),
        null, serverPrincipalParts[0], serverPrincipalParts[1],
        serviceConstants.getSaslProperties(), null, transport, wrapUgi, conf);

    } else {
      serverPrincipalParts = null;
      transport = new TSocket(serverAddress.getHostName(),
        serverAddress.getPort(), connectionTimeout);
    }
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IOException("Transport exception while opening transport: " + e.getMessage(), e);
    }
    LOGGER.debug("Successfully opened transport: " + transport + " to " + serverAddress);
  }

  protected boolean isConnected() {
    return transport != null && transport.isOpen();
  }

  public synchronized void close() {
    if (isConnected()) {
      transport.close();
    }
  }
  public int getRetryCount() { return rpcRetryTotal; }
}