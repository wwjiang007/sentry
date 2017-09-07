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
package org.apache.sentry.core.common.transport;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.exception.SentryHdfsServiceException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The RetryClientInvocationHandler is a proxy class for handling thrift calls for non-pool
 * model. Currently only one client connection is allowed.
 * <p>
 * For every rpc call, if the client is not connected, it will first connect to one of the
 * sentry servers, and then do the thrift call to the connected sentry server, which will
 * execute the requested method and return back the response. If it is failed with connection
 * problem, it will close the current connection and retry (reconnect and resend the
 * thrift call) no more than rpcRetryTotal times. If the client is already connected, it
 * will reuse the existing connection, and do the thrift call.
 * <p>
 * During reconnection, invocatiaon handler will first cycle through all the configured sentry servers, and
 * then retry the whole server list no more than connectionFullRetryTotal times. In this
 * case, it won't introduce more latency when some server fails.
 * <p>
 */

public final class RetryClientInvocationHandler extends SentryClientInvocationHandler {
  private static final Logger LOGGER =
    LoggerFactory.getLogger(RetryClientInvocationHandler.class);
  private SentryConnection client = null;
  private final int maxRetryCount;
  private final long connRetryDelayInMs;

  /**
   * Initialize the sentry configurations, including rpc retry count and client connection
   * configs for SentryPolicyServiceClientDefaultImpl
   */
  public RetryClientInvocationHandler(Configuration conf, SentryConnection clientObject,
                                      SentryClientTransportConfigInterface transportConfig) {
    Preconditions.checkNotNull(conf, "Configuration object cannot be null");
    Preconditions.checkNotNull(clientObject, "Client Object cannot be null");
    client = clientObject;
    maxRetryCount = transportConfig.getSentryRpcRetryTotal(conf);
    connRetryDelayInMs = transportConfig.getSentryRpcConnRetryDelayInMs(conf);
  }

  /**
   * For every rpc call, if the client is not connected, it will first connect to a sentry
   * server, and then do the thrift call to the connected sentry server, which will
   * execute the requested method and return back the response. If it is failed with
   * connection problem, it will close the current connection, and retry (reconnect and
   * resend the thrift call) no more than rpcRetryTotal times. Throw SentryUserException
   * if failed retry after rpcRetryTotal times.
   * if it is failed with other exception, method would just re-throw the exception.
   */
  @Override
  public synchronized Object invokeImpl(Object proxy, Method method, Object[] args) throws Exception {
    String methodName = method.getName();

    // This is an interesting special case. When we running a debugging session, it may try to
    // show the client value by calling toString(). We do not want any connection
    // to be established for toString() method.
    if ("toString".equals(methodName)) {
      return method.invoke(client, args);
    }

    Exception lastExc = null;
    for (int retryCount = 0; retryCount < maxRetryCount; retryCount++) {
      connect();

      // do the thrift call
      try {
        LOGGER.debug("Calling {}", methodName);
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        // Get the target exception, check if SentryUserException or TTransportException is wrapped.
        // TTransportException means there is a connection problem.
        LOGGER.error("failed to execute {}", method.getName(), e);
        Throwable targetException = e.getCause();
        if (!((targetException instanceof SentryUserException) ||
            (targetException instanceof SentryHdfsServiceException))) {
          throw e;
        }
        Throwable sentryTargetException = targetException.getCause();
        // If there has connection problem, eg, invalid connection if the service restarted,
        // sentryTargetException instanceof TTransportException will be true.
        if (sentryTargetException instanceof TTransportException) {
          // Retry when the exception is caused by connection problem.
          lastExc = new TTransportException(sentryTargetException);
          LOGGER.error("Thrift call failed", lastExc);
          // The connection to the server is bad, inform the client of the problem
          client.invalidate();
        } else {
          // Semantic exception which does not indicate the connection failure.
          // Do not need to reconnect to the sentry server.
          if (targetException instanceof SentryUserException) {
            throw (SentryUserException) targetException;
          } else {
            throw (SentryHdfsServiceException) targetException;
          }
        }
      }
    }

    // Throw the exception as reaching the max rpc retry num.
    String error = String.format("Request failed, %d retries attempted ", maxRetryCount);
    throw new SentryUserException(error, lastExc);
  }

  /**
   * Connect the client, retry multiple times
   * @throws Exception
   */
  private void connect() throws Exception {
    Throwable lastExc = null;
    for (int retryCount = 0;  retryCount < maxRetryCount; retryCount++) {
      try {
        // If there is a TTransportException while connecting to sentry server, retry is
        // attempted. For the rest, retry is not attempted.
        client.connect();
        return;
      } catch (TTransportException failure) {
        // Retry when the exception is caused by connection problem.
        LOGGER.error("Failed to connect", failure);
        lastExc = failure;
      } catch (Exception failure) {
        Throwable causeException = failure.getCause();
        if (!(causeException instanceof TTransportException)) {
          LOGGER.error("Failed to connect to Sentry Server", failure);
          throw new SentryUserException(failure.getMessage(), causeException);
        }
        lastExc = causeException;
      }

      // Sleep until connection retry delay
      try {
        Thread.sleep(connRetryDelayInMs);
      } catch (InterruptedException ex) {
        // Resetting the interrupt flag
        Thread.currentThread().interrupt();
        throw ex;
      }
    }
    assert lastExc != null;
    throw new SentryUserException (lastExc.getMessage(), lastExc);
  }

  @Override
  public synchronized void close() {
    //We are done with this client
    client.done();
  }
}
