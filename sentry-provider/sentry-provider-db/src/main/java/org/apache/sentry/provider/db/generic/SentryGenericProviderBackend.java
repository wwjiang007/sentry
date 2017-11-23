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
package org.apache.sentry.provider.db.generic;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.exception.SentryConfigurationException;
import org.apache.sentry.provider.common.CacheProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.common.ProviderBackendContext;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryRole;
import org.apache.sentry.provider.db.generic.tools.command.TSentryPrivilegeConverter;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * This class used when any component such as Hive, Solr or Sqoop want to integration with the Sentry service
 */
public class SentryGenericProviderBackend extends CacheProvider implements ProviderBackend {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryGenericProviderBackend.class);
  private final Configuration conf;
  private volatile boolean initialized = false;
  private String componentType;
  private String serviceName;
  private boolean enableCaching;
  private String privilegeConverter;

  // ProviderBackend should have the same construct to support the reflect in authBinding,
  // eg:SqoopAuthBinding
  public SentryGenericProviderBackend(Configuration conf, String resource) //NOPMD
      throws Exception {
    this.conf = conf;
    this.enableCaching = conf.getBoolean(ServiceConstants.ClientConfig.ENABLE_CACHING, ServiceConstants.ClientConfig.ENABLE_CACHING_DEFAULT);
    this.privilegeConverter = conf.get(ServiceConstants.ClientConfig.PRIVILEGE_CONVERTER);
    this.setServiceName(conf.get(ServiceConstants.ClientConfig.SERVICE_NAME));
    this.setComponentType(conf.get(ServiceConstants.ClientConfig.COMPONENT_TYPE));
  }

  @Override
  public void initialize(ProviderBackendContext context) {
    if (initialized) {
      throw new IllegalStateException("SentryGenericProviderBackend has already been initialized, cannot be initialized twice");
    }

    Preconditions.checkNotNull(serviceName, "Service name is not defined. Use configuration parameter: " + conf.get(ServiceConstants.ClientConfig.SERVICE_NAME));
    Preconditions.checkNotNull(componentType, "Component type is not defined. Use configuration parameter: " + conf.get(ServiceConstants.ClientConfig.COMPONENT_TYPE));

    if (enableCaching) {
      if (privilegeConverter == null) {
        throw new SentryConfigurationException(ServiceConstants.ClientConfig.PRIVILEGE_CONVERTER + " not configured.");
      }

      Constructor<?> privilegeConverterConstructor;
      TSentryPrivilegeConverter sentryPrivilegeConverter;
      try {
        privilegeConverterConstructor = Class.forName(privilegeConverter).getDeclaredConstructor(String.class, String.class);
        privilegeConverterConstructor.setAccessible(true);
        sentryPrivilegeConverter = (TSentryPrivilegeConverter) privilegeConverterConstructor.newInstance(getComponentType(), getServiceName());
      } catch (NoSuchMethodException | ClassNotFoundException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException("Failed to create privilege converter of type " + privilegeConverter, e);
      }
      LOGGER.debug("Starting Updateable Cache");
      UpdatableCache cache = new UpdatableCache(conf, getComponentType(), getServiceName(), sentryPrivilegeConverter);
      try {
        cache.startUpdateThread(true);
      } catch (Exception e) {
        throw new RuntimeException("Failed to get privileges from Sentry to build cache.", e);
      }
      super.initialize(cache);
    }
    this.initialized = true;
  }

  /**
   *  The Sentry-296(generate client for connection pooling) has already finished development and reviewed by now. When it
   *  was committed to master, the getClient method was needed to refactor using the connection pool
   */
  private SentryGenericServiceClient getClient() throws Exception {
    return SentryGenericServiceClientFactory.create(conf);
  }

  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups,
      ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    if (!initialized) {
      throw new IllegalStateException("SentryGenericProviderBackend has not been properly initialized");
    }
    if (enableCaching) {
      return super.getPrivileges(groups, roleSet, authorizableHierarchy);
    } else {
      try (SentryGenericServiceClient client = getClient()){
        return ImmutableSet.copyOf(client.listPrivilegesForProvider(componentType, serviceName,
            roleSet, groups, Arrays.asList(authorizableHierarchy)));
      } catch (SentryUserException e) {
        String msg = "Unable to obtain privileges from server: " + e.getMessage();
        LOGGER.error(msg, e);
      } catch (Exception e) {
        String msg = "Unable to obtain client:" + e.getMessage();
        LOGGER.error(msg, e);
      }
    }
    return ImmutableSet.of();
  }

  @Override
  public ImmutableSet<String> getRoles(Set<String> groups, ActiveRoleSet roleSet) {
    if (!initialized) {
      throw new IllegalStateException("SentryGenericProviderBackend has not been properly initialized");
    }
    if (enableCaching) {
      return super.getRoles(groups, roleSet);
    } else {
      try (SentryGenericServiceClient client = getClient()){
        Set<TSentryRole> tRoles = Sets.newHashSet();
        //get the roles according to group
        String requestor = UserGroupInformation.getCurrentUser().getShortUserName();
        for (String group : groups) {
          tRoles.addAll(client.listRolesByGroupName(requestor, group, getComponentType()));
        }
        Set<String> roles = Sets.newHashSet();
        for (TSentryRole tRole : tRoles) {
          roles.add(tRole.getRoleName());
        }
        return ImmutableSet.copyOf(roleSet.isAll() ? roles : Sets.intersection(roles, roleSet.getRoles()));
      } catch (SentryUserException e) {
        String msg = "Unable to obtain roles from server: " + e.getMessage();
        LOGGER.error(msg, e);
      } catch (Exception e) {
        String msg = "Unable to obtain client:" + e.getMessage();
        LOGGER.error(msg, e);
      }
      return ImmutableSet.of();
    }
  }

  /**
   * SentryGenericProviderBackend does nothing in the validatePolicy()
   */
  @Override
  public void validatePolicy(boolean strictValidation)
      throws SentryConfigurationException {
    if (!initialized) {
      throw new IllegalStateException("SentryGenericProviderBackend has not been properly initialized");
    }
  }

  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups, Set<String> users,
                                              ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    // SentryGenericProviderBackend doesn't support getPrivileges for user now.
    return getPrivileges(groups, roleSet);
  }

  @Override
  public void close() {
  }

  public void setComponentType(String componentType) {
    this.componentType = componentType;
  }

  public String getComponentType() {
    return componentType;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }
}
