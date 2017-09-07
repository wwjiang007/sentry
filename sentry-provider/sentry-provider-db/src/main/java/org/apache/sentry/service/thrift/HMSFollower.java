/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package org.apache.sentry.service.thrift;


import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.List;
import javax.jdo.JDODataStoreException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME_DEPRECATED;
import org.apache.sentry.provider.db.service.persistent.PathsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HMSFollower is the thread which follows the Hive MetaStore state changes from Sentry.
 * It gets the full update and notification logs from HMS and applies it to
 * update permissions stored in Sentry using SentryStore and also update the &lt obj,path &gt state
 * stored for HDFS-Sentry sync.
 */
public class HMSFollower implements Runnable, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HMSFollower.class);
  private static boolean connectedToHms = false;
  private SentryHMSClient client;
  private final Configuration authzConf;
  private final SentryStore sentryStore;
  private final NotificationProcessor notificationProcessor;
  private final HiveNotificationFetcher notificationFetcher;
  private final boolean hdfsSyncEnabled;

  private final LeaderStatusMonitor leaderMonitor;

  /**
   * Configuring Hms Follower thread.
   *
   * @param conf sentry configuration
   * @param store sentry store
   * @param leaderMonitor singleton instance of LeaderStatusMonitor
   */
  HMSFollower(Configuration conf, SentryStore store, LeaderStatusMonitor leaderMonitor,
              HiveConnectionFactory hiveConnectionFactory) {
    this(conf, store, leaderMonitor, hiveConnectionFactory, null);
  }

  /**
   * Constructor should be used only for testing purposes.
   *
   * @param conf sentry configuration
   * @param store sentry store
   * @param leaderMonitor
   * @param authServerName Server that sentry is Authorizing
   */
  @VisibleForTesting
  public HMSFollower(Configuration conf, SentryStore store, LeaderStatusMonitor leaderMonitor,
              HiveConnectionFactory hiveConnectionFactory, String authServerName) {
    LOGGER.info("HMSFollower is being initialized");
    authzConf = conf;
    this.leaderMonitor = leaderMonitor;
    sentryStore = store;

    if (authServerName == null) {
      authServerName = conf.get(AUTHZ_SERVER_NAME.getVar(),
        conf.get(AUTHZ_SERVER_NAME_DEPRECATED.getVar(), AUTHZ_SERVER_NAME_DEPRECATED.getDefault()));
    }

    notificationProcessor = new NotificationProcessor(sentryStore, authServerName, authzConf);
    client = new SentryHMSClient(authzConf, hiveConnectionFactory);
    hdfsSyncEnabled = SentryServiceUtil.isHDFSSyncEnabledNoCache(authzConf); // no cache to test different settings for hdfs sync
    notificationFetcher = new HiveNotificationFetcher(sentryStore, hiveConnectionFactory);
  }

  @VisibleForTesting
  public static boolean isConnectedToHms() {
    return connectedToHms;
  }

  @VisibleForTesting
  void setSentryHmsClient(SentryHMSClient client) {
    this.client = client;
  }

  @Override
  public void close() {
    if (client != null) {
      // Close any outstanding connections to HMS
      try {
        client.disconnect();
      } catch (Exception failure) {
        LOGGER.error("Failed to close the Sentry Hms Client", failure);
      }
    }

    notificationFetcher.close();
  }

  @Override
  public void run() {
    long lastProcessedNotificationId;
    try {
      // Initializing lastProcessedNotificationId based on the latest persisted notification ID.
      lastProcessedNotificationId = sentryStore.getLastProcessedNotificationID();
    } catch (Exception e) {
      LOGGER.error("Failed to get the last processed notification id from sentry store, "
          + "Skipping the processing", e);
      return;
    }
    // Wake any clients connected to this service waiting for HMS already processed notifications.
    wakeUpWaitingClientsForSync(lastProcessedNotificationId);
    // Only the leader should listen to HMS updates
    if (!isLeader()) {
      // Close any outstanding connections to HMS
      close();
      return;
    }
    syncupWithHms(lastProcessedNotificationId);
  }

  private boolean isLeader() {
    return (leaderMonitor == null) || leaderMonitor.isLeader();
  }

  @VisibleForTesting
  String getAuthServerName() {
    return notificationProcessor.getAuthServerName();
  }

  /**
   * Processes new Hive Metastore notifications.
   *
   * <p>If no notifications are processed yet, then it
   * does a full initial snapshot of the Hive Metastore followed by new notifications updates that
   * could have happened after it.
   *
   * <p>Clients connections waiting for an event notification will be
   * woken up afterwards.
   */
  private void syncupWithHms(long notificationId) {
    try {
      client.connect();
      connectedToHms = true;
    } catch (Throwable e) {
      LOGGER.error("HMSFollower cannot connect to HMS!!", e);
      return;
    }

    try {
      /* Before getting notifications, it checks if a full HMS snapshot is required. */
      if (isFullSnapshotRequired(notificationId)) {
        createFullSnapshot();
        return;
      }

      Collection<NotificationEvent> notifications =
          notificationFetcher.fetchNotifications(notificationId);

      // After getting notifications, it checks if the HMS did some clean-up and notifications
      // are out-of-sync with Sentry.
      if (areNotificationsOutOfSync(notifications, notificationId)) {
        createFullSnapshot();
        return;
      }

      // Continue with processing new notifications if no snapshots are done.
      processNotifications(notifications);
    } catch (TException e) {
      LOGGER.error("An error occurred while fetching HMS notifications: {}", e.getMessage());
      close();
    } catch (Throwable t) {
      // catching errors to prevent the executor to halt.
      LOGGER.error("Exception in HMSFollower! Caused by: " + t.getMessage(), t);

      close();
    }
  }

  /**
   * Checks if a new full HMS snapshot request is needed by checking if:
   * <ul>
   *   <li>No snapshots has been persisted yet.</li>
   *   <li>The current notification Id on the HMS is less than the
   *   latest processed by Sentry.</li>
   * </ul>
   *
   * @param latestSentryNotificationId The notification Id to check against the HMS
   * @return True if a full snapshot is required; False otherwise.
   * @throws Exception If an error occurs while checking the SentryStore or the HMS client.
   */
  private boolean isFullSnapshotRequired(long latestSentryNotificationId) throws Exception {
    if (sentryStore.isHmsNotificationEmpty()) {
      return true;
    }

    long currentHmsNotificationId = notificationFetcher.getCurrentNotificationId();
    if (currentHmsNotificationId < latestSentryNotificationId) {
      LOGGER.info("The latest notification ID on HMS is less than the latest notification ID "
          + "processed by Sentry. Need to request a full HMS snapshot.");
      return true;
    }

    return false;
  }

  /**
   * Checks if the HMS and Sentry processed notifications are out-of-sync.
   * This could happen because the HMS did some clean-up of old notifications
   * and Sentry was not requesting notifications during that time.
   *
   * @param events All new notifications to check for an out-of-sync.
   * @param latestProcessedId The latest notification processed by Sentry to check against the
   *        list of notifications events.
   * @return True if an out-of-sync is found; False otherwise.
   */
  private boolean areNotificationsOutOfSync(Collection<NotificationEvent> events,
      long latestProcessedId) {
    if (events.isEmpty()) {
      return false;
    }

    /*
     * If the sequence of notifications has a gap, then an out-of-sync might
     * have happened due to the following issue:
     *
     * - HDFS sync was disabled or Sentry was shutdown for a time period longer than
     * the HMS notification clean-up thread causing old notifications to be deleted.
     *
     * HMS notifications may contain both gaps in the sequence and duplicates
     * (the same ID repeated more then once for different events).
     *
     * To accept duplicates (see NotificationFetcher for more info), then a gap is found
     * if the 1st notification received is higher than the current ID processed + 1.
     * i.e.
     *   1st ID = 3, latest ID = 3 (duplicate found but no gap detected)
     *   1st ID = 4, latest ID = 3 (consecutive ID found but no gap detected)
     *   1st ID = 5, latest ID = 3 (a gap is detected)
     */

    List<NotificationEvent> eventList = (List<NotificationEvent>) events;
    long firstNotificationId = eventList.get(0).getEventId();

    if (firstNotificationId > (latestProcessedId + 1)) {
      LOGGER.info("Current HMS notifications are out-of-sync with latest Sentry processed"
          + "notifications. Need to request a full HMS snapshot.");
      return true;
    }

    return false;
  }

  /**
   * Request for full snapshot and persists it if there is no snapshot available in the
   * sentry store. Also, wakes-up any waiting clients.
   *
   * @return ID of last notification processed.
   * @throws Exception if there are failures
   */
  private long createFullSnapshot() throws Exception {
    LOGGER.debug("Attempting to take full HMS snapshot");
    PathsImage snapshotInfo = client.getFullSnapshot();
    if (snapshotInfo.getPathImage().isEmpty()) {
      return snapshotInfo.getId();
    }

    // Check we're still the leader before persisting the new snapshot
    if (!isLeader()) {
      return SentryStore.EMPTY_NOTIFICATION_ID;
    }

    try {
      LOGGER.debug("Persisting HMS path full snapshot");

      if (hdfsSyncEnabled) {
        sentryStore.persistFullPathsImage(snapshotInfo.getPathImage(), snapshotInfo.getId());
      } else {
        // We need to persist latest notificationID for next poll
        sentryStore.persistLastProcessedNotificationID(snapshotInfo.getId());
      }
    } catch (Exception failure) {
      LOGGER.error("Received exception while persisting HMS path full snapshot ");
      throw failure;
    }
    // Wake up any HMS waiters that could have been put on hold before getting the
    // eventIDBefore value.
    wakeUpWaitingClientsForSync(snapshotInfo.getId());
    // HMSFollower connected to HMS and it finished full snapshot if that was required
    // Log this message only once
    LOGGER.info("Sentry HMS support is ready");
    return snapshotInfo.getId();
  }

  /**
   * Process the collection of notifications and wake up any waiting clients.
   * Also, persists the notification ID regardless of processing result.
   *
   * @param events list of event to be processed
   * @throws Exception if the complete notification list is not processed because of JDO Exception
   */
  public void processNotifications(Collection<NotificationEvent> events) throws Exception {
    boolean isNotificationProcessed;
    if (events.isEmpty()) {
      return;
    }

    for (NotificationEvent event : events) {
      isNotificationProcessed = false;
      try {
        // Only the leader should process the notifications
        if (!isLeader()) {
          return;
        }
        isNotificationProcessed = notificationProcessor.processNotificationEvent(event);
      } catch (Exception e) {
        if (e.getCause() instanceof JDODataStoreException) {
          LOGGER.info("Received JDO Storage Exception, Could be because of processing "
              + "duplicate notification");
          if (event.getEventId() <= sentryStore.getLastProcessedNotificationID()) {
            // Rest of the notifications need not be processed.
            LOGGER.error("Received event with Id: {} which is smaller then the ID "
                + "persisted in store", event.getEventId());
            break;
          }
        } else {
          LOGGER.error("Processing the notification with ID:{} failed with exception {}",
              event.getEventId(), e);
        }
      }
      if (!isNotificationProcessed) {
        try {
          // Update the notification ID in the persistent store even when the notification is
          // not processed as the content in in the notification is not valid.
          // Continue processing the next notification.
          LOGGER.debug("Explicitly Persisting Notification ID:{}", event.getEventId());
          sentryStore.persistLastProcessedNotificationID(event.getEventId());
        } catch (Exception failure) {
          LOGGER.error("Received exception while persisting the notification ID "
              + event.getEventId());
          throw failure;
        }
      }
      // Wake up any HMS waiters that are waiting for this ID.
      wakeUpWaitingClientsForSync(event.getEventId());
    }
  }

  /**
   * Wakes up HMS waiters waiting for a specific event notification.
   *
   * @param eventId Id of a notification
   */
  private void wakeUpWaitingClientsForSync(long eventId) {
    CounterWait counterWait = sentryStore.getCounterWait();

    // Wake up any HMS waiters that are waiting for this ID.
    // counterWait should never be null, but tests mock SentryStore and a mocked one
    // doesn't have it.
    if (counterWait != null) {
      counterWait.update(eventId);
    }
  }
}
