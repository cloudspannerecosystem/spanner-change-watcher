/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.publisher;

import com.google.api.core.ApiService;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher.PublishListener;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/** Samples for spanner-change-publisher. */
public class Samples {
  // The Spanner database that is being watched and the Pubsub topic that the changes are being
  // published to may reside in different Google Cloud projects.
  private final String spannerProjectId;
  private final Credentials spannerCredentials;
  private final String pubsubProjectId;
  private final Credentials pubsubCredentials;

  public Samples() throws IOException {
    this(
        ServiceOptions.getDefaultProjectId(),
        GoogleCredentials.getApplicationDefault(),
        ServiceOptions.getDefaultProjectId(),
        GoogleCredentials.getApplicationDefault());
  }

  public Samples(
      String spannerProjectId,
      Credentials spannerCredentials,
      String pubsubProjectId,
      Credentials pubsubCredentials) {
    this.spannerProjectId = spannerProjectId;
    this.spannerCredentials = spannerCredentials;
    this.pubsubProjectId = pubsubProjectId;
    this.pubsubCredentials = pubsubCredentials;
  }

  /** Publish changes from a single table in a database to Pubsub. */
  public void publishChangesFromSingleTableExample(
      String instance, // "my-instance"
      String database, // "my-database"
      String table, // "MY_TABLE"
      String topic // "my-topic"
      ) throws InterruptedException, IOException {

    // Setup Spanner change watcher.
    Spanner spanner =
        SpannerOptions.newBuilder()
            .setProjectId(spannerProjectId)
            .setCredentials(spannerCredentials)
            .build()
            .getService();
    DatabaseId databaseId = DatabaseId.of(spannerProjectId, instance, database);
    TableId tableId = TableId.of(databaseId, table);
    SpannerTableChangeWatcher watcher = SpannerTableTailer.newBuilder(spanner, tableId).build();

    // Setup Spanner change publisher.
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerTableChangeEventPublisher eventPublisher =
        SpannerTableChangeEventPublisher.newBuilder(watcher, client)
            .setTopicName(String.format("projects/%s/topics/%s", pubsubProjectId, topic))
            .setCredentials(pubsubCredentials)
            .addListener(
                new PublishListener() {
                  @Override
                  public void onPublished(
                      TableId table, Timestamp commitTimestamp, String messageId) {
                    System.out.printf(
                        "Published change for table %s at %s%n", table, commitTimestamp.toString());
                    latch.countDown();
                  }

                  @Override
                  public void onFailure(TableId table, Timestamp commitTimestamp, Throwable t) {
                    System.err.printf(
                        "Failed to publish change for table %s at %s: %s",
                        table, commitTimestamp, t);
                  }
                })
            .build();
    // Start the change publisher. This will automatically also start the change watcher.
    eventPublisher.startAsync().awaitRunning();
    System.out.println("Change publisher started");
    // Wait until we have published 3 changes.
    latch.await();
    // Stop the publisher and wait for it to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
  }

  /** Publish changes from all tables in a database to a single Pubsub topic. */
  public void publishChangesFromAllTablesExample(
      String instance, // "my-instance"
      String database, // "my-database"
      String topic // "my-topic"
      ) throws InterruptedException, IOException {
    // Setup Spanner change watcher.
    Spanner spanner =
        SpannerOptions.newBuilder()
            .setProjectId(spannerProjectId)
            .setCredentials(spannerCredentials)
            .build()
            .getService();
    DatabaseId databaseId = DatabaseId.of(spannerProjectId, instance, database);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();

    // Setup Spanner change publisher.
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerDatabaseChangeEventPublisher eventPublisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
            .setTopicNameFormat(String.format("projects/%s/topics/%s", pubsubProjectId, topic))
            .addListener(
                new PublishListener() {
                  @Override
                  public void onPublished(
                      TableId table, Timestamp commitTimestamp, String messageId) {
                    System.out.printf(
                        "Published change for table %s at %s%n", table, commitTimestamp.toString());
                    latch.countDown();
                  }

                  @Override
                  public void onFailure(TableId table, Timestamp commitTimestamp, Throwable t) {
                    System.err.printf(
                        "Failed to publish change for table %s at %s: %s",
                        table, commitTimestamp, t);
                  }
                })
            .setCredentials(pubsubCredentials)
            .build();
    // Start the change publisher. This will automatically also start the change watcher.
    eventPublisher.startAsync().awaitRunning();
    System.out.println("Change publisher started");
    // Wait until we have published 3 changes.
    latch.await();
    // Stop the publisher and wait for it to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
  }

  /** Publish changes from all tables in a database to a separate Pubsub topic per table. */
  public void publishChangesFromAllTablesToSeparateTopicsExample(
      String instance, // "my-instance"
      String database // "my-database"
      ) throws InterruptedException, IOException {
    // Pubsub configuration.
    String topicFormat = "change-log-%database%-%table%";

    // Setup Spanner change watcher.
    Spanner spanner =
        SpannerOptions.newBuilder()
            .setProjectId(spannerProjectId)
            .setCredentials(spannerCredentials)
            .build()
            .getService();
    DatabaseId databaseId = DatabaseId.of(spannerProjectId, instance, database);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();

    // Setup Spanner change publisher.
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerDatabaseChangeEventPublisher eventPublisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
            .setTopicNameFormat(
                String.format("projects/%s/topics/%s", pubsubProjectId, topicFormat))
            .addListener(
                new PublishListener() {
                  @Override
                  public void onPublished(
                      TableId table, Timestamp commitTimestamp, String messageId) {
                    System.out.printf(
                        "Published change for table %s at %s%n", table, commitTimestamp.toString());
                    latch.countDown();
                  }

                  @Override
                  public void onFailure(TableId table, Timestamp commitTimestamp, Throwable t) {
                    System.err.printf(
                        "Failed to publish change for table %s at %s: %s",
                        table, commitTimestamp, t);
                  }
                })
            .setCredentials(pubsubCredentials)
            .build();
    // Start the change publisher. This will automatically also start the change watcher.
    eventPublisher.startAsync().awaitRunning();
    System.out.println("Change publisher started");
    // Wait until we have published 3 changes.
    latch.await();
    // Stop the publisher and wait for it to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
  }

  /**
   * Change publishers implement the {@link ApiService} interface and allows users to be notified if
   * a publisher fails.
   */
  public void errorHandling(
      String instance, // "my-instance"
      String database, // "my-database"
      String topic // "my-topic"
      ) throws InterruptedException, IOException {
    // Setup Spanner change watcher.
    Spanner spanner =
        SpannerOptions.newBuilder()
            .setProjectId(spannerProjectId)
            .setCredentials(spannerCredentials)
            .build()
            .getService();
    DatabaseId databaseId = DatabaseId.of(spannerProjectId, instance, database);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();

    // Setup Spanner change publisher.
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerDatabaseChangeEventPublisher eventPublisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
            .setTopicNameFormat(String.format("projects/%s/topics/%s", pubsubProjectId, topic))
            .addListener(
                new PublishListener() {
                  @Override
                  public void onPublished(
                      TableId table, Timestamp commitTimestamp, String messageId) {
                    System.out.printf(
                        "Published change for table %s at %s%n", table, commitTimestamp.toString());
                    latch.countDown();
                  }

                  @Override
                  public void onFailure(TableId table, Timestamp commitTimestamp, Throwable t) {
                    System.err.printf(
                        "Failed to publish change for table %s at %s: %s",
                        table, commitTimestamp, t);
                  }
                })
            .build();
    // Add an ApiService listener to watch for failures.
    eventPublisher.addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            // A failed publisher cannot be restarted and will stop the publishing of any further
            // row changes.
            System.err.printf(
                "Database change publisher failed.%n    State before failure: %s%n    Error: %s%n",
                from, failure.getMessage());
            System.exit(1);
          }
        },
        MoreExecutors.directExecutor());
    // Start the change publisher. This will automatically also start the change watcher.
    eventPublisher.startAsync().awaitRunning();
    System.out.println("Change publisher started");
    // Wait until we have published 3 changes.
    latch.await();
    // Stop the publisher and wait for it to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
  }
}
