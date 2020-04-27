package com.google.cloud.spanner.publisher;

import com.google.api.core.ApiService;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
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
class Samples {
  /** Publish changes from a single table in a database to Pubsub. */
  static void publishChangesFromSingleTableExample() throws InterruptedException, IOException {
    String projectId = ServiceOptions.getDefaultProjectId();
    // Cloud Spanner configuration.
    String instance = "my-instance";
    String database = "my-database";
    String table = "MY_TABLE";
    // Pubsub configuration.
    String topic = "my-topic";

    // Setup Spanner change watcher.
    Spanner spanner = SpannerOptions.getDefaultInstance().getService();
    DatabaseId databaseId = DatabaseId.of(projectId, instance, database);
    TableId tableId = TableId.of(databaseId, table);
    SpannerTableChangeWatcher watcher = SpannerTableTailer.newBuilder(spanner, tableId).build();

    // Setup Spanner change publisher.
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerTableChangeEventPublisher eventPublisher =
        SpannerTableChangeEventPublisher.newBuilder(watcher, client)
            .setTopicName(String.format("projects/%s/topics/%s", projectId, topic))
            .setListener(
                new PublishListener() {
                  @Override
                  public void onPublished(
                      TableId table, Timestamp commitTimestamp, String messageId) {
                    System.out.printf(
                        "Published change for table %s at %s%n", table, commitTimestamp.toString());
                    latch.countDown();
                  }
                })
            .build();
    // Start the change publisher. This will automatically also start the change watcher.
    eventPublisher.startAsync().awaitRunning();
    // Wait until we have published 3 changes.
    latch.await();
    // Stop the publisher and wait for it to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
  }

  /** Publish changes from all tables in a database to a single Pubsub topic. */
  static void publishChangesFromAllTablesExample() throws InterruptedException, IOException {
    String projectId = ServiceOptions.getDefaultProjectId();
    // Cloud Spanner configuration.
    String instance = "my-instance";
    String database = "my-database";
    // Pubsub configuration.
    String topic = "my-topic";

    // Setup Spanner change watcher.
    Spanner spanner = SpannerOptions.getDefaultInstance().getService();
    DatabaseId databaseId = DatabaseId.of(projectId, instance, database);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).build();

    // Setup Spanner change publisher.
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerDatabaseChangeEventPublisher eventPublisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
            .setTopicNameFormat(String.format("projects/%s/topics/%s", projectId, topic))
            .setListener(
                new PublishListener() {
                  @Override
                  public void onPublished(
                      TableId table, Timestamp commitTimestamp, String messageId) {
                    System.out.printf(
                        "Published change for table %s at %s%n", table, commitTimestamp.toString());
                    latch.countDown();
                  }
                })
            .build();
    // Start the change publisher. This will automatically also start the change watcher.
    eventPublisher.startAsync().awaitRunning();
    // Wait until we have published 3 changes.
    latch.await();
    // Stop the publisher and wait for it to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
  }

  /** Publish changes from all tables in a database to a separate Pubsub topic per table. */
  static void publishChangesFromAllTablesToSeparateTopicsExample()
      throws InterruptedException, IOException {
    String projectId = ServiceOptions.getDefaultProjectId();
    // Cloud Spanner configuration.
    String instance = "my-instance";
    String database = "my-database";
    // Pubsub configuration.
    String topicFormat = "change-log-%database%-%table%";

    // Setup Spanner change watcher.
    Spanner spanner = SpannerOptions.getDefaultInstance().getService();
    DatabaseId databaseId = DatabaseId.of(projectId, instance, database);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).build();

    // Setup Spanner change publisher.
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerDatabaseChangeEventPublisher eventPublisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
            .setTopicNameFormat(String.format("projects/%s/topics/%s", projectId, topicFormat))
            .setListener(
                new PublishListener() {
                  @Override
                  public void onPublished(
                      TableId table, Timestamp commitTimestamp, String messageId) {
                    System.out.printf(
                        "Published change for table %s at %s%n", table, commitTimestamp.toString());
                    latch.countDown();
                  }
                })
            .build();
    // Start the change publisher. This will automatically also start the change watcher.
    eventPublisher.startAsync().awaitRunning();
    // Wait until we have published 3 changes.
    latch.await();
    // Stop the publisher and wait for it to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
  }

  /**
   * Change publishers implement the {@link ApiService} interface and allows users to be notified if
   * a publisher fails.
   */
  static void errorHandling() throws InterruptedException, IOException {
    String projectId = ServiceOptions.getDefaultProjectId();
    // Cloud Spanner configuration.
    String instance = "my-instance";
    String database = "my-database";
    // Pubsub configuration.
    String topic = "my-topic";

    // Setup Spanner change watcher.
    Spanner spanner = SpannerOptions.getDefaultInstance().getService();
    DatabaseId databaseId = DatabaseId.of(projectId, instance, database);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).build();

    // Setup Spanner change publisher.
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerDatabaseChangeEventPublisher eventPublisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
            .setTopicNameFormat(String.format("projects/%s/topics/%s", projectId, topic))
            .setListener(
                new PublishListener() {
                  @Override
                  public void onPublished(
                      TableId table, Timestamp commitTimestamp, String messageId) {
                    System.out.printf(
                        "Published change for table %s at %s%n", table, commitTimestamp.toString());
                    latch.countDown();
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
    // Wait until we have published 3 changes.
    latch.await();
    // Stop the publisher and wait for it to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
  }
}
