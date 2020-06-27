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

package com.google.cloud.spanner.publisher.sample;

import com.google.api.core.ApiService;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.publisher.SpannerDatabaseChangeEventPublisher;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher.PublishListener;
import com.google.cloud.spanner.publisher.SpannerToAvroFactory;
import com.google.cloud.spanner.publisher.SpannerToAvroFactory.SpannerToAvro;
import com.google.cloud.spanner.publisher.SpannerToJsonFactory;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.avro.generic.GenericRecord;

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

  /**
   * Publish changes from all tables in a database to a separate Pubsub topic per table. The topics
   * are created automatically by the Publisher.
   */
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
            .setCreateTopicsIfNotExist(true)
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

  /** Subscribe changes from Pubsub and decode Avro data. */
  public void subscribeToChanges(
      String instance, // "my-instance"
      String database, // "my-database"
      String topic, // "my-topic"
      String subscription // "my-subscription" subscribing to "my-topic"
      ) throws IOException, InterruptedException {
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
            .setCredentials(pubsubCredentials)
            .build();
    // Start the change publisher. This will automatically also start the change watcher.
    eventPublisher.startAsync().awaitRunning();
    System.out.println("Change publisher started");

    // Keep a cache of converters as these are expensive to create.
    Map<TableId, SpannerToAvro> converters = new HashMap<>();
    // Start a subscriber.
    Subscriber subscriber =
        Subscriber.newBuilder(
                ProjectSubscriptionName.of(pubsubProjectId, subscription),
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    // Get the change metadata.
                    DatabaseId database = DatabaseId.of(message.getAttributesOrThrow("Database"));
                    TableId table = TableId.of(database, message.getAttributesOrThrow("Table"));
                    Timestamp commitTimestamp =
                        Timestamp.parseTimestamp(message.getAttributesOrThrow("Timestamp"));
                    // Get the changed row and decode the data.
                    SpannerToAvro converter = converters.get(table);
                    if (converter == null) {
                      converter = SpannerToAvroFactory.INSTANCE.create(client, table);
                      converters.put(table, converter);
                    }
                    try {
                      GenericRecord record = converter.decodeRecord(message.getData());
                      System.out.println("--- Received changed record ---");
                      System.out.printf("Database: %s%n", database);
                      System.out.printf("Table: %s%n", table);
                      System.out.printf("Commit timestamp: %s%n", commitTimestamp);
                      System.out.printf("Data: %s%n", record);
                    } catch (Exception e) {
                      System.err.printf("Failed to decode avro record: %s%n", e.getMessage());
                    } finally {
                      consumer.ack();
                      latch.countDown();
                    }
                  }
                })
            .setCredentialsProvider(FixedCredentialsProvider.create(pubsubCredentials))
            .build();
    subscriber.startAsync().awaitRunning();

    // Wait until we have received 3 changes.
    latch.await();
    // Stop the publisher and subscriber and wait for them to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
    subscriber.stopAsync().awaitTerminated();
  }

  /** Subscribe changes from Pubsub and decode JSON data. */
  public void subscribeToChangesAsJson(
      String instance, // "my-instance"
      String database, // "my-database"
      String topic, // "my-topic"
      String subscription // "my-subscription" subscribing to "my-topic"
      ) throws IOException, InterruptedException {
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

    // Setup Spanner change publisher to publish changes in JSON format.
    final CountDownLatch latch = new CountDownLatch(3);
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerDatabaseChangeEventPublisher eventPublisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
            .setTopicNameFormat(String.format("projects/%s/topics/%s", pubsubProjectId, topic))
            .setConverterFactory(SpannerToJsonFactory.INSTANCE)
            .setCredentials(pubsubCredentials)
            .build();
    // Start the change publisher. This will automatically also start the change watcher.
    eventPublisher.startAsync().awaitRunning();
    System.out.println("Change publisher started");

    // Start a subscriber.
    Subscriber subscriber =
        Subscriber.newBuilder(
                ProjectSubscriptionName.of(pubsubProjectId, subscription),
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    // Get the change metadata.
                    DatabaseId database = DatabaseId.of(message.getAttributesOrThrow("Database"));
                    TableId table = TableId.of(database, message.getAttributesOrThrow("Table"));
                    Timestamp commitTimestamp =
                        Timestamp.parseTimestamp(message.getAttributesOrThrow("Timestamp"));
                    // Get the changed row and decode the data.
                    try {
                      JsonElement json = JsonParser.parseString(message.getData().toStringUtf8());
                      System.out.println("--- Received changed record ---");
                      System.out.printf("Database: %s%n", database);
                      System.out.printf("Table: %s%n", table);
                      System.out.printf("Commit timestamp: %s%n", commitTimestamp);
                      System.out.printf("Data: %s%n", json.toString());
                    } catch (Exception e) {
                      System.err.printf("Failed to parse json record: %s%n", e.getMessage());
                    } finally {
                      consumer.ack();
                      latch.countDown();
                    }
                  }
                })
            .setCredentialsProvider(FixedCredentialsProvider.create(pubsubCredentials))
            .build();
    subscriber.startAsync().awaitRunning();

    // Wait until we have received 3 changes.
    latch.await();
    // Stop the publisher and subscriber and wait for them to release all resources.
    eventPublisher.stopAsync().awaitTerminated();
    subscriber.stopAsync().awaitTerminated();
  }
}
