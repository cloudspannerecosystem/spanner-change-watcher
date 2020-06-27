/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.publisher.sample;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.publisher.SpannerDatabaseChangeEventPublisher;
import com.google.cloud.spanner.publisher.SpannerToJsonFactory;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;

public class SimpleChangePublisherSample {

  public static void main(String[] args) throws InterruptedException, IOException {
    if (args.length != 4) {
      System.out.println(
          String.format(
              "Invalid arguments. Usage: java %s <instanceId> <databaseId> <topicId> <subscriptionId>",
              SimpleChangePublisherSample.class.getName()));
      System.exit(1);
    }
    String instance = args[0];
    String database = args[1];
    String topic = args[2];
    String subscription = args[3];
    SpannerOptions options = SpannerOptions.newBuilder().build();
    String project = options.getProjectId();
    // Create a connection to a Spanner database.
    System.out.println(
        String.format(
            "Connecting to projects/%s/instances/%s/databases/%s...",
            options.getProjectId(), instance, database));
    Spanner spanner = options.getService();
    DatabaseId databaseId = DatabaseId.of(project, instance, database);

    System.out.println("Checking/creating sample database...");
    SampleData.createSampleDatabase(spanner, databaseId);

    // Create and start a SpannerDatabaseChangeEventPublisher.
    System.out.println("Starting change publisher...");
    SpannerDatabaseChangeEventPublisher publisher = createPublisher(spanner, databaseId, topic);

    // Create and start a Pubsub Subscriber.
    System.out.println("Checking/creating subscription...");
    createSubscriptionIfNotExists(project, topic, subscription);
    System.out.println("Creating subscriber...");
    Subscriber subscriber = createSubscriber(project, subscription);

    // Write some data to the database. This will then be published to Pubsub, sent to the
    // subscriber and then written to the console.
    System.out.println("Writing data to Cloud Spanner...");
    SampleData.writeExampleData(spanner.getDatabaseClient(databaseId));
    // Wait a little to allow all data to be written, and the callback to write the data to the
    // console.
    Thread.sleep(10_000L);
    System.out.println("Finished writing test data...");

    // Wait for the user to hit <Enter> before exiting.
    System.out.println("The Database Change Publisher is still running in the background.");
    System.out.println("You can write additional data to the database.");
    System.out.println("This will cause the data to be written to this console.");
    System.out.println("Press <Enter> to close this application.");

    System.in.read();
    System.out.println("Closing change publisher and subscriber...");
    publisher.stopAsync();
    subscriber.stopAsync();
    publisher.awaitTerminated();
    subscriber.awaitTerminated();
    System.out.println("Change publisher and subscriber closed.");
  }

  static SpannerDatabaseChangeEventPublisher createPublisher(
      Spanner spanner, DatabaseId databaseId, String topic) throws IOException {
    String project = databaseId.getInstanceId().getProject();
    // First create a change watcher for all tables in the database that have a commit timestamp
    // column.
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();
    // Then create a change publisher using the change watcher.
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    SpannerDatabaseChangeEventPublisher publisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
            .setConverterFactory(SpannerToJsonFactory.INSTANCE)
            .setTopicNameFormat(String.format("projects/%s/topics/%s", project, topic))
            .setCreateTopicsIfNotExist(true)
            .build();
    // Start the change publisher and wait until it is running.
    publisher.startAsync().awaitRunning();
    return publisher;
  }

  static Subscriber createSubscriber(String project, String subscription) throws IOException {
    // Start a subscriber.
    Subscriber subscriber =
        Subscriber.newBuilder(
                ProjectSubscriptionName.of(project, subscription),
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
                    }
                  }
                })
            .build();
    subscriber.startAsync().awaitRunning();
    return subscriber;
  }

  static void createSubscriptionIfNotExists(String project, String topic, String subscription)
      throws IOException {
    try (SubscriptionAdminClient client =
        SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder().build())) {
      ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(project, subscription);
      try {
        client.getSubscription(subscriptionName);
      } catch (NotFoundException e) {
        TopicName topicName = TopicName.of(project, topic);
        client.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 60);
      }
    }
  }
}
