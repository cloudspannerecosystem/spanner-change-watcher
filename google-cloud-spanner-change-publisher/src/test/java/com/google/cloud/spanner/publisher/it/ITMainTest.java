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

package com.google.cloud.spanner.publisher.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.publisher.Main;
import com.google.cloud.spanner.publisher.it.PubsubTestHelper.ITPubsubEnv;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper;
import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ITMainTest {
  private static final Logger logger =
      Logger.getLogger(ITSpannerDatabaseChangeEventPublisherTest.class.getName());
  private static final ITPubsubEnv env = new ITPubsubEnv();
  private static final String[] tables = new String[] {"NUMBERS1", "NUMBERS2"};
  private static final List<String> topics = new ArrayList<>();
  private static final List<String> subscriptions = new ArrayList<>();
  private static Database database;
  private static Subscriber[] subscribers;
  private static List<List<PubsubMessage>> receivedMessages;
  private static CountDownLatch receivedMessagesCount = new CountDownLatch(0);

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    database =
        env.createTestDb(
            Arrays.asList(
                "CREATE TABLE NUMBERS1 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                "CREATE TABLE NUMBERS2 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)"));
    logger.info(String.format("Created database %s", database.getId().toString()));

    receivedMessages = new ArrayList<>(tables.length);
    subscribers = new Subscriber[tables.length];
    int i = 0;
    for (String table : tables) {
      receivedMessages.add(Collections.synchronizedList(new ArrayList<PubsubMessage>()));

      String topic =
          String.format(
              "projects/%s/topics/spanner-update-%s-%s",
              PubsubTestHelper.getPubsubProjectId(), database.getId().getDatabase(), table);
      env.topicAdminClient.createTopic(topic);
      topics.add(topic);
      logger.info(String.format("Created topic for table %s", table));

      String subscription =
          String.format(
              "projects/%s/subscriptions/spanner-update-%s-%s",
              PubsubTestHelper.getPubsubProjectId(), database.getId().getDatabase(), table);
      env.subAdminClient.createSubscription(
          subscription, topic, PushConfig.getDefaultInstance(), 10);
      logger.info(String.format("Created subscription %s", subscription));

      final int index = i;
      subscribers[i] =
          Subscriber.newBuilder(
                  subscription,
                  new MessageReceiver() {
                    @Override
                    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                      logger.info(String.format("Received message %s", message.toString()));
                      receivedMessages.get(index).add(message);
                      receivedMessagesCount.countDown();
                      consumer.ack();
                    }
                  })
              .setCredentialsProvider(
                  FixedCredentialsProvider.create(PubsubTestHelper.getPubsubCredentials()))
              .build();
      subscribers[i].startAsync().awaitRunning();
      i++;
    }
  }

  @AfterClass
  public static void teardown() {
    SpannerTestHelper.teardownSpanner(env);
    for (Subscriber subscriber : subscribers) {
      subscriber.stopAsync();
    }

    for (String subscription : subscriptions) {
      env.subAdminClient.deleteSubscription(subscription);
      logger.info(String.format("Dropped test subscription %s", subscription));
    }
    for (String topic : topics) {
      env.topicAdminClient.deleteTopic(topic);
      logger.info(String.format("Dropped test topic %s", topic));
    }
  }

  @Test
  public void testMain() throws Throwable {
    // Set the needed properties as system properties.
    System.setProperty("scep.spanner.project", SpannerTestHelper.getSpannerProjectId());
    System.setProperty(
        "scep.spanner.credentials",
        MoreObjects.firstNonNull(System.getProperty("spanner.credentials"), ""));
    System.setProperty("scep.spanner.instance", database.getId().getInstanceId().getInstance());
    System.setProperty("scep.spanner.database", database.getId().getDatabase());
    System.setProperty("scep.spanner.allTables", "true");
    // TODO: Set commit timestamp repository properties.
    System.setProperty("scep.spanner.pollInterval", "PT0.050S");

    System.setProperty("scep.pubsub.project", PubsubTestHelper.getPubsubProjectId());
    System.setProperty(
        "scep.pubsub.credentials",
        MoreObjects.firstNonNull(System.getProperty("pubsub.credentials"), ""));
    System.setProperty("scep.pubsub.topicNameFormat", "spanner-update-%database%-%table%");

    // Start an event publisher using the Main.main() method.
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(
        new Runnable() {
          @Override
          public void run() {
            try {
              Main.main(new String[] {});
            } catch (IOException e) {
            }
          }
        });
    Main.getPublisher().awaitRunning();

    Spanner spanner = env.getSpanner();
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    receivedMessagesCount = new CountDownLatch(4);
    client.writeAtLeastOnce(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("NUMBERS1")
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("ONE")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertOrUpdateBuilder("NUMBERS2")
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("TWO")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertOrUpdateBuilder("NUMBERS1")
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("THREE")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertOrUpdateBuilder("NUMBERS2")
                .set("ID")
                .to(4L)
                .set("NAME")
                .to("FOUR")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build()));
    receivedMessagesCount.await(10L, TimeUnit.SECONDS);
    assertThat(receivedMessages.get(0)).hasSize(2);
    assertThat(receivedMessages.get(1)).hasSize(2);
    Main.getPublisher().stopAsync().awaitTerminated();
  }
}
