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
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher;
import com.google.cloud.spanner.publisher.it.PubsubTestHelper.ITPubsubEnv;
import com.google.cloud.spanner.watcher.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class ITSpannerTableChangeEventPublisherTest {
  private static final Logger logger =
      Logger.getLogger(ITSpannerTableChangeEventPublisherTest.class.getName());
  private static final ITPubsubEnv env = new ITPubsubEnv();
  private static Database database;
  private static Subscriber subscriber;
  private static List<PubsubMessage> receivedMessages =
      Collections.synchronizedList(new ArrayList<PubsubMessage>());
  private static CountDownLatch receivedMessagesCount = new CountDownLatch(0);

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    PubsubTestHelper.createTestTopic(env);
    PubsubTestHelper.createTestSubscription(env);
    database =
        env.createTestDb(
            Collections.singleton(
                "CREATE TABLE NUMBERS (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)"));
    logger.info(String.format("Created database %s", database.getId().toString()));
    subscriber =
        Subscriber.newBuilder(
                String.format(
                    "projects/%s/subscriptions/%s",
                    PubsubTestHelper.PUBSUB_PROJECT_ID, env.subscriptionId),
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    logger.info(String.format("Received message %s", message.toString()));
                    receivedMessages.add(message);
                    receivedMessagesCount.countDown();
                    consumer.ack();
                  }
                })
            .setCredentialsProvider(
                FixedCredentialsProvider.create(PubsubTestHelper.getPubSubCredentials()))
            .build();
    subscriber.startAsync().awaitRunning();
  }

  @AfterClass
  public static void teardown() {
    SpannerTestHelper.teardownSpanner(env);
    subscriber.stopAsync();
    PubsubTestHelper.deleteTestSubscription(env);
    PubsubTestHelper.deleteTestTopic(env);
  }

  @Test
  public void testEventPublisher() throws Exception {
    Spanner spanner = env.getSpanner();
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    SpannerTableChangeWatcher capturer =
        SpannerTableTailer.newBuilder(spanner, TableId.of(database.getId(), "NUMBERS"))
            .setPollInterval(Duration.ofMillis(50L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, database.getId())
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .setCreateTableIfNotExists()
                    .build())
            .build();
    SpannerTableChangeEventPublisher eventPublisher =
        SpannerTableChangeEventPublisher.newBuilder(capturer, client)
            .setTopicName(
                String.format(
                    "projects/%s/topics/%s", PubsubTestHelper.PUBSUB_PROJECT_ID, env.topicId))
            .setCredentials(PubsubTestHelper.getPubSubCredentials())
            .build();
    eventPublisher.start();

    receivedMessagesCount = new CountDownLatch(3);
    client.writeAtLeastOnce(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("NUMBERS")
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("ONE")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertOrUpdateBuilder("NUMBERS")
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("TWO")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertOrUpdateBuilder("NUMBERS")
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("THREE")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build()));
    receivedMessagesCount.await(10L, TimeUnit.SECONDS);
    assertThat(receivedMessages).hasSize(3);
    eventPublisher.stop();
    assertThat(eventPublisher.awaitTermination(10L, TimeUnit.SECONDS)).isTrue();
  }
}
