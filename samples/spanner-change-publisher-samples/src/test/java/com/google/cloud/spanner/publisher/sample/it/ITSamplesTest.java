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

package com.google.cloud.spanner.publisher.sample.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher;
import com.google.cloud.spanner.publisher.SpannerToAvroFactory;
import com.google.cloud.spanner.publisher.SpannerToAvroFactory.SpannerToAvro;
import com.google.cloud.spanner.publisher.SpannerToJsonFactory;
import com.google.cloud.spanner.publisher.SpannerToJsonFactory.SpannerToJson;
import com.google.cloud.spanner.publisher.it.PubsubTestHelper;
import com.google.cloud.spanner.publisher.it.PubsubTestHelper.ITPubsubEnv;
import com.google.cloud.spanner.publisher.sample.Samples;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for the spanner-change-watcher samples. */
@RunWith(JUnit4.class)
public class ITSamplesTest {
  private static final Logger logger = Logger.getLogger(ITSamplesTest.class.getName());
  private static final ITPubsubEnv env = new ITPubsubEnv();
  private static DatabaseId databaseId;
  private static Database database;
  private static DatabaseClient client;
  private static ExecutorService executor = Executors.newFixedThreadPool(1);
  private CountDownLatch systemExitLatch;
  private Future<String> currentSample;

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    database =
        env.createTestDb(
            ImmutableList.of(
                "CREATE TABLE NUMBERS1 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                "CREATE TABLE NUMBERS2 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                "CREATE TABLE NUMBERS_WITHOUT_COMMIT_TIMESTAMP (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP) PRIMARY KEY (ID)"));
    databaseId = database.getId();
    client = env.getSpanner().getDatabaseClient(databaseId);
    logger.info(String.format("Created database %s", database.getId().toString()));

    PubsubTestHelper.createTestTopic(env);
    PubsubTestHelper.createTestSubscription(env);
  }

  @AfterClass
  public static void teardown() {
    SpannerTestHelper.teardownSpanner(env);
    PubsubTestHelper.deleteTestSubscription(env);
    PubsubTestHelper.deleteTestTopic(env);
    PubsubTestHelper.teardownPubsub(env);
    executor.shutdown();
  }

  @Before
  public void createSystemExitLatch() {
    systemExitLatch = new CountDownLatch(1);
  }

  @After
  public void deleteRows() {
    if (currentSample != null && !(currentSample.isDone() || currentSample.isCancelled())) {
      currentSample.cancel(true);
    }
    client.write(
        ImmutableList.of(
            Mutation.delete("NUMBERS1", KeySet.all()),
            Mutation.delete("NUMBERS2", KeySet.all()),
            Mutation.delete("NUMBERS_WITHOUT_COMMIT_TIMESTAMP", KeySet.all())));
  }

  private interface SampleRunnable {
    public void run() throws InterruptedException, IOException;
  }

  private final class TestSecurityManager extends SecurityManager {
    @Override
    public void checkExit(int status) {
      if (status == 1) {
        systemExitLatch.countDown();
        if (currentSample != null) {
          currentSample.cancel(true);
        }
        throw new SecurityException("Do not exit from test");
      }
    }

    @Override
    public void checkPermission(Permission perm) {}
  }

  private Future<String> runSample(SampleRunnable example, CountDownLatch startedLatch)
      throws InterruptedException, IOException {
    currentSample =
        executor.submit(
            new Callable<String>() {
              @Override
              public String call() throws Exception {
                PrintStream stdOut = System.out;
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                PrintStream out =
                    new PrintStream(bout, true) {
                      @Override
                      public void write(byte buf[], int off, int len) {
                        super.write(buf, off, len);
                        if (bout.toString().contains("Change publisher started")) {
                          startedLatch.countDown();
                        }
                      }
                    };
                System.setOut(out);
                try {
                  example.run();
                  return bout.toString();
                } catch (Throwable t) {
                  logger.log(Level.SEVERE, "Failed to run sample", t);
                  throw t;
                } finally {
                  System.setOut(stdOut);
                }
              }
            });
    return currentSample;
  }

  static final String[] NUMBER_NAMES = new String[] {"ONE", "TWO", "THREE"};

  Iterable<Mutation> insertOrUpdateNumbers(String table, int begin, int end, Timestamp ts) {
    ImmutableList.Builder<Mutation> builder =
        ImmutableList.builderWithExpectedSize(NUMBER_NAMES.length);
    for (int i = begin; i < end; i++) {
      builder.add(
          Mutation.newInsertOrUpdateBuilder(table)
              .set("ID")
              .to(Long.valueOf(i + 1))
              .set("NAME")
              .to(NUMBER_NAMES[i])
              .set("LAST_MODIFIED")
              .to(ts)
              .build());
    }
    return builder.build();
  }

  Iterable<Struct> numberRows(Timestamp commitTs, int begin, int end) {
    ImmutableList.Builder<Struct> builder =
        ImmutableList.builderWithExpectedSize(NUMBER_NAMES.length);
    for (int i = begin; i < end; i++) {
      builder.add(
          Struct.newBuilder()
              .set("ID")
              .to(Long.valueOf(i + 1))
              .set("NAME")
              .to(NUMBER_NAMES[i])
              .set("LAST_MODIFIED")
              .to(commitTs)
              .build());
    }
    return builder.build();
  }

  @Test
  public void testPublishChangesFromSingleTableExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Samples samples =
        new Samples(
            SpannerTestHelper.getSpannerProjectId(),
            SpannerTestHelper.getSpannerCredentials(),
            PubsubTestHelper.getPubsubProjectId(),
            PubsubTestHelper.getPubsubCredentials());
    Future<String> out =
        runSample(
            () -> {
              samples.publishChangesFromSingleTableExample(
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "NUMBERS1",
                  env.topicId);
            },
            latch);
    assertThat(latch.await(120L, TimeUnit.SECONDS)).isTrue();

    final List<ByteString> receivedRows = Collections.synchronizedList(new ArrayList<>(3));
    final CountDownLatch receivedMessagesLatch = new CountDownLatch(3);
    Subscriber subscriber =
        Subscriber.newBuilder(
                String.format(
                    "projects/%s/subscriptions/%s",
                    PubsubTestHelper.getPubsubProjectId(), env.subscriptionId),
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    consumer.ack();
                    receivedRows.add(message.getData());
                    receivedMessagesLatch.countDown();
                  }
                })
            .setCredentialsProvider(
                FixedCredentialsProvider.create(PubsubTestHelper.getPubsubCredentials()))
            .build();
    subscriber.startAsync().awaitRunning();

    Timestamp commitTs =
        client.write(
            insertOrUpdateNumbers("NUMBERS1", 0, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(30L, TimeUnit.SECONDS);
    assertThat(receivedMessagesLatch.await(30L, TimeUnit.SECONDS)).isTrue();

    TableId table = TableId.of(databaseId, "NUMBERS1");
    SpannerToAvro converter = SpannerToAvroFactory.INSTANCE.create(client, table);
    for (Struct row : numberRows(commitTs, 0, NUMBER_NAMES.length)) {
      assertThat(res)
          .contains(
              String.format("Published change for table %s at %s%n", table, commitTs.toString()));
      ByteString record = converter.convert(row);
      assertThat(receivedRows).contains(record);
    }
    subscriber.stopAsync().awaitTerminated();
  }

  @Test
  public void testPublishChangesFromAllTablesExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Samples samples =
        new Samples(
            SpannerTestHelper.getSpannerProjectId(),
            SpannerTestHelper.getSpannerCredentials(),
            PubsubTestHelper.getPubsubProjectId(),
            PubsubTestHelper.getPubsubCredentials());
    Future<String> out =
        runSample(
            () -> {
              samples.publishChangesFromAllTablesExample(
                  databaseId.getInstanceId().getInstance(), databaseId.getDatabase(), env.topicId);
            },
            latch);
    assertThat(latch.await(60L, TimeUnit.SECONDS)).isTrue();

    final List<ByteString> receivedRows = new ArrayList<>(3);
    final CountDownLatch receivedMessagesLatch = new CountDownLatch(3);
    Subscriber subscriber =
        Subscriber.newBuilder(
                String.format(
                    "projects/%s/subscriptions/%s",
                    PubsubTestHelper.getPubsubProjectId(), env.subscriptionId),
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    consumer.ack();
                    receivedRows.add(message.getData());
                    receivedMessagesLatch.countDown();
                  }
                })
            .setCredentialsProvider(
                FixedCredentialsProvider.create(PubsubTestHelper.getPubsubCredentials()))
            .build();
    subscriber.startAsync().awaitRunning();

    // First write to the table without a commit timestamp. These changes should not be picked up.
    client.write(
        insertOrUpdateNumbers(
            "NUMBERS_WITHOUT_COMMIT_TIMESTAMP", 0, NUMBER_NAMES.length, Timestamp.now()));
    Timestamp commitTs1 =
        client.write(insertOrUpdateNumbers("NUMBERS1", 0, 1, Value.COMMIT_TIMESTAMP));
    Timestamp commitTs2 =
        client.write(
            insertOrUpdateNumbers("NUMBERS2", 1, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(30L, TimeUnit.SECONDS);
    assertThat(receivedMessagesLatch.await(30L, TimeUnit.SECONDS)).isTrue();

    TableId table1 = TableId.of(databaseId, "NUMBERS1");
    SpannerToAvro converter1 = SpannerToAvroFactory.INSTANCE.create(client, table1);
    for (Struct row : numberRows(commitTs1, 0, 1)) {
      assertThat(res)
          .contains(
              String.format("Published change for table %s at %s%n", table1, commitTs1.toString()));
      ByteString record = converter1.convert(row);
      assertThat(receivedRows).contains(record);
    }
    TableId table2 = TableId.of(databaseId, "NUMBERS2");
    SpannerToAvro converter2 = SpannerToAvroFactory.INSTANCE.create(client, table2);
    for (Struct row : numberRows(commitTs2, 1, NUMBER_NAMES.length)) {
      assertThat(res)
          .contains(
              String.format("Published change for table %s at %s%n", table2, commitTs2.toString()));
      ByteString record = converter2.convert(row);
      assertThat(receivedRows).contains(record);
    }
    assertThat(res).doesNotContain("NUMBERS_WITHOUT_COMMIT_TIMESTAMP");
    subscriber.stopAsync().awaitTerminated();
  }

  @Test
  public void testPublishChangesFromAllTablesToSeparateTopicsExample() throws Exception {
    // The topics will automatically be created by the publisher, but this ensures they are deleted
    // after the test has run.
    // topicFormat = "change-log-%database%-%table%";
    String topic1 = String.format("change-log-%s-NUMBERS1", databaseId.getDatabase());
    String topic2 = String.format("change-log-%s-NUMBERS2", databaseId.getDatabase());
    env.registerTestTopic(topic1);
    env.registerTestTopic(topic2);

    CountDownLatch latch = new CountDownLatch(1);
    Samples samples =
        new Samples(
            SpannerTestHelper.getSpannerProjectId(),
            SpannerTestHelper.getSpannerCredentials(),
            PubsubTestHelper.getPubsubProjectId(),
            PubsubTestHelper.getPubsubCredentials());
    Future<String> out =
        runSample(
            () -> {
              samples.publishChangesFromAllTablesToSeparateTopicsExample(
                  databaseId.getInstanceId().getInstance(), databaseId.getDatabase());
            },
            latch);
    assertThat(latch.await(60L, TimeUnit.SECONDS)).isTrue();

    String subscription1 = String.format("sub-change-log-%s-NUMBERS1", databaseId.getDatabase());
    String subscription2 = String.format("sub-change-log-%s-NUMBERS2", databaseId.getDatabase());
    // Create a couple of subscriptions to get the published changes.
    env.createTestSubscription(topic1, subscription1);
    env.createTestSubscription(topic2, subscription2);

    final CountDownLatch receivedMessagesLatch = new CountDownLatch(3);
    final List<ByteString> receivedRows1 = new ArrayList<>(3);
    final List<ByteString> receivedRows2 = new ArrayList<>(3);
    Subscriber subscriber1 =
        Subscriber.newBuilder(
                String.format(
                    "projects/%s/subscriptions/%s",
                    PubsubTestHelper.getPubsubProjectId(), subscription1),
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    consumer.ack();
                    receivedRows1.add(message.getData());
                    receivedMessagesLatch.countDown();
                  }
                })
            .setCredentialsProvider(
                FixedCredentialsProvider.create(PubsubTestHelper.getPubsubCredentials()))
            .build();
    subscriber1.startAsync().awaitRunning();
    Subscriber subscriber2 =
        Subscriber.newBuilder(
                String.format(
                    "projects/%s/subscriptions/%s",
                    PubsubTestHelper.getPubsubProjectId(), subscription2),
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    consumer.ack();
                    receivedRows2.add(message.getData());
                    receivedMessagesLatch.countDown();
                  }
                })
            .setCredentialsProvider(
                FixedCredentialsProvider.create(PubsubTestHelper.getPubsubCredentials()))
            .build();
    subscriber2.startAsync().awaitRunning();

    Timestamp commitTs1 =
        client.write(insertOrUpdateNumbers("NUMBERS1", 0, 1, Value.COMMIT_TIMESTAMP));
    Timestamp commitTs2 =
        client.write(
            insertOrUpdateNumbers("NUMBERS2", 1, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(30L, TimeUnit.SECONDS);
    assertThat(receivedMessagesLatch.await(30L, TimeUnit.SECONDS)).isTrue();

    TableId table1 = TableId.of(databaseId, "NUMBERS1");
    SpannerToAvro converter1 = SpannerToAvroFactory.INSTANCE.create(client, table1);
    for (Struct row : numberRows(commitTs1, 0, 1)) {
      assertThat(res)
          .contains(
              String.format("Published change for table %s at %s%n", table1, commitTs1.toString()));
      ByteString record = converter1.convert(row);
      assertThat(receivedRows1).contains(record);
      assertThat(receivedRows2).doesNotContain(record);
    }
    TableId table2 = TableId.of(databaseId, "NUMBERS2");
    SpannerToAvro converter2 = SpannerToAvroFactory.INSTANCE.create(client, table2);
    for (Struct row : numberRows(commitTs2, 1, NUMBER_NAMES.length)) {
      assertThat(res)
          .contains(
              String.format("Published change for table %s at %s%n", table2, commitTs2.toString()));
      ByteString record = converter2.convert(row);
      assertThat(receivedRows1).doesNotContain(record);
      assertThat(receivedRows2).contains(record);
    }
    subscriber1.stopAsync().awaitTerminated();
    subscriber2.stopAsync().awaitTerminated();
  }

  @Test
  public void testErrorHandling() throws Exception {
    String topic = "test-topic-" + new Random().nextLong();
    env.createTestTopic(topic);

    SecurityManager currentSecurityManager = System.getSecurityManager();
    try {
      System.setSecurityManager(new TestSecurityManager());
      CountDownLatch latch = new CountDownLatch(1);
      Samples samples =
          new Samples(
              SpannerTestHelper.getSpannerProjectId(),
              SpannerTestHelper.getSpannerCredentials(),
              PubsubTestHelper.getPubsubProjectId(),
              PubsubTestHelper.getPubsubCredentials());
      runSample(
          () -> {
            samples.errorHandling(
                databaseId.getInstanceId().getInstance(), databaseId.getDatabase(), topic);
          },
          latch);
      assertThat(latch.await(60L, TimeUnit.SECONDS)).isTrue();

      Logger logger = Logger.getLogger(SpannerTableChangeEventPublisher.class.getName());
      Level level = logger.getLevel();
      PrintStream stdErr = System.err;
      try {
        logger.setLevel(Level.OFF);
        ByteArrayOutputStream berr = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(berr);
        System.setErr(err);
        // Delete the topic that we are publishing to. This should trigger a failure when a change
        // is detected.
        env.topicAdminClient.deleteTopic(
            TopicName.format(PubsubTestHelper.getPubsubProjectId(), topic));
        client.write(
            insertOrUpdateNumbers("NUMBERS1", 0, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
        assertThat(systemExitLatch.await(60L, TimeUnit.SECONDS)).isTrue();
        String errors = berr.toString();
        assertThat(errors).contains("Database change publisher failed.");
        assertThat(errors).contains("State before failure: RUNNING");
      } finally {
        logger.setLevel(level);
        System.setErr(stdErr);
      }
    } finally {
      System.setSecurityManager(currentSecurityManager);
    }
  }

  @Test
  public void testSubscribeToChanges() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Samples samples =
        new Samples(
            SpannerTestHelper.getSpannerProjectId(),
            SpannerTestHelper.getSpannerCredentials(),
            PubsubTestHelper.getPubsubProjectId(),
            PubsubTestHelper.getPubsubCredentials());
    Future<String> out =
        runSample(
            () -> {
              samples.subscribeToChanges(
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  env.topicId,
                  env.subscriptionId);
            },
            latch);
    assertThat(latch.await(60L, TimeUnit.SECONDS)).isTrue();

    // First write to the table without a commit timestamp. These changes should not be picked up.
    client.write(
        insertOrUpdateNumbers(
            "NUMBERS_WITHOUT_COMMIT_TIMESTAMP", 0, NUMBER_NAMES.length, Timestamp.now()));
    Timestamp commitTs1 =
        client.write(insertOrUpdateNumbers("NUMBERS1", 0, 1, Value.COMMIT_TIMESTAMP));
    Timestamp commitTs2 =
        client.write(
            insertOrUpdateNumbers("NUMBERS2", 1, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(60L, TimeUnit.SECONDS);

    TableId table1 = TableId.of(databaseId, "NUMBERS1");
    SpannerToAvro converter1 = SpannerToAvroFactory.INSTANCE.create(client, table1);
    for (Struct row : numberRows(commitTs1, 0, 1)) {
      assertThat(res).contains(String.format("Table: %s%n", table1));
      assertThat(res).contains(String.format("Commit timestamp: %s%n", commitTs1));
      ByteString record = converter1.convert(row);
      assertThat(res).contains(String.format("Data: %s%n", converter1.decodeRecord(record)));
    }
    TableId table2 = TableId.of(databaseId, "NUMBERS2");
    SpannerToAvro converter2 = SpannerToAvroFactory.INSTANCE.create(client, table2);
    for (Struct row : numberRows(commitTs2, 1, NUMBER_NAMES.length)) {
      assertThat(res).contains(String.format("Table: %s%n", table2));
      assertThat(res).contains(String.format("Commit timestamp: %s%n", commitTs2));
      ByteString record = converter2.convert(row);
      assertThat(res).contains(String.format("Data: %s%n", converter2.decodeRecord(record)));
    }
    assertThat(res).doesNotContain("NUMBERS_WITHOUT_COMMIT_TIMESTAMP");
  }

  @Test
  public void testSubscribeToChangesAsJson() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Samples samples =
        new Samples(
            SpannerTestHelper.getSpannerProjectId(),
            SpannerTestHelper.getSpannerCredentials(),
            PubsubTestHelper.getPubsubProjectId(),
            PubsubTestHelper.getPubsubCredentials());
    Future<String> out =
        runSample(
            () -> {
              samples.subscribeToChangesAsJson(
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  env.topicId,
                  env.subscriptionId);
            },
            latch);
    assertThat(latch.await(60L, TimeUnit.SECONDS)).isTrue();

    // First write to the table without a commit timestamp. These changes should not be picked up.
    client.write(
        insertOrUpdateNumbers(
            "NUMBERS_WITHOUT_COMMIT_TIMESTAMP", 0, NUMBER_NAMES.length, Timestamp.now()));
    Timestamp commitTs1 =
        client.write(insertOrUpdateNumbers("NUMBERS1", 0, 1, Value.COMMIT_TIMESTAMP));
    Timestamp commitTs2 =
        client.write(
            insertOrUpdateNumbers("NUMBERS2", 1, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(60L, TimeUnit.SECONDS);

    TableId table1 = TableId.of(databaseId, "NUMBERS1");
    SpannerToJson converter1 = SpannerToJsonFactory.INSTANCE.create(client, table1);
    for (Struct row : numberRows(commitTs1, 0, 1)) {
      assertThat(res).contains(String.format("Table: %s%n", table1));
      assertThat(res).contains(String.format("Commit timestamp: %s%n", commitTs1));
      ByteString record = converter1.convert(row);
      JsonElement json = JsonParser.parseString(record.toStringUtf8());
      assertThat(res).contains(String.format("Data: %s%n", json));
    }
    TableId table2 = TableId.of(databaseId, "NUMBERS2");
    SpannerToJson converter2 = SpannerToJsonFactory.INSTANCE.create(client, table2);
    for (Struct row : numberRows(commitTs2, 1, NUMBER_NAMES.length)) {
      assertThat(res).contains(String.format("Table: %s%n", table2));
      assertThat(res).contains(String.format("Commit timestamp: %s%n", commitTs2));
      ByteString record = converter2.convert(row);
      JsonElement json = JsonParser.parseString(record.toStringUtf8());
      assertThat(res).contains(String.format("Data: %s%n", json));
    }
    assertThat(res).doesNotContain("NUMBERS_WITHOUT_COMMIT_TIMESTAMP");
  }
}
