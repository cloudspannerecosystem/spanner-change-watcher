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

package com.google.cloud.spanner.watcher.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.AsyncRunner.AsyncWork;
import com.google.cloud.spanner.AsyncTransactionManager.TransactionContextFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.DatabaseClientWithChangeSets;
import com.google.cloud.spanner.watcher.DatabaseClientWithChangeSets.AsyncRunnerWithChangeSet;
import com.google.cloud.spanner.watcher.DatabaseClientWithChangeSets.AsyncTransactionManagerWithChangeSet;
import com.google.cloud.spanner.watcher.DatabaseClientWithChangeSets.TransactionManagerWithChangeSet;
import com.google.cloud.spanner.watcher.DatabaseClientWithChangeSets.TransactionRunnerWithChangeSet;
import com.google.cloud.spanner.watcher.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.watcher.SpannerTableChangeSetPoller;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper.ITSpannerEnv;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class ITSpannerTableChangeSetPollerTest {
  private static final Logger logger =
      Logger.getLogger(ITSpannerTableChangeSetPollerTest.class.getName());
  private static final ITSpannerEnv env = new ITSpannerEnv();
  private static Database database;
  private final Queue<Struct> receivedChanges = new ConcurrentLinkedQueue<>();
  private volatile Timestamp lastCommitTimestamp;
  private volatile CountDownLatch latch = new CountDownLatch(0);

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    database =
        env.createTestDb(
            ImmutableList.of(
                "CREATE TABLE NUMBERS (ID INT64 NOT NULL, NAME STRING(100), CHANGE_SET_ID STRING(MAX)) PRIMARY KEY (ID)",
                "CREATE TABLE CHANGE_SETS (CHANGE_SET_ID STRING(MAX), COMMIT_TIMESTAMP TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (CHANGE_SET_ID)",
                "CREATE INDEX IDX_NUMBERS_CHANGE_SET ON NUMBERS (CHANGE_SET_ID)"));
    logger.info(String.format("Created database %s", database.getId().toString()));
  }

  @AfterClass
  public static void teardown() {
    SpannerTestHelper.teardownSpanner(env);
  }

  @Before
  public void deleteRowsInNumbers() {
    Spanner spanner = env.getSpanner();
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    client.writeAtLeastOnce(ImmutableList.of(Mutation.delete("NUMBERS", KeySet.all())));
  }

  @Test
  public void testSpannerChangeSetPoller() throws Exception {
    Spanner spanner = env.getSpanner();
    SpannerTableChangeSetPoller poller =
        SpannerTableChangeSetPoller.newBuilder(spanner, TableId.of(database.getId(), "NUMBERS"))
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, database.getId())
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    poller.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            logger.info(
                String.format(
                    "Received changed for table %s: %s", table, row.asStruct().toString()));
            receivedChanges.add(row.asStruct());
            lastCommitTimestamp = commitTimestamp;
            latch.countDown();
          }
        });
    poller.startAsync().awaitRunning();

    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    latch = new CountDownLatch(3);
    String changeSetId = UUID.randomUUID().toString();
    Timestamp commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("CHANGE_SETS")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .set("COMMIT_TIMESTAMP")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("ONE")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(2L)
                    .set("NAME")
                    .to("TWO")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(3L)
                    .set("NAME")
                    .to("THREE")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build()));

    List<Struct> inserts = drainChanges();
    assertThat(inserts).hasSize(3);
    assertThat(inserts)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("ONE")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("TWO")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("THREE")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs);

    latch = new CountDownLatch(2);
    changeSetId = UUID.randomUUID().toString();
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("CHANGE_SETS")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .set("COMMIT_TIMESTAMP")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(4L)
                    .set("NAME")
                    .to("FOUR")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("FIVE")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build()));

    inserts = drainChanges();
    assertThat(inserts).hasSize(2);
    assertThat(inserts)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(4L)
                .set("NAME")
                .to("FOUR")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("FIVE")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs);

    latch = new CountDownLatch(2);
    changeSetId = UUID.randomUUID().toString();
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("CHANGE_SETS")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .set("COMMIT_TIMESTAMP")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("one")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build(),
                Mutation.newUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("five")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build()));

    List<Struct> updates = drainChanges();
    assertThat(updates).hasSize(2);
    assertThat(updates)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("one")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("five")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs);

    // Verify that deletes are not picked up by the poller.
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.delete("NUMBERS", Key.of(2L)), Mutation.delete("NUMBERS", Key.of(3L))));
    Thread.sleep(500L);
    assertThat(receivedChanges).isEmpty();
    poller.stopAsync().awaitTerminated();
  }

  @Test
  public void testSpannerChangeSetPollerUsingChangeSetDatabaseClient() throws Exception {
    Spanner spanner = env.getSpanner();
    SpannerTableChangeSetPoller poller =
        SpannerTableChangeSetPoller.newBuilder(spanner, TableId.of(database.getId(), "NUMBERS"))
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, database.getId())
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    poller.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            logger.info(
                String.format(
                    "Received changed for table %s: %s", table, row.asStruct().toString()));
            receivedChanges.add(row.asStruct());
            lastCommitTimestamp = commitTimestamp;
            latch.countDown();
          }
        });
    poller.startAsync().awaitRunning();

    DatabaseClientWithChangeSets client =
        DatabaseClientWithChangeSets.of(spanner.getDatabaseClient(database.getId()));
    latch = new CountDownLatch(3);
    String changeSetId = client.newChangeSetId();
    Timestamp commitTs =
        client.writeAtLeastOnce(
            changeSetId,
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("ONE")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(2L)
                    .set("NAME")
                    .to("TWO")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(3L)
                    .set("NAME")
                    .to("THREE")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build()));

    List<Struct> inserts = drainChanges();
    assertThat(inserts).hasSize(3);
    assertThat(inserts)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("ONE")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("TWO")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("THREE")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs);

    latch = new CountDownLatch(2);
    changeSetId = client.newChangeSetId();
    commitTs =
        client.writeAtLeastOnce(
            changeSetId,
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(4L)
                    .set("NAME")
                    .to("FOUR")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("FIVE")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build()));

    inserts = drainChanges();
    assertThat(inserts).hasSize(2);
    assertThat(inserts)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(4L)
                .set("NAME")
                .to("FOUR")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("FIVE")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs);

    latch = new CountDownLatch(2);
    changeSetId = client.newChangeSetId();
    commitTs =
        client.writeAtLeastOnce(
            changeSetId,
            Arrays.asList(
                Mutation.newUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("one")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build(),
                Mutation.newUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("five")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .build()));

    List<Struct> updates = drainChanges();
    assertThat(updates).hasSize(2);
    assertThat(updates)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("one")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("five")
                .set("CHANGE_SET_ID")
                .to(changeSetId)
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs);

    // Write mutations using a read/write transaction.
    testMutationsWithTransactionRunner(client);
    // Write changes using DML.
    testDmlWithTransactionRunner(client);

    // Write mutations using a transaction manager.
    testMutationsWithTransactionManager(client);
    // Write changes using DML through a transaction manager.
    testDmlWithTransactionManager(client);

    // Write mutations using an async read/write transaction runner.
    testMutationsWithAsyncRunner(client);
    // Write changes using async DML.
    testDmlWithAsyncRunner(client);

    // Write mutations using an async transaction manager.
    testMutationsWithAsyncTransactionManager(client);
    // Write changes using DML through an async transaction manager.
    testDmlWithAsyncTransactionManager(client);

    // Verify that deletes are not picked up by the poller.
    commitTs = client.writeAtLeastOnce(Arrays.asList(Mutation.delete("NUMBERS", Key.of(2L))));
    Thread.sleep(500L);
    assertThat(receivedChanges).isEmpty();

    poller.stopAsync().awaitTerminated();
  }

  private void testMutationsWithTransactionRunner(DatabaseClientWithChangeSets client)
      throws Exception {
    latch = new CountDownLatch(2);
    TransactionRunnerWithChangeSet runner = client.readWriteTransaction();
    runner.run(
        new TransactionCallable<Void>() {
          @Override
          public Void run(TransactionContext transaction) throws Exception {
            transaction.buffer(
                Arrays.asList(
                    Mutation.newUpdateBuilder("NUMBERS")
                        .set("ID")
                        .to(1L)
                        .set("NAME")
                        .to("En")
                        .set("CHANGE_SET_ID")
                        .to(runner.getChangeSetId())
                        .build(),
                    Mutation.newUpdateBuilder("NUMBERS")
                        .set("ID")
                        .to(5L)
                        .set("NAME")
                        .to("Fem")
                        .set("CHANGE_SET_ID")
                        .to(runner.getChangeSetId())
                        .build()));
            return null;
          }
        });
    Timestamp commitTs = runner.getCommitTimestamp();

    List<Struct> updates = drainChanges();
    assertThat(updates).hasSize(2);
    assertThat(updates)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("En")
                .set("CHANGE_SET_ID")
                .to(runner.getChangeSetId())
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("Fem")
                .set("CHANGE_SET_ID")
                .to(runner.getChangeSetId())
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs);
  }

  private void testDmlWithTransactionRunner(DatabaseClientWithChangeSets client) throws Exception {
    latch = new CountDownLatch(2);
    TransactionRunnerWithChangeSet runner = client.readWriteTransaction();
    runner.run(
        new TransactionCallable<Void>() {
          @Override
          public Void run(TransactionContext transaction) throws Exception {
            String sql = "UPDATE NUMBERS SET NAME=@name, CHANGE_SET_ID=@changeSet WHERE ID=@id";
            transaction.batchUpdate(
                Arrays.asList(
                    Statement.newBuilder(sql)
                        .bind("name")
                        .to("Tre")
                        .bind("id")
                        .to(3L)
                        .bind("changeSet")
                        .to(runner.getChangeSetId())
                        .build(),
                    Statement.newBuilder(sql)
                        .bind("name")
                        .to("Fire")
                        .bind("id")
                        .to(4L)
                        .bind("changeSet")
                        .to(runner.getChangeSetId())
                        .build()));
            return null;
          }
        });
    Timestamp commitTs = runner.getCommitTimestamp();

    List<Struct> updates = drainChanges();
    assertThat(updates).hasSize(2);
    assertThat(updates)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("Tre")
                .set("CHANGE_SET_ID")
                .to(runner.getChangeSetId())
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(4L)
                .set("NAME")
                .to("Fire")
                .set("CHANGE_SET_ID")
                .to(runner.getChangeSetId())
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs);
  }

  @SuppressWarnings("resource")
  private void testMutationsWithTransactionManager(DatabaseClientWithChangeSets client)
      throws Exception {
    latch = new CountDownLatch(2);
    try (TransactionManagerWithChangeSet manager = client.transactionManager()) {
      TransactionContext txn = manager.begin();
      while (true) {
        try {
          txn.buffer(
              Arrays.asList(
                  Mutation.newUpdateBuilder("NUMBERS")
                      .set("ID")
                      .to(1L)
                      .set("NAME")
                      .to("Uno")
                      .set("CHANGE_SET_ID")
                      .to(manager.getChangeSetId())
                      .build(),
                  Mutation.newUpdateBuilder("NUMBERS")
                      .set("ID")
                      .to(5L)
                      .set("NAME")
                      .to("Cinque")
                      .set("CHANGE_SET_ID")
                      .to(manager.getChangeSetId())
                      .build()));
          manager.commit();
          break;
        } catch (AbortedException e) {
          Thread.sleep(e.getRetryDelayInMillis() / 1000);
          txn = manager.resetForRetry();
        }
      }
      Timestamp commitTs = manager.getCommitTimestamp();

      List<Struct> updates = drainChanges();
      assertThat(updates).hasSize(2);
      assertThat(updates)
          .containsExactly(
              Struct.newBuilder()
                  .set("ID")
                  .to(1L)
                  .set("NAME")
                  .to("Uno")
                  .set("CHANGE_SET_ID")
                  .to(manager.getChangeSetId())
                  .build(),
              Struct.newBuilder()
                  .set("ID")
                  .to(5L)
                  .set("NAME")
                  .to("Cinque")
                  .set("CHANGE_SET_ID")
                  .to(manager.getChangeSetId())
                  .build());
      assertThat(lastCommitTimestamp).isEqualTo(commitTs);
    }
  }

  @SuppressWarnings("resource")
  private void testDmlWithTransactionManager(DatabaseClientWithChangeSets client) throws Exception {
    latch = new CountDownLatch(2);
    try (TransactionManagerWithChangeSet manager = client.transactionManager()) {
      TransactionContext txn = manager.begin();
      while (true) {
        try {
          String sql = "UPDATE NUMBERS SET NAME=@name, CHANGE_SET_ID=@changeSet WHERE ID=@id";
          txn.batchUpdate(
              Arrays.asList(
                  Statement.newBuilder(sql)
                      .bind("name")
                      .to("Tres")
                      .bind("id")
                      .to(3L)
                      .bind("changeSet")
                      .to(manager.getChangeSetId())
                      .build(),
                  Statement.newBuilder(sql)
                      .bind("name")
                      .to("Cuatro")
                      .bind("id")
                      .to(4L)
                      .bind("changeSet")
                      .to(manager.getChangeSetId())
                      .build()));
          manager.commit();
          break;
        } catch (AbortedException e) {
          Thread.sleep(e.getRetryDelayInMillis() / 1000);
          txn = manager.resetForRetry();
        }
      }
      Timestamp commitTs = manager.getCommitTimestamp();

      List<Struct> updates = drainChanges();
      assertThat(updates).hasSize(2);
      assertThat(updates)
          .containsExactly(
              Struct.newBuilder()
                  .set("ID")
                  .to(3L)
                  .set("NAME")
                  .to("Tres")
                  .set("CHANGE_SET_ID")
                  .to(manager.getChangeSetId())
                  .build(),
              Struct.newBuilder()
                  .set("ID")
                  .to(4L)
                  .set("NAME")
                  .to("Cuatro")
                  .set("CHANGE_SET_ID")
                  .to(manager.getChangeSetId())
                  .build());
      assertThat(lastCommitTimestamp).isEqualTo(commitTs);
    }
  }

  private void testMutationsWithAsyncRunner(DatabaseClientWithChangeSets client) throws Exception {
    latch = new CountDownLatch(2);
    ExecutorService exec = Executors.newSingleThreadExecutor();
    AsyncRunnerWithChangeSet runner = client.runAsync();
    runner.runAsync(
        new AsyncWork<Void>() {
          @Override
          public ApiFuture<Void> doWorkAsync(TransactionContext txn) {
            txn.buffer(
                Arrays.asList(
                    Mutation.newUpdateBuilder("NUMBERS")
                        .set("ID")
                        .to(1L)
                        .set("NAME")
                        .to("En")
                        .set("CHANGE_SET_ID")
                        .to(runner.getChangeSetId())
                        .build(),
                    Mutation.newUpdateBuilder("NUMBERS")
                        .set("ID")
                        .to(5L)
                        .set("NAME")
                        .to("Fem")
                        .set("CHANGE_SET_ID")
                        .to(runner.getChangeSetId())
                        .build()));
            return ApiFutures.immediateFuture(null);
          }
        },
        exec);
    ApiFuture<Timestamp> commitTs = runner.getCommitTimestamp();

    List<Struct> updates = drainChanges();
    assertThat(updates).hasSize(2);
    assertThat(updates)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("En")
                .set("CHANGE_SET_ID")
                .to(runner.getChangeSetId())
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("Fem")
                .set("CHANGE_SET_ID")
                .to(runner.getChangeSetId())
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs.get());
    exec.shutdown();
  }

  private void testDmlWithAsyncRunner(DatabaseClientWithChangeSets client) throws Exception {
    latch = new CountDownLatch(2);
    ExecutorService exec = Executors.newSingleThreadExecutor();
    AsyncRunnerWithChangeSet runner = client.runAsync();
    runner.runAsync(
        new AsyncWork<Void>() {
          @Override
          public ApiFuture<Void> doWorkAsync(TransactionContext txn) {
            String sql = "UPDATE NUMBERS SET NAME=@name, CHANGE_SET_ID=@changeSet WHERE ID=@id";
            txn.batchUpdateAsync(
                Arrays.asList(
                    Statement.newBuilder(sql)
                        .bind("name")
                        .to("Tre")
                        .bind("id")
                        .to(3L)
                        .bind("changeSet")
                        .to(runner.getChangeSetId())
                        .build(),
                    Statement.newBuilder(sql)
                        .bind("name")
                        .to("Fire")
                        .bind("id")
                        .to(4L)
                        .bind("changeSet")
                        .to(runner.getChangeSetId())
                        .build()));
            return ApiFutures.immediateFuture(null);
          }
        },
        exec);
    ApiFuture<Timestamp> commitTs = runner.getCommitTimestamp();

    List<Struct> updates = drainChanges();
    assertThat(updates).hasSize(2);
    assertThat(updates)
        .containsExactly(
            Struct.newBuilder()
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("Tre")
                .set("CHANGE_SET_ID")
                .to(runner.getChangeSetId())
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(4L)
                .set("NAME")
                .to("Fire")
                .set("CHANGE_SET_ID")
                .to(runner.getChangeSetId())
                .build());
    assertThat(lastCommitTimestamp).isEqualTo(commitTs.get());
    exec.shutdown();
  }

  private void testMutationsWithAsyncTransactionManager(DatabaseClientWithChangeSets client)
      throws Exception {
    latch = new CountDownLatch(2);
    try (AsyncTransactionManagerWithChangeSet manager = client.transactionManagerAsync()) {
      Timestamp commitTs;
      TransactionContextFuture txn = manager.beginAsync();
      while (true) {
        try {
          commitTs =
              txn.then(
                      (context, v) -> {
                        context.buffer(
                            Arrays.asList(
                                Mutation.newUpdateBuilder("NUMBERS")
                                    .set("ID")
                                    .to(1L)
                                    .set("NAME")
                                    .to("Uno")
                                    .set("CHANGE_SET_ID")
                                    .to(manager.getChangeSetId())
                                    .build(),
                                Mutation.newUpdateBuilder("NUMBERS")
                                    .set("ID")
                                    .to(5L)
                                    .set("NAME")
                                    .to("Cinque")
                                    .set("CHANGE_SET_ID")
                                    .to(manager.getChangeSetId())
                                    .build()));
                        return ApiFutures.immediateFuture(null);
                      },
                      MoreExecutors.directExecutor())
                  .commitAsync()
                  .get();
          break;
        } catch (AbortedException e) {
          Thread.sleep(e.getRetryDelayInMillis() / 1000);
          txn = manager.resetForRetryAsync();
        }
      }

      List<Struct> updates = drainChanges();
      assertThat(updates).hasSize(2);
      assertThat(updates)
          .containsExactly(
              Struct.newBuilder()
                  .set("ID")
                  .to(1L)
                  .set("NAME")
                  .to("Uno")
                  .set("CHANGE_SET_ID")
                  .to(manager.getChangeSetId())
                  .build(),
              Struct.newBuilder()
                  .set("ID")
                  .to(5L)
                  .set("NAME")
                  .to("Cinque")
                  .set("CHANGE_SET_ID")
                  .to(manager.getChangeSetId())
                  .build());
      assertThat(lastCommitTimestamp).isEqualTo(commitTs);
    }
  }

  private void testDmlWithAsyncTransactionManager(DatabaseClientWithChangeSets client)
      throws Exception {
    latch = new CountDownLatch(2);
    try (AsyncTransactionManagerWithChangeSet manager = client.transactionManagerAsync()) {
      TransactionContextFuture txn = manager.beginAsync();
      Timestamp commitTs;
      while (true) {
        try {
          String sql = "UPDATE NUMBERS SET NAME=@name, CHANGE_SET_ID=@changeSet WHERE ID=@id";
          commitTs =
              txn.then(
                      (context, v) ->
                          context.batchUpdateAsync(
                              Arrays.asList(
                                  Statement.newBuilder(sql)
                                      .bind("name")
                                      .to("Tres")
                                      .bind("id")
                                      .to(3L)
                                      .bind("changeSet")
                                      .to(manager.getChangeSetId())
                                      .build(),
                                  Statement.newBuilder(sql)
                                      .bind("name")
                                      .to("Cuatro")
                                      .bind("id")
                                      .to(4L)
                                      .bind("changeSet")
                                      .to(manager.getChangeSetId())
                                      .build())),
                      MoreExecutors.directExecutor())
                  .commitAsync()
                  .get();
          break;
        } catch (AbortedException e) {
          Thread.sleep(e.getRetryDelayInMillis() / 1000);
          txn = manager.resetForRetryAsync();
        }
      }

      List<Struct> updates = drainChanges();
      assertThat(updates).hasSize(2);
      assertThat(updates)
          .containsExactly(
              Struct.newBuilder()
                  .set("ID")
                  .to(3L)
                  .set("NAME")
                  .to("Tres")
                  .set("CHANGE_SET_ID")
                  .to(manager.getChangeSetId())
                  .build(),
              Struct.newBuilder()
                  .set("ID")
                  .to(4L)
                  .set("NAME")
                  .to("Cuatro")
                  .set("CHANGE_SET_ID")
                  .to(manager.getChangeSetId())
                  .build());
      assertThat(lastCommitTimestamp).isEqualTo(commitTs);
    }
  }

  private ImmutableList<Struct> drainChanges() throws Exception {
    assertThat(latch.await(10L, TimeUnit.SECONDS)).isTrue();
    ImmutableList<Struct> changes = ImmutableList.copyOf(receivedChanges);
    receivedChanges.clear();
    return changes;
  }
}
