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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper.ITSpannerEnv;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
public class ITSpannerDatabaseTailerTest {
  private static final Logger logger =
      Logger.getLogger(ITSpannerDatabaseTailerTest.class.getName());
  private static final ITSpannerEnv env = new ITSpannerEnv();
  private static Database database;
  private final Queue<Struct> receivedChanges = new ConcurrentLinkedQueue<>();
  private volatile CountDownLatch latch = new CountDownLatch(0);

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    database =
        env.createTestDb(
            Arrays.asList(
                "CREATE TABLE NUMBERS1 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                "CREATE TABLE NUMBERS2 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)"));
    logger.info(String.format("Created database %s", database.getId().toString()));
  }

  @AfterClass
  public static void teardown() {
    SpannerTestHelper.teardownSpanner(env);
  }

  @Test
  public void testSpannerTailer() throws Exception {
    Spanner spanner = env.getSpanner();
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, database.getId())
            .allTables()
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, database.getId())
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    tailer.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            logger.info(
                String.format(
                    "Received changed for table %s: %s", table, row.asStruct().toString()));
            receivedChanges.add(row.asStruct());
            latch.countDown();
          }
        });
    tailer.startAsync().awaitRunning();

    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    latch = new CountDownLatch(3);
    Timestamp commitTs =
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
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("TWO")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("THREE")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    latch = new CountDownLatch(2);
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("NUMBERS2")
                    .set("ID")
                    .to(4L)
                    .set("NAME")
                    .to("FOUR")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS1")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("FIVE")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
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
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("FIVE")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    latch = new CountDownLatch(2);
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newUpdateBuilder("NUMBERS1")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("one")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newUpdateBuilder("NUMBERS1")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("five")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
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
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("five")
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    // Verify that deletes are not picked up by the poller.
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.delete("NUMBERS2", Key.of(2L)), Mutation.delete("NUMBERS1", Key.of(3L))));
    Thread.sleep(500L);
    assertThat(receivedChanges).isEmpty();
    tailer.stopAsync().awaitTerminated();
  }

  private ImmutableList<Struct> drainChanges() throws Exception {
    assertThat(latch.await(5L, TimeUnit.SECONDS)).isTrue();
    ImmutableList<Struct> changes = ImmutableList.copyOf(receivedChanges);
    receivedChanges.clear();
    return changes;
  }
}
