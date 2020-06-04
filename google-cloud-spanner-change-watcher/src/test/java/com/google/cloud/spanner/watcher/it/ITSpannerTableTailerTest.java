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
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.FixedShardProvider;
import com.google.cloud.spanner.watcher.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.TimeBasedShardProvider;
import com.google.cloud.spanner.watcher.TimeBasedShardProvider.Interval;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper.ITSpannerEnv;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
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
public class ITSpannerTableTailerTest {
  private static final Logger logger = Logger.getLogger(ITSpannerTableTailerTest.class.getName());
  private static final ITSpannerEnv env = new ITSpannerEnv();
  private static Database database;
  private final Queue<Struct> receivedChanges = new ConcurrentLinkedQueue<>();
  private volatile CountDownLatch latch = new CountDownLatch(0);

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    database =
        env.createTestDb(
            ImmutableList.of(
                "CREATE TABLE NUMBERS (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                "CREATE TABLE NUMBERS_WITH_SHARDS (ID INT64 NOT NULL, NAME STRING(100), SHARD_ID STRING(MAX), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                "CREATE INDEX IDX_NUMBERS_SHARDS ON NUMBERS_WITH_SHARDS (SHARD_ID, LAST_MODIFIED)"));
    logger.info(String.format("Created database %s", database.getId().toString()));
  }

  @AfterClass
  public static void teardown() {
    SpannerTestHelper.teardownSpanner(env);
  }

  @Before
  public void deleteRowsInNumbersWithShards() {
    Spanner spanner = env.getSpanner();
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    client.writeAtLeastOnce(ImmutableList.of(Mutation.delete("NUMBERS_WITH_SHARDS", KeySet.all())));
  }

  @Test
  public void testSpannerTailer() throws Exception {
    Spanner spanner = env.getSpanner();
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(spanner, TableId.of(database.getId(), "NUMBERS"))
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
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(4L)
                    .set("NAME")
                    .to("FOUR")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS")
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
                Mutation.newUpdateBuilder("NUMBERS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("one")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newUpdateBuilder("NUMBERS")
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
                Mutation.delete("NUMBERS", Key.of(2L)), Mutation.delete("NUMBERS", Key.of(3L))));
    Thread.sleep(500L);
    assertThat(receivedChanges).isEmpty();
    tailer.stopAsync().awaitTerminated();
  }

  @Test
  public void testSpannerTailerWithTimebasedShard() throws Exception {
    Spanner spanner = env.getSpanner();
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(spanner, TableId.of(database.getId(), "NUMBERS_WITH_SHARDS"))
            .setShardProvider(TimeBasedShardProvider.create("SHARD_ID", Interval.DAY))
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

    String currentShard = Interval.DAY.getCurrentShardId(client.singleUse());
    Timestamp commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("ONE")
                    .set("SHARD_ID")
                    .to(currentShard)
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(2L)
                    .set("NAME")
                    .to("TWO")
                    .set("SHARD_ID")
                    .to(currentShard)
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(3L)
                    .set("NAME")
                    .to("THREE")
                    .set("SHARD_ID")
                    .to(currentShard)
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
                .set("SHARD_ID")
                .to(currentShard)
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("TWO")
                .set("SHARD_ID")
                .to(currentShard)
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("THREE")
                .set("SHARD_ID")
                .to(currentShard)
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    latch = new CountDownLatch(2);
    currentShard = Interval.DAY.getCurrentShardId(client.singleUse());
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(4L)
                    .set("NAME")
                    .to("FOUR")
                    .set("SHARD_ID")
                    .to(currentShard)
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("FIVE")
                    .set("SHARD_ID")
                    .to(currentShard)
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
                .set("SHARD_ID")
                .to(currentShard)
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("FIVE")
                .set("SHARD_ID")
                .to(currentShard)
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    latch = new CountDownLatch(2);
    currentShard = Interval.DAY.getCurrentShardId(client.singleUse());
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("one")
                    .set("SHARD_ID")
                    .to(currentShard)
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("five")
                    .set("SHARD_ID")
                    .to(currentShard)
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
                .set("SHARD_ID")
                .to(currentShard)
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("five")
                .set("SHARD_ID")
                .to(currentShard)
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    // Verify that deletes are not picked up by the poller.
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.delete("NUMBERS_WITH_SHARDS", Key.of(2L)),
                Mutation.delete("NUMBERS_WITH_SHARDS", Key.of(3L))));
    Thread.sleep(500L);
    assertThat(receivedChanges).isEmpty();
    tailer.stopAsync().awaitTerminated();
  }

  @Test
  public void testSpannerTailerWithFixedShard() throws Exception {
    final ImmutableList<String> shards = ImmutableList.of("EAST", "WEST");

    Spanner spanner = env.getSpanner();
    List<SpannerTableTailer> tailers = new ArrayList<>();
    // Create one tailer for each shard. Each tailer is responsible for polling that part of the
    // table and reporting those changes.
    for (String shard : shards) {
      SpannerTableTailer tailer =
          SpannerTableTailer.newBuilder(
                  spanner, TableId.of(database.getId(), "NUMBERS_WITH_SHARDS"))
              .setShardProvider(FixedShardProvider.create("SHARD_ID", shard))
              .setPollInterval(Duration.ofMillis(10L))
              .setCommitTimestampRepository(
                  SpannerCommitTimestampRepository.newBuilder(spanner, database.getId())
                      // We need to use a separate LAST_SEEN_COMMIT_TIMESTAMPS table for each
                      // tailer.
                      .setCommitTimestampsTable("LAST_SEEN_COMMIT_TIMESTAMPS_" + shard)
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
      tailer.startAsync();
      tailers.add(tailer);
    }
    for (SpannerTableTailer tailer : tailers) {
      tailer.awaitRunning();
    }

    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    latch = new CountDownLatch(3);

    Timestamp commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("ONE")
                    .set("SHARD_ID")
                    .to(shards.get(0))
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(2L)
                    .set("NAME")
                    .to("TWO")
                    .set("SHARD_ID")
                    .to(shards.get(1))
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(3L)
                    .set("NAME")
                    .to("THREE")
                    .set("SHARD_ID")
                    .to(shards.get(0))
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
                .set("SHARD_ID")
                .to(shards.get(0))
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("TWO")
                .set("SHARD_ID")
                .to(shards.get(1))
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("THREE")
                .set("SHARD_ID")
                .to(shards.get(0))
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    latch = new CountDownLatch(2);
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(4L)
                    .set("NAME")
                    .to("FOUR")
                    .set("SHARD_ID")
                    .to(shards.get(1))
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("FIVE")
                    .set("SHARD_ID")
                    .to(shards.get(0))
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
                .set("SHARD_ID")
                .to(shards.get(1))
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("FIVE")
                .set("SHARD_ID")
                .to(shards.get(0))
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    latch = new CountDownLatch(2);
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.newUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("one")
                    .set("SHARD_ID")
                    .to(shards.get(0))
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newUpdateBuilder("NUMBERS_WITH_SHARDS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("five")
                    .set("SHARD_ID")
                    .to(shards.get(0))
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
                .set("SHARD_ID")
                .to(shards.get(0))
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build(),
            Struct.newBuilder()
                .set("ID")
                .to(5L)
                .set("NAME")
                .to("five")
                .set("SHARD_ID")
                .to(shards.get(0))
                .set("LAST_MODIFIED")
                .to(commitTs)
                .build());

    // Verify that deletes are not picked up by the poller.
    commitTs =
        client.writeAtLeastOnce(
            Arrays.asList(
                Mutation.delete("NUMBERS_WITH_SHARDS", Key.of(2L)),
                Mutation.delete("NUMBERS_WITH_SHARDS", Key.of(3L))));
    Thread.sleep(500L);
    assertThat(receivedChanges).isEmpty();
    for (SpannerTableTailer tailer : tailers) {
      tailer.stopAsync();
    }
    for (SpannerTableTailer tailer : tailers) {
      tailer.awaitTerminated();
    }
  }

  private ImmutableList<Struct> drainChanges() throws Exception {
    assertThat(latch.await(5L, TimeUnit.SECONDS)).isTrue();
    ImmutableList<Struct> changes = ImmutableList.copyOf(receivedChanges);
    receivedChanges.clear();
    return changes;
  }
}
