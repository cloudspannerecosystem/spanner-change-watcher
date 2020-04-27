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
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.watcher.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper.ITSpannerEnv;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.threeten.bp.Duration;

@RunWith(Parameterized.class)
public class ITSpannerDatabaseTailerStressTest {
  private static final String[] TABLE_NAMES = new String[] {"TABLE1", "TABLE2", "TABLE3"};

  @Parameter(0)
  public int changeCount;

  @Parameter(1)
  public int changeRunners;

  @Parameters(name = "change count= {0}, runners= {1}")
  public static Collection<Object[]> parameters() {
    List<Object[]> params = new ArrayList<>();
    for (int runners = 8; runners <= 256; runners *= 2) {
      for (int changeCount = runners; changeCount <= 1024; changeCount *= 2) {
        params.add(new Object[] {changeCount, runners});
      }
    }
    return params;
  }

  static class TestKey {
    private final TableId tableId;
    private final Long id;

    TestKey(TableId tableId, Long id) {
      this.tableId = tableId;
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TestKey)) {
        return false;
      }
      TestKey other = (TestKey) o;
      return Objects.equals(tableId, other.tableId) && Objects.equals(id, other.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableId, id);
    }
  }

  private static final Logger logger =
      Logger.getLogger(ITSpannerTableTailerStressTest.class.getName());
  private static final ITSpannerEnv env = new ITSpannerEnv();
  private static Database database;
  private final Object lock = new Object();
  private final ConcurrentMap<TestKey, Timestamp> lastWrittenTimestamps = new ConcurrentHashMap<>();
  private final ConcurrentMap<TestKey, Timestamp> lastReceivedTimestamps =
      new ConcurrentHashMap<>();
  private final AtomicInteger sentChanges = new AtomicInteger();

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    List<String> statements = new LinkedList<>();
    for (String table : TABLE_NAMES) {
      statements.add(String.format(ITSpannerTableTailerStressTest.CREATE_TABLE, table));
    }
    database = env.createTestDb(statements);
    logger.info(String.format("Created database %s", database.getId().toString()));
  }

  @AfterClass
  public static void teardown() {
    SpannerTestHelper.teardownSpanner(env);
  }

  @After
  public void deleteTestData() {
    Spanner spanner = env.getSpanner();
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    for (String table : TABLE_NAMES) {
      client.write(Collections.singleton(Mutation.delete(table, KeySet.all())));
    }
    sentChanges.set(0);
  }

  @Test
  public void testStressSpannerTailer() throws Exception {
    System.out.printf("Starting test (changeCount=%d, runners=%d)\n", changeCount, changeRunners);
    Spanner spanner = env.getSpanner();
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, database.getId())
            .allTables()
            .setPollInterval(Duration.ofMillis(1L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, database.getId())
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    final CountDownLatch latch = new CountDownLatch(1);
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            lastReceivedTimestamps.put(
                new TestKey(table, row.getLong("ColInt64")), commitTimestamp);
            if (sentChanges.get() == changeCount * TABLE_NAMES.length
                && lastReceivedTimestamps.equals(lastWrittenTimestamps)) {
              latch.countDown();
            }
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.printf(
        "Change watcher started (changeCount=%d, runners=%d)\n", changeCount, changeRunners);

    Stopwatch watch = Stopwatch.createStarted();
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    ListeningExecutorService executor =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(changeRunners));
    List<ListenableFuture<Void>> futures = new ArrayList<>(changeRunners);
    for (int i = 0; i < changeRunners; i++) {
      futures.add(
          executor.submit(new GenerateChangesCallable(client, changeCount / changeRunners)));
    }
    Futures.allAsList(futures).get(300L, TimeUnit.SECONDS);
    System.out.printf(
        "Finished writing changes in %d seconds (changeCount=%d, runners=%d)\n",
        watch.elapsed(TimeUnit.SECONDS), changeCount, changeRunners);
    latch.await(300L, TimeUnit.SECONDS);
    System.out.printf(
        "Finished test in %d seconds (changeCount=%d, runners=%d)\n",
        watch.elapsed(TimeUnit.SECONDS), changeCount, changeRunners);
    assertThat(lastReceivedTimestamps).isEqualTo(lastWrittenTimestamps);
    watcher.stopAsync().awaitTerminated();
    executor.shutdown();
  }

  final class GenerateChangesCallable implements Callable<Void> {
    private final DatabaseClient client;
    private final int numChanges;

    GenerateChangesCallable(DatabaseClient client, int numChanges) {
      this.client = client;
      this.numChanges = numChanges;
    }

    @Override
    public Void call() {
      for (int i = 0; i < numChanges; i++) {
        List<Mutation> mutations = new LinkedList<>();
        for (String table : TABLE_NAMES) {
          mutations.add(ITSpannerTableTailerStressTest.createRandomMutation(table, changeCount));
        }
        Timestamp ts = client.write(mutations);
        sentChanges.addAndGet(TABLE_NAMES.length);
        synchronized (lock) {
          int tableIndex = 0;
          for (Mutation mutation : mutations) {
            Long id = mutation.asMap().get("ColInt64").getInt64();
            TestKey key = new TestKey(TableId.of(database.getId(), TABLE_NAMES[tableIndex]), id);
            Timestamp current = lastWrittenTimestamps.get(key);
            if (current == null || ts.compareTo(current) > 0) {
              lastWrittenTimestamps.put(key, ts);
            }
            tableIndex++;
          }
        }
      }
      return null;
    }
  }
}
