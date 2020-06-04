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
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.Samples;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper.ITSpannerEnv;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.security.Permission;
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
  private static final Logger logger = Logger.getLogger(ITSpannerTableTailerTest.class.getName());
  private static final ITSpannerEnv env = new ITSpannerEnv();
  private static DatabaseId databaseId;
  private static Database database;
  private static DatabaseClient client;
  private static ExecutorService executor = Executors.newSingleThreadExecutor();
  private CountDownLatch systemExitLatch;

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    database =
        env.createTestDb(
            ImmutableList.of(
                "CREATE TABLE NUMBERS1 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                "CREATE TABLE NUMBERS2 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                "CREATE TABLE MY_TABLE (ID INT64, NAME STRING(MAX), SHARD_ID STRING(MAX), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
                "CREATE INDEX IDX_MY_TABLE_SHARDING ON MY_TABLE (SHARD_ID, LAST_MODIFIED)",
                "CREATE TABLE NUMBERS_WITHOUT_COMMIT_TIMESTAMP (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP) PRIMARY KEY (ID)"));
    databaseId = database.getId();
    client = env.getSpanner().getDatabaseClient(databaseId);
    logger.info(String.format("Created database %s", database.getId().toString()));
  }

  @AfterClass
  public static void teardown() {
    SpannerTestHelper.teardownSpanner(env);
    executor.shutdown();
  }

  @Before
  public void createSystemExitLatch() {
    systemExitLatch = new CountDownLatch(1);
  }

  @After
  public void deleteRows() {
    client.write(
        ImmutableList.of(
            Mutation.delete("NUMBERS1", KeySet.all()),
            Mutation.delete("NUMBERS2", KeySet.all()),
            Mutation.delete("MY_TABLE", KeySet.all()),
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
        throw new SecurityException("Do not exit from test");
      }
    }

    @Override
    public void checkPermission(Permission perm) {}
  }

  private Future<String> runSample(SampleRunnable example, CountDownLatch startedLatch)
      throws InterruptedException, IOException {
    return executor.submit(
        new Callable<String>() {
          @Override
          public String call() throws Exception {
            PrintStream stdOut = System.out;
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            PrintStream out =
                new PrintStream(bout) {
                  @Override
                  public void write(byte buf[], int off, int len) {
                    super.write(buf, off, len);
                    if (new String(buf).equals("Started change watcher")) {
                      startedLatch.countDown();
                    }
                  }
                };
            System.setOut(out);
            try {
              example.run();
              return bout.toString();
            } finally {
              System.setOut(stdOut);
            }
          }
        });
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
  public void testWatchSingleTableExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchSingleTableExample(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "NUMBERS1");
            },
            latch);
    latch.await(30L, TimeUnit.SECONDS);
    Timestamp commitTs =
        client.write(
            insertOrUpdateNumbers("NUMBERS1", 0, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(30L, TimeUnit.SECONDS);
    TableId table = TableId.of(databaseId, "NUMBERS1");
    for (Struct row : numberRows(commitTs, 0, NUMBER_NAMES.length)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table, row.toString()));
    }
  }

  @Test
  public void testWatchAllTablesExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchAllTablesExample(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase());
            },
            latch);
    latch.await(30L, TimeUnit.SECONDS);
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
    TableId table1 = TableId.of(databaseId, "NUMBERS1");
    for (Struct row : numberRows(commitTs1, 0, 1)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table1, row.toString()));
    }
    TableId table2 = TableId.of(databaseId, "NUMBERS2");
    for (Struct row : numberRows(commitTs2, 1, NUMBER_NAMES.length)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table2, row.toString()));
    }
    assertThat(res).doesNotContain("NUMBERS_WITHOUT_COMMIT_TIMESTAMP");
  }

  @Test
  public void testWatchSetOfTablesExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchSetOfTablesExample(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "NUMBERS1",
                  "NUMBERS2");
            },
            latch);
    latch.await(30L, TimeUnit.SECONDS);
    Timestamp commitTs1 =
        client.write(insertOrUpdateNumbers("NUMBERS1", 0, 1, Value.COMMIT_TIMESTAMP));
    Timestamp commitTs2 =
        client.write(
            insertOrUpdateNumbers("NUMBERS2", 1, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(30L, TimeUnit.SECONDS);
    TableId table1 = TableId.of(databaseId, "NUMBERS1");
    for (Struct row : numberRows(commitTs1, 0, 1)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table1, row.toString()));
    }
    TableId table2 = TableId.of(databaseId, "NUMBERS2");
    for (Struct row : numberRows(commitTs2, 1, NUMBER_NAMES.length)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table2, row.toString()));
    }
  }

  @Test
  public void testWatchAllExceptOfSomeTablesExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchAllExceptOfSomeTablesExample(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "NUMBERS1",
                  "NUMBERS_WITHOUT_COMMIT_TIMESTAMP");
            },
            latch);
    latch.await(30L, TimeUnit.SECONDS);
    // First write to the excluded table. These changes should not be picked up.
    client.write(insertOrUpdateNumbers("NUMBERS1", 0, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    Timestamp commitTs =
        client.write(
            insertOrUpdateNumbers("NUMBERS2", 0, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(30L, TimeUnit.SECONDS);
    TableId table = TableId.of(databaseId, "NUMBERS2");
    for (Struct row : numberRows(commitTs, 0, NUMBER_NAMES.length)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table, row.toString()));
    }
    assertThat(res).doesNotContain("NUMBERS1");
  }

  @Test
  public void testWatchTableWithSpecificPollInterval() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchTableWithSpecificPollInterval(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "NUMBERS1");
            },
            latch);
    latch.await(30L, TimeUnit.SECONDS);
    Timestamp commitTs =
        client.write(
            insertOrUpdateNumbers("NUMBERS1", 0, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(30L, TimeUnit.SECONDS);
    TableId table = TableId.of(databaseId, "NUMBERS1");
    for (Struct row : numberRows(commitTs, 0, NUMBER_NAMES.length)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table, row.toString()));
    }
  }

  @Test
  public void testErrorHandling() throws Exception {
    env.getSpanner()
        .getDatabaseAdminClient()
        .updateDatabaseDdl(
            databaseId.getInstanceId().getInstance(),
            databaseId.getDatabase(),
            ImmutableList.of(
                "CREATE TABLE NUMBERS3 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)"),
            null)
        .get();
    SecurityManager currentSecurityManager = System.getSecurityManager();
    try {
      System.setSecurityManager(new TestSecurityManager());
      CountDownLatch latch = new CountDownLatch(1);
      runSample(
          () -> {
            Samples.errorHandling(
                databaseId.getInstanceId().getProject(),
                databaseId.getInstanceId().getInstance(),
                databaseId.getDatabase());
          },
          latch);
      latch.await(30L, TimeUnit.SECONDS);
      // Drop one of the tables that is being watched.
      Logger logger = Logger.getLogger(SpannerTableTailer.class.getName());
      Level level = logger.getLevel();
      PrintStream stdErr = System.err;
      try {
        logger.setLevel(Level.OFF);
        ByteArrayOutputStream berr = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(berr);
        System.setErr(err);
        env.getSpanner()
            .getDatabaseAdminClient()
            .updateDatabaseDdl(
                databaseId.getInstanceId().getInstance(),
                databaseId.getDatabase(),
                ImmutableList.of("DROP TABLE NUMBERS3"),
                null)
            .get();
        assertThat(systemExitLatch.await(30L, TimeUnit.SECONDS)).isTrue();
        String errors = berr.toString();
        assertThat(errors).contains("Database change watcher failed.");
        assertThat(errors).contains("State before failure: RUNNING");
        assertThat(errors).contains("Table not found: NUMBERS3");
      } finally {
        logger.setLevel(level);
        System.setErr(stdErr);
      }
    } finally {
      System.setSecurityManager(currentSecurityManager);
    }
  }

  @Test
  public void testCustomCommitTimestampRepository() throws Exception {
    // Create a separate database for the commit timestamps.
    Database commitTimestampsDb = env.createTestDb(ImmutableList.of());
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.customCommitTimestampRepository(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "NUMBERS1",
                  commitTimestampsDb.getId().getDatabase(),
                  "MY_LAST_SEEN_COMMIT_TIMESTAMPS");
            },
            latch);
    latch.await(30L, TimeUnit.SECONDS);
    Timestamp[] timestamps = new Timestamp[NUMBER_NAMES.length];
    for (int i = 0; i < NUMBER_NAMES.length; i++) {
      timestamps[i] =
          client.write(insertOrUpdateNumbers("NUMBERS1", i, i + 1, Value.COMMIT_TIMESTAMP));
    }
    String res = out.get(30L, TimeUnit.SECONDS);
    TableId table = TableId.of(databaseId, "NUMBERS1");
    for (int i = 0; i < NUMBER_NAMES.length; i++) {
      Struct row = numberRows(timestamps[i], i, i + 1).iterator().next();
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table, row.toString()));
    }
  }

  @Test
  public void testInMemCommitTimestampRepository() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.inMemCommitTimestampRepository(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "NUMBERS1");
            },
            latch);
    latch.await(30L, TimeUnit.SECONDS);
    Timestamp[] timestamps = new Timestamp[NUMBER_NAMES.length];
    for (int i = 0; i < NUMBER_NAMES.length; i++) {
      timestamps[i] =
          client.write(insertOrUpdateNumbers("NUMBERS1", i, i + 1, Value.COMMIT_TIMESTAMP));
    }
    String res = out.get(30L, TimeUnit.SECONDS);
    TableId table = TableId.of(databaseId, "NUMBERS1");
    for (int i = 0; i < NUMBER_NAMES.length; i++) {
      Struct row = numberRows(timestamps[i], i, i + 1).iterator().next();
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table, row.toString()));
    }
  }

  @Test
  public void testCustomExecutorExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.customExecutorExample(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase());
            },
            latch);
    latch.await(30L, TimeUnit.SECONDS);
    Timestamp commitTs1 =
        client.write(insertOrUpdateNumbers("NUMBERS1", 0, 1, Value.COMMIT_TIMESTAMP));
    Timestamp commitTs2 =
        client.write(
            insertOrUpdateNumbers("NUMBERS2", 1, NUMBER_NAMES.length, Value.COMMIT_TIMESTAMP));
    String res = out.get(30L, TimeUnit.SECONDS);
    TableId table1 = TableId.of(databaseId, "NUMBERS1");
    for (Struct row : numberRows(commitTs1, 0, 1)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table1, row.toString()));
    }
    TableId table2 = TableId.of(databaseId, "NUMBERS2");
    for (Struct row : numberRows(commitTs2, 1, NUMBER_NAMES.length)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table2, row.toString()));
    }
  }

  @Test
  public void testWatchTableWithTimebasedShardingExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchTableWithTimebasedShardingExample(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "MY_TABLE");
            },
            latch);
    latch.await(120L, TimeUnit.SECONDS);
    String res = out.get(120L, TimeUnit.SECONDS);
    assertThat(res).contains("1, Name 1,");
    assertThat(res).contains("2, Name 2,");
    assertThat(res).contains("3, Name 3,");
  }
}
