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

package com.google.cloud.spanner.watcher.sample.it;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper.ITSpannerEnv;
import com.google.cloud.spanner.watcher.sample.Samples;
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
  private static final Logger logger = Logger.getLogger(ITSamplesTest.class.getName());
  private static final ITSpannerEnv env = new ITSpannerEnv();
  private static DatabaseId databaseId;
  private static DatabaseClient client;
  private static final ExecutorService executor = Executors.newSingleThreadExecutor();
  private CountDownLatch systemExitLatch;

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    Database database = env.createTestDb(
        ImmutableList.of(
            "CREATE TABLE NUMBERS1 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
            "CREATE TABLE NUMBERS2 (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
            "CREATE TABLE MY_TABLE (ID INT64, NAME STRING(MAX), SHARD_ID STRING(MAX), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
            "CREATE INDEX IDX_MY_TABLE_SHARDING ON MY_TABLE (SHARD_ID, LAST_MODIFIED DESC)",
            "CREATE TABLE MY_TABLE_NULLABLE_SHARD (ID INT64, NAME STRING(MAX), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true), PROCESSED BOOL, SHARD_ID INT64 AS (CASE WHEN PROCESSED THEN NULL ELSE MOD(FARM_FINGERPRINT(NAME), 19) END) STORED) PRIMARY KEY (ID)",
            "CREATE NULL_FILTERED INDEX IDX_MY_TABLE_NULLABLE_SHARDING ON MY_TABLE_NULLABLE_SHARD (SHARD_ID, LAST_MODIFIED DESC)",
            "CREATE TABLE MULTIPLE_COMMIT_TS (ID INT64, NAME STRING(MAX), LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true), LAST_BATCH_JOB TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (ID)",
            "CREATE TABLE NUMBERS_WITHOUT_COMMIT_TIMESTAMP (ID INT64 NOT NULL, NAME STRING(100), LAST_MODIFIED TIMESTAMP) PRIMARY KEY (ID)",
            "CREATE TABLE CHANGE_SETS (CHANGE_SET_ID STRING(MAX), COMMIT_TIMESTAMP TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (CHANGE_SET_ID)",
            "CREATE TABLE DATA_TABLE (ID INT64, NAME STRING(MAX), CHANGE_SET_ID STRING(MAX)) PRIMARY KEY (ID)",
            "CREATE INDEX IDX_DATA_TABLE_CHANGE_SET_ID ON DATA_TABLE (CHANGE_SET_ID)"));
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
    void run() throws InterruptedException, IOException;
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

  private Future<String> runSample(SampleRunnable example, CountDownLatch startedLatch) {
    return executor.submit(
        new Callable<String>() {
          @Override
          public String call() throws Exception {
            PrintStream stdOut = System.out;
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            PrintStream out =
                new PrintStream(bout) {
                  @Override
                  public void write(byte[] buf, int off, int len) {
                    super.write(buf, off, len);
                    if (bout.toString().contains("Started change watcher")) {
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
  static final String[] MULTIPLE_COMMIT_TS_NAMES =
      new String[] {"ONE", "TWO", "THREE", "FOUR", "FIVE"};

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

  Iterable<Struct> multipleCommitTSRowsLastModified(Timestamp commitTs, int begin, int end) {
    ImmutableList.Builder<Struct> builder =
        ImmutableList.builderWithExpectedSize(MULTIPLE_COMMIT_TS_NAMES.length);
    for (int i = begin; i < end; i++) {
      builder.add(
          Struct.newBuilder()
              .set("ID")
              .to(Long.valueOf(i + 1))
              .set("NAME")
              .to(MULTIPLE_COMMIT_TS_NAMES[i])
              .set("LAST_MODIFIED")
              .to(commitTs)
              .set("LAST_BATCH")
              .to(Timestamp.MIN_VALUE)
              .build());
    }
    return builder.build();
  }

  Iterable<Struct> multipleCommitTSRowsLastBatch(Timestamp commitTs, int begin, int end) {
    ImmutableList.Builder<Struct> builder =
        ImmutableList.builderWithExpectedSize(MULTIPLE_COMMIT_TS_NAMES.length);
    for (int i = begin; i < end; i++) {
      builder.add(
          Struct.newBuilder()
              .set("ID")
              .to(Long.valueOf(i + 1))
              .set("NAME")
              .to(MULTIPLE_COMMIT_TS_NAMES[i])
              .set("LAST_MODIFIED")
              .to(Timestamp.MIN_VALUE)
              .set("LAST_BATCH_JOB")
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
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
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
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
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
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
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
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
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
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
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
      assertTrue(latch.await(300L, TimeUnit.SECONDS));
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
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
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
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
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
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
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
  public void testWatchTableWithFixedShardProviderExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchTableWithShardingExample(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "MY_TABLE");
            },
            latch);
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
    client.write(
        ImmutableList.of(
            Mutation.newInsertBuilder("MY_TABLE")
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("Name 1")
                .set("SHARD_ID")
                .to("WEST")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertBuilder("MY_TABLE")
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("Name 2")
                .set("SHARD_ID")
                .to("EAST")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertBuilder("MY_TABLE")
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("Name 3")
                .set("SHARD_ID")
                .to("WEST")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build()));
    String res = out.get(30L, TimeUnit.SECONDS);
    assertThat(res).contains("1, Name 1, WEST");
    assertThat(res).contains("2, Name 2, EAST");
    assertThat(res).contains("3, Name 3, WEST");
  }

  @Test
  public void testWatchTableWithNotNullShardProviderExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchTableWithNotNullShardProviderExample(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "MY_TABLE_NULLABLE_SHARD",
                  "IDX_MY_TABLE_NULLABLE_SHARDING");
            },
            latch);
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
    client.write(
        ImmutableList.of(
            Mutation.newInsertBuilder("MY_TABLE_NULLABLE_SHARD")
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("Name 1")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertBuilder("MY_TABLE_NULLABLE_SHARD")
                .set("ID")
                .to(2L)
                .set("NAME")
                .to("Name 2")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            Mutation.newInsertBuilder("MY_TABLE_NULLABLE_SHARD")
                .set("ID")
                .to(3L)
                .set("NAME")
                .to("Name 3")
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build()));
    String res = out.get(30L, TimeUnit.SECONDS);
    assertThat(res).contains("1, Name 1");
    assertThat(res).contains("2, Name 2");
    assertThat(res).contains("3, Name 3");
  }

  @Test
  public void testWatchTableWithTimebasedShardProviderExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchTableWithTimebasedShardProviderExample(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase(),
                  "MY_TABLE");
            },
            latch);
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
    String res = out.get(120L, TimeUnit.SECONDS);
    assertThat(res).contains("1, Name 1,");
    assertThat(res).contains("2, Name 2,");
    assertThat(res).contains("3, Name 3,");
  }

  @Test
  public void testWatchTableWithMultipleCommitTimestampColumnsExample() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchTableWithMultipleCommitTimestampColumns(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase());
            },
            latch);
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
    // First do a write using the commit timestamp column that should not be picked up.
    Timestamp ts1 =
        client.write(
            ImmutableList.of(
                Mutation.newInsertOrUpdateBuilder("MULTIPLE_COMMIT_TS")
                    .set("ID")
                    .to(1L)
                    .set("NAME")
                    .to("ONE")
                    .set("LAST_MODIFIED")
                    .to(Timestamp.MIN_VALUE)
                    .set("LAST_BATCH_JOB")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("MULTIPLE_COMMIT_TS")
                    .set("ID")
                    .to(2L)
                    .set("NAME")
                    .to("TWO")
                    .set("LAST_MODIFIED")
                    .to(Timestamp.MIN_VALUE)
                    .set("LAST_BATCH_JOB")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build()));
    // Then do a write that does use the LAST_MODIFIED column. This change should be picked up.
    Timestamp ts2 =
        client.write(
            ImmutableList.of(
                Mutation.newInsertOrUpdateBuilder("MULTIPLE_COMMIT_TS")
                    .set("ID")
                    .to(3L)
                    .set("NAME")
                    .to("THREE")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("MULTIPLE_COMMIT_TS")
                    .set("ID")
                    .to(4L)
                    .set("NAME")
                    .to("FOUR")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .set("LAST_BATCH_JOB")
                    .to(Timestamp.MIN_VALUE)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("MULTIPLE_COMMIT_TS")
                    .set("ID")
                    .to(5L)
                    .set("NAME")
                    .to("FIVE")
                    .set("LAST_MODIFIED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .set("LAST_BATCH_JOB")
                    .to(Timestamp.MIN_VALUE)
                    .build()));
    String res = out.get(60L, TimeUnit.SECONDS);

    TableId table = TableId.of(databaseId, "MULTIPLE_COMMIT_TS");
    for (Struct row : multipleCommitTSRowsLastBatch(ts1, 0, 2)) {
      assertThat(res)
          .doesNotContain(
              String.format("Received change for table %s: %s%n", table, row.toString()));
    }
    for (Struct row : multipleCommitTSRowsLastModified(ts2, 3, 5)) {
      assertThat(res)
          .contains(String.format("Received change for table %s: %s%n", table, row.toString()));
    }
  }

  @Test
  public void testWatchTableWithoutCommitTimestampColumn() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    Future<String> out =
        runSample(
            () -> {
              Samples.watchTableWithoutCommitTimestampColumn(
                  databaseId.getInstanceId().getProject(),
                  databaseId.getInstanceId().getInstance(),
                  databaseId.getDatabase());
            },
            latch);
    assertTrue(latch.await(300L, TimeUnit.SECONDS));
    // The sample itself already writes 3 changes.
    String res = out.get(60L, TimeUnit.SECONDS);
    TableId table = TableId.of(databaseId, "DATA_TABLE");
    TableId changeSetTable = TableId.of(databaseId, "CHANGE_SETS");
    assertThat(res).contains(String.format("Received change for table %s: [1, One, ", table));
    assertThat(res).contains(String.format("Received change for table %s: [2, Two, ", table));
    assertThat(res).contains(String.format("Received change for table %s: [3, Three, ", table));
    assertThat(res).doesNotContain(String.format("Received change for table %s:", changeSetTable));
  }
}
