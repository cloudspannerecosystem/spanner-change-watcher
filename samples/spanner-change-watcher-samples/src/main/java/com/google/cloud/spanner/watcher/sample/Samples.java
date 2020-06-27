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

package com.google.cloud.spanner.watcher.sample;

import com.google.api.core.ApiService;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.CommitTimestampRepository;
import com.google.cloud.spanner.watcher.FixedShardProvider;
import com.google.cloud.spanner.watcher.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.TimebasedShardProvider;
import com.google.cloud.spanner.watcher.TimebasedShardProvider.Interval;
import com.google.cloud.spanner.watcher.TimebasedShardProvider.TimebasedShardId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.threeten.bp.Duration;

/** Samples for spanner-change-watcher. */
public class Samples {

  /** Watch a single table in a database for changes. */
  public static void watchSingleTableExample(
      String project, // "my-project"
      String instance, // "my-instance"
      String database, // "my-database"
      String table // "MY_TABLE"
      ) throws InterruptedException {
    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    TableId tableId = TableId.of(DatabaseId.of(project, instance, database), table);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerTableChangeWatcher watcher = SpannerTableTailer.newBuilder(spanner, tableId).build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
  }

  /** Watch all tables in a database for changes. */
  public static void watchAllTablesExample(
      String project, // "my-project"
      String instance, // "my-instance"
      String database // "my-database"
      ) throws InterruptedException {

    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    DatabaseId databaseId = DatabaseId.of(project, instance, database);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
  }

  /** Watch a set of tables in a database for changes. */
  public static void watchSetOfTablesExample(
      String project, // "my-project"
      String instance, // "my-instance"
      String database, // "my-database"
      String table1, // "MY_TABLE1"
      String table2 // "MY_TABLE2"
      ) throws InterruptedException {

    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    DatabaseId databaseId = DatabaseId.of(project, instance, database);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).includeTables(table1, table2).build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
  }

  /** Watch all except some tables in a database for changes. */
  public static void watchAllExceptOfSomeTablesExample(
      String project, // "my-project"
      String instance, // "my-instance"
      String database, // "my-database"
      String... excludedTables)
      throws InterruptedException {

    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    DatabaseId databaseId = DatabaseId.of(project, instance, database);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId)
            .allTables()
            .except(excludedTables)
            .build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
  }

  /** Watch a single table for changes with a very low poll interval. */
  public static void watchTableWithSpecificPollInterval(
      String project, // "my-project"
      String instance, // "my-instance"
      String database, // "my-database"
      String table // "MY_TABLE"
      ) throws InterruptedException {
    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    TableId tableId = TableId.of(DatabaseId.of(project, instance, database), table);
    final CountDownLatch latch = new CountDownLatch(3);
    // Poll the table every 10 milliseconds.
    SpannerTableChangeWatcher watcher =
        SpannerTableTailer.newBuilder(spanner, tableId)
            .setPollInterval(Duration.ofMillis(10L))
            .build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
  }

  /**
   * Change watchers implement the {@link ApiService} interface and allows users to be notified if a
   * watcher fails.
   */
  public static void errorHandling(
      String project, // "my-project"
      String instance, // "my-instance"
      String database // "my-database"
      ) throws InterruptedException {
    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    DatabaseId databaseId = DatabaseId.of(project, instance, database);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    // Add an ApiService listener to watch for failures.
    watcher.addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            // A failed watcher cannot be restarted and will stop the notification of any further
            // row changes.
            System.err.printf(
                "Database change watcher failed.%n    State before failure: %s%n    Error: %s%n",
                from, failure.getMessage());
            try {
              System.exit(1);
            } catch (SecurityException e) {
              System.err.println("System.exit(1) not allowed by SecurityManager");
            }
          }
        },
        MoreExecutors.directExecutor());
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");
  }

  /**
   * {@link SpannerTableTailer}s store the last seen commit timestamp in a table in the same
   * database as the table that is being watched. A user may also specify a custom table name to use
   * or even a table in a different database.
   */
  public static void customCommitTimestampRepository(
      String project, // "my-project"
      String instance, // "my-instance"
      String database, // "my-database"
      String table, // "MY_TABLE"
      String commitTimestampsDatabase, // "my-commit-timestamp-db"
      String commitTimestampsTable // "MY_LAST_SEEN_COMMIT_TIMESTAMPS"
      ) throws InterruptedException {
    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    TableId tableId = TableId.of(DatabaseId.of(project, instance, database), table);
    DatabaseId commitTimestampDbId = DatabaseId.of(project, instance, commitTimestampsDatabase);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerTableChangeWatcher watcher =
        SpannerTableTailer.newBuilder(spanner, tableId)
            // Use a custom commit timestamp repository that stores the last seen commit timestamps
            // in a different database than the database that is being watched.
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, commitTimestampDbId)
                    .setCommitTimestampsTable(commitTimestampsTable)
                    // Create the table if it does not already exist.
                    .setCreateTableIfNotExists(true)
                    .build())
            .build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
  }

  /**
   * {@link SpannerTableTailer}s store the last seen commit timestamp in a table in the same
   * database as the table that is being watched. A user may also specify a custom repository where
   * the last seen commit timestamp should be stored. This could also be an in-memory data store if
   * the watcher only needs to report changes that occur while the watcher is running.
   */
  public static void inMemCommitTimestampRepository(
      String project, // "my-project"
      String instance, // "my-instance"
      String database, // "my-database"
      String table // "MY_TABLE"
      ) throws InterruptedException {
    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    TableId tableId = TableId.of(DatabaseId.of(project, instance, database), table);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerTableChangeWatcher watcher =
        SpannerTableTailer.newBuilder(spanner, tableId)
            .setCommitTimestampRepository(
                new CommitTimestampRepository() {
                  private final ConcurrentMap<TableId, Timestamp> timestamps =
                      new ConcurrentHashMap<>();

                  @Override
                  public void set(TableId table, Timestamp commitTimestamp) {
                    timestamps.put(tableId, commitTimestamp);
                  }

                  @Override
                  public Timestamp get(TableId table) {
                    // Returns Timestamp.now() as the default value if there is no commit timestamp
                    // known for the table. This means that the watcher will only report changes
                    // that occur after this watcher has been started.
                    return timestamps.getOrDefault(tableId, Timestamp.now());
                  }
                })
            .build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
  }

  /** Use a custom executor for the change watcher. */
  public static void customExecutorExample(
      String project, // "my-project"
      String instance, // "my-instance"
      String database // "my-database"
      ) throws InterruptedException {
    Spanner spanner = SpannerOptions.getDefaultInstance().getService();
    DatabaseId databaseId = DatabaseId.of(SpannerOptions.getDefaultProjectId(), instance, database);
    final CountDownLatch latch = new CountDownLatch(3);
    // Create an executor with a thread pool containing 8 threads. A Spanner database change watcher
    // will normally create a thread pool containing as many threads as there are tables being
    // watched. This guarantees that each change watcher for a table will always have a thread
    // available to use to poll for changes. Using an executor with a smaller number of threads will
    // consume less resources, but could cause a delay in the delivery of change events. All changes
    // will eventually be reported.
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(8);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId)
            .allTables()
            .setExecutor(executor)
            .build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
    // An executor that is passed in to the change watcher is not managed by the watcher and must be
    // shutdown by the owner.
    executor.shutdown();
  }

  /**
   * Watch a table that contains a sharding column with one watcher for each shard value. The sample
   * assumes that the given table has the following structure:
   *
   * <pre>{@code
   * CREATE TABLE MY_TABLE (
   *   ID            INT64,
   *   NAME          STRING(MAX),
   *   SHARD_ID      STRING(MAX),
   *   LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)
   * ) PRIMARY KEY (ID);
   *
   * CREATE INDEX IDX_MY_TABLE_SHARD_ID ON MY_TABLE (SHARD_ID, LAST_MODIFIED_AT DESC);
   * }</pre>
   */
  public static void watchTableWithShardingExample(
      String project, // "my-project"
      String instance, // "my-instance"
      String database, // "my-database"
      String table // "MY_TABLE"
      ) throws InterruptedException {
    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    DatabaseId databaseId = DatabaseId.of(project, instance, database);
    TableId tableId = TableId.of(databaseId, table);
    final CountDownLatch latch = new CountDownLatch(3);

    // The table has two possible shard values.
    ImmutableList<String> shards = ImmutableList.of("EAST", "WEST");
    // We create one watcher for each shard.
    List<SpannerTableChangeWatcher> watchers = new LinkedList<>();
    for (String shard : shards) {
      SpannerTableChangeWatcher watcher =
          SpannerTableTailer.newBuilder(spanner, tableId)
              .setShardProvider(FixedShardProvider.create("SHARD_ID", shard))
              .build();
      watcher.addCallback(
          new RowChangeCallback() {
            @Override
            public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
              System.out.printf(
                  "Received change for table %s: %s%n", table, row.asStruct().toString());
              latch.countDown();
            }
          });
      watcher.startAsync();
      watchers.add(watcher);
    }
    // Wait for all watchers to have started.
    for (SpannerTableChangeWatcher watcher : watchers) {
      watcher.awaitRunning();
    }
    System.out.println("Started change watcher");

    // Wait until we have received 3 changes.
    latch.await();
    System.out.println("Received 3 changes, stopping change watcher");
    // Stop the pollers and wait for them to release all resources.
    for (SpannerTableChangeWatcher watcher : watchers) {
      watcher.stopAsync();
    }
    for (SpannerTableChangeWatcher watcher : watchers) {
      watcher.awaitTerminated();
    }
  }

  /**
   * Watch a table using an automatic time based sharding algorithm. The sample assumes that the
   * given table has the following structure:
   *
   * <pre>{@code
   * CREATE TABLE MY_TABLE (
   *   ID            INT64,
   *   NAME          STRING(MAX),
   *   SHARD_ID      STRING(MAX),
   *   LAST_MODIFIED TIMESTAMP OPTIONS (allow_commit_timestamp=true)
   * ) PRIMARY KEY (ID);
   *
   * CREATE INDEX IDX_MY_TABLE_SHARD_ID ON MY_TABLE (SHARD_ID, LAST_MODIFIED_AT DESC);
   * }</pre>
   */
  public static void watchTableWithTimebasedShardProviderExample(
      String project, // "my-project"
      String instance, // "my-instance"
      String database, // "my-database"
      String table // "MY_TABLE"
      ) throws InterruptedException {
    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    TableId tableId = TableId.of(DatabaseId.of(project, instance, database), table);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerTableChangeWatcher watcher =
        SpannerTableTailer.newBuilder(spanner, tableId)
            // Automatically create a new shard id for each day.
            .setShardProvider(TimebasedShardProvider.create("SHARD_ID", Interval.DAY))
            .build();
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Received change for table %s: %s%n", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.println("Started change watcher");

    // Any client that writes to the table must not only update the actual data and the commit
    // timestamp column, but also the value in the SHARD_ID column using the expression that
    // corresponds with the chosen automatic sharding interval.
    DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(project, instance, database));

    // Write data using mutations. We first need to get the shard id that we should use for the
    // change.
    TimebasedShardId currentShardId = Interval.DAY.getCurrentShardId(client.singleUse());
    client.write(
        ImmutableList.of(
            Mutation.newInsertBuilder(table)
                .set("ID")
                .to(1L)
                .set("NAME")
                .to("Name 1")
                .set("SHARD_ID")
                .to(currentShardId.getValue())
                .set("LAST_MODIFIED")
                .to(Value.COMMIT_TIMESTAMP)
                .build()));

    // Write data using DML.
    client
        .readWriteTransaction()
        .run(
            new TransactionCallable<Void>() {
              @Override
              public Void run(TransactionContext transaction) throws Exception {
                Statement statement1 =
                    Statement.newBuilder(
                            String.format(
                                "INSERT INTO `%s`\n"
                                    + "(ID, NAME, SHARD_ID, LAST_MODIFIED)\n"
                                    + "VALUES (@id, @name, %s, PENDING_COMMIT_TIMESTAMP())",
                                // getShardIdExpression() returns the function that computes the
                                // current shard id. Including this directly in the DML statement
                                // instead of fetching it from the database first saves us a
                                // round-trip to the database.
                                table, Interval.DAY.getShardIdExpression()))
                        .bind("id")
                        .to(2L)
                        .bind("name")
                        .to("Name 2")
                        .build();
                Statement statement2 =
                    statement1.toBuilder().bind("id").to(3L).bind("name").to("Name 3").build();
                transaction.batchUpdate(ImmutableList.of(statement1, statement2));
                return null;
              }
            });

    // Wait until we have received 3 changes.
    latch.await();
    System.out.println("Received 3 changes, stopping change watcher");
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
  }
}
