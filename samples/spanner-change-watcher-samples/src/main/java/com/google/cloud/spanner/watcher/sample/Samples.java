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
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.CommitTimestampRepository;
import com.google.cloud.spanner.watcher.DatabaseClientWithChangeSets;
import com.google.cloud.spanner.watcher.DatabaseClientWithChangeSets.TransactionRunnerWithChangeSet;
import com.google.cloud.spanner.watcher.FixedShardProvider;
import com.google.cloud.spanner.watcher.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeSetPoller;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.cloud.spanner.watcher.SpannerTableChangeSetPoller;
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
import java.util.UUID;
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

  /**
   * Watch a database that contains one or more tables with multiple columns that have the option
   * allow_commit_timestamp=true. If a table for example contains a column that contains the commit
   * timestamp of the most recent update as well as an additional column that contains the commit
   * timestamp of a background job that runs every night, the watcher needs to be told which of the
   * commit timestamp columns to watch.
   *
   * <p>Example table:
   *
   * <pre>{@code
   * CREATE TABLE MY_TABLE (
   *   ID             INT64,
   *   NAME           STRING(MAX),
   *   LAST_MODIFIED  TIMESTAMP OPTIONS (allow_commit_timestamp=true),
   *   LAST_BATCH_JOB TIMESTAMP OPTIONS (allow_commit_timestamp=true),
   * ) PRIMARY KEY (ID);
   * }</pre>
   */
  public static void watchTableWithMultipleCommitTimestampColumns(
      String project, // "my-project"
      String instance, // "my-instance"
      String database // "my-database"
      ) throws InterruptedException {

    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    DatabaseId databaseId = DatabaseId.of(project, instance, database);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId)
            .allTables()
            // Use the commit timestamp column `LAST_MODIFIED` for all tables.
            .setCommitTimestampColumnFunction((tableId) -> "LAST_MODIFIED")
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
   * Watch a database that contains one or more tables that do not have a commit timestamp column.
   * Having a commit timestamp column in a data table can have a couple of disadvantages:
   *
   * <ol>
   *   <li>A table that has been updated using DML with a PENDING_COMMIT_TIMESTAMP() function call
   *       will no longer be readable during the remainder of the transaction (see {@link
   *       https://cloud.google.com/spanner/docs/commit-timestamp#dml})
   *   <li>A commit timestamp column alone should not be indexed. Instead, it should only be
   *       included in an index where the first part of the index is not a monotonically increasing
   *       value (see {@link https://cloud.google.com/spanner/docs/commit-timestamp#keys-indexes})
   * </ol>
   *
   * An application can use a {@link SpannerTableChangeSetPoller} or {@link
   * SpannerDatabaseChangeSetPoller} when the data table(s) do not contain a commit timestamp
   * column.
   *
   * <p>Example data model:
   *
   * <pre>{@code
   * -- This table keeps track of all read/write transactions.
   * CREATE TABLE CHANGE_SETS (
   *   CHANGE_SET_ID    STRING(MAX),
   *   COMMIT_TIMESTAMP TIMESTAMP OPTIONS (allow_commit_timestamp=true)
   * ) PRIMARY KEY (CHANGE_SET_ID);
   *
   * -- This is the data table and contains a reference to the table that keeps track of all
   * -- read/write transactions.
   * CREATE TABLE DATA_TABLE (
   *   ID             INT64,
   *   NAME           STRING(MAX),
   *   CHANGE_SET_ID  STRING(MAX)
   * ) PRIMARY KEY (ID);
   *
   * -- Create a secondary index on the CHANGE_SET_ID column. This can safely be done, as the
   * -- column will not contain monotonically increasing values.
   * CREATE INDEX IDX_DATA_TABLE_CHANGE_SET_ID ON DATA_TABLE (CHANGE_SET_ID);
   * }</pre>
   */
  public static void watchTableWithoutCommitTimestampColumn(
      String project, // "my-project"
      String instance, // "my-instance"
      String database // "my-database"
      ) throws InterruptedException {

    Spanner spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();
    DatabaseId databaseId = DatabaseId.of(project, instance, database);
    final CountDownLatch latch = new CountDownLatch(3);
    SpannerDatabaseChangeWatcher watcher =
        // SpannerDatabaseChangeSetPoller uses a CHANGE_SETS table (table name is configurable) to
        // monitor for changes to the data tables that reference this table.
        SpannerDatabaseChangeSetPoller.newBuilder(spanner, databaseId).allTables().build();
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

    // SpannerDatabaseChangeSetPoller requires all read/write transactions to add a record to the
    // CHANGE_SETS table for the change to be picked up. An application can handle this manually, or
    // an application can use the DatabaseClient wrapper that is supplied by the
    // spanner-change-watcher library.

    // Manually adding the CHANGE_SET record.
    DatabaseClient client = spanner.getDatabaseClient(databaseId);
    TransactionRunner runner = client.readWriteTransaction();
    String changeSetId = UUID.randomUUID().toString();
    runner.run(
        new TransactionCallable<Long>() {
          @Override
          public Long run(TransactionContext transaction) throws Exception {
            // Buffer a mutation for the CHANGE_SET table. This will automatically be sent together
            // with the Commit RPC when the transaction is committed.
            transaction.buffer(
                Mutation.newInsertOrUpdateBuilder("CHANGE_SETS")
                    .set("CHANGE_SET_ID")
                    .to(changeSetId)
                    .set("COMMIT_TIMESTAMP")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build());
            // Execute an update statement. This statement should use the unique change set id that
            // had been created for this transaction.
            return transaction.executeUpdate(
                Statement.newBuilder(
                        "INSERT INTO DATA_TABLE (ID, NAME, CHANGE_SET_ID) VALUES (@id, @name, @changeSet)")
                    .bind("id")
                    .to(1L)
                    .bind("name")
                    .to("One")
                    .bind("changeSet")
                    .to(changeSetId)
                    .build());
          }
        });

    // Adding the CHANGE_SET record by using the DatabaseClientWithChangeSets wrapper.
    DatabaseClientWithChangeSets clientWithChangeSets = DatabaseClientWithChangeSets.of(client);
    // Create a transaction runner. This runner will automatically generate a unique change set id
    // and it will automatically insert a record to the CHANGE_SETS table. All DML statements and
    // mutations in the transaction must use the change set id that is associated with the
    // transaction runner.
    TransactionRunnerWithChangeSet runnerWithChangeSet =
        clientWithChangeSets.readWriteTransaction();
    runnerWithChangeSet.run(
        new TransactionCallable<Long>() {
          @Override
          public Long run(TransactionContext transaction) throws Exception {
            // Add a mutation for the data table.
            transaction.buffer(
                Mutation.newInsertOrUpdateBuilder("DATA_TABLE")
                    .set("ID")
                    .to(2L)
                    .set("NAME")
                    .to("Two")
                    .set("CHANGE_SET_ID")
                    .to(runnerWithChangeSet.getChangeSetId())
                    .build());
            // Execute an update statement.
            return transaction.executeUpdate(
                Statement.newBuilder(
                        "INSERT INTO DATA_TABLE (ID, NAME, CHANGE_SET_ID) VALUES (@id, @name, @changeSet)")
                    .bind("id")
                    .to(3L)
                    .bind("name")
                    .to("Three")
                    .bind("changeSet")
                    .to(runnerWithChangeSet.getChangeSetId())
                    .build());
          }
        });

    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    watcher.stopAsync().awaitTerminated();
  }
}
