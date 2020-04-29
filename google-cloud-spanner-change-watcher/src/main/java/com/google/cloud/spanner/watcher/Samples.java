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

package com.google.cloud.spanner.watcher;

import com.google.api.core.ApiService;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.common.util.concurrent.MoreExecutors;
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
}
