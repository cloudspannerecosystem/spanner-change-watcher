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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.TableId;
import java.io.IOException;

/** Simple sample for using a {@link SpannerDatabaseChangeWatcher}. */
class SimpleChangeWatcherSample {

  public static void main(String[] args) throws InterruptedException, IOException {
    if (args.length != 2) {
      System.out.println(
          String.format(
              "Missing instanceId and databaseId. Usage: java %s <instanceId> <databaseId>",
              SimpleChangeWatcherSample.class.getName()));
      System.exit(1);
    }
    String instance = args[0];
    String database = args[1];
    SpannerOptions options = SpannerOptions.newBuilder().build();
    // Create a connection to a Spanner database.
    System.out.println(
        String.format(
            "Connecting to projects/%s/instances/%s/databases/%s...",
            options.getProjectId(), instance, database));
    Spanner spanner = options.getService();
    DatabaseId databaseId = DatabaseId.of(spanner.getOptions().getProjectId(), instance, database);

    System.out.println("Checking/creating sample database...");
    SampleData.createSampleDatabase(spanner, databaseId);

    // Create and start a SpannerDatabaseChangeWatcher.
    System.out.println("Starting change watcher...");
    SpannerDatabaseChangeWatcher watcher = createWatcher(spanner, databaseId);

    // Write some data to the database. This should then be written to the console.
    System.out.println("Writing data to Cloud Spanner...");
    SampleData.writeExampleData(spanner.getDatabaseClient(databaseId));
    // Wait a little to allow all data to be written, and the callback to write the data to the
    // console.
    Thread.sleep(10_000L);
    System.out.println("Finished writing test data...");

    // Wait for the user to hit <Enter> before exiting.
    System.out.println("The Database Change Watcher is still running in the background.");
    System.out.println("You can write additional data to the database.");
    System.out.println("This will cause the data to be written to this console.");
    System.out.println("Press <Enter> to close this application.");

    System.in.read();
    System.out.println("Closing change watcher...");
    watcher.stopAsync().awaitTerminated();
    System.out.println("Change watcher closed.");
  }

  /**
   * Creates and starts a {@link SpannerDatabaseChangeWatcher} for all the tables in the sample
   * database.
   */
  static SpannerDatabaseChangeWatcher createWatcher(Spanner spanner, DatabaseId databaseId) {
    // Create a change watcher for all tables in the database that have a commit timestamp column.
    SpannerDatabaseChangeWatcher watcher =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();
    // Add a callback to receive change notifications.
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf(
                "Change received for table %s: %s%n", table.getTable(), row.asStruct().toString());
          }
        });
    // Start the change watcher and wait until it is running.
    watcher.startAsync().awaitRunning();
    return watcher;
  }
}
