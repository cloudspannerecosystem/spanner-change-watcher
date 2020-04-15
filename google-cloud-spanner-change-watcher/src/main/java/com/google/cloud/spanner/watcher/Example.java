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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import java.util.concurrent.CountDownLatch;
import org.threeten.bp.Duration;

class Example {

  static void tableTailerExample() throws Exception {
    Spanner spanner = SpannerOptions.getDefaultInstance().getService();
    DatabaseId databaseId = DatabaseId.of("my-project", "my-instance", "my-database");
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(spanner, TableId.of(databaseId, "MY_TABLE"))
            // Poll every 100 milliseconds (default is 1 second)
            .setPollInterval(Duration.ofMillis(100L))
            // Use a specific commit timestamp repository.
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, databaseId)
                    // Create the LAST_SEEN_COMMIT_TIMESTAMPS table if it does not already exist.
                    .setCreateTableIfNotExists()
                    .build())
            .build();
    final CountDownLatch latch = new CountDownLatch(3);
    tailer.start(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf("Received change for table %s: %s", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    tailer.stopAsync().get();
  }

  static void databaseTailerExample() throws Exception {
    Spanner spanner = SpannerOptions.getDefaultInstance().getService();
    DatabaseId databaseId = DatabaseId.of("my-project", "my-instance", "my-database");
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, databaseId)
            // Watch all tables that have a commit timestamp column.
            .setAllTables()
            // Except for these tables.
            .excludeTables("TABLE1", "TABLE2")
            .build();
    final CountDownLatch latch = new CountDownLatch(3);
    tailer.start(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            System.out.printf("Received change for table %s: %s", table, row.asStruct().toString());
            latch.countDown();
          }
        });
    // Wait until we have received 3 changes.
    latch.await();
    // Stop the poller and wait for it to release all resources.
    tailer.stopAsync().get();
  }
}
