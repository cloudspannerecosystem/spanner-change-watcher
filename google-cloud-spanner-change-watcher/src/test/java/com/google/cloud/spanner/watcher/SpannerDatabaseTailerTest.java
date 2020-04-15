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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SpannerDatabaseTailerTest extends AbstractMockServerTest {
  @Test
  public void testReceiveChanges() throws Exception {
    DatabaseId db = DatabaseId.of("p", "i", "d");
    try {
      SpannerDatabaseTailer tailer =
          SpannerDatabaseTailer.newBuilder(spanner, db)
              .setAllTables()
              .setPollInterval(Duration.ofSeconds(100L))
              .setCommitTimestampRepository(
                  SpannerCommitTimestampRepository.newBuilder(spanner, db)
                      .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                      .build())
              .build();
      final AtomicInteger receivedRows = new AtomicInteger();
      final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
      tailer.start(
          new RowChangeCallback() {
            @Override
            public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
              latch.countDown();
              receivedRows.incrementAndGet();
            }
          });
      latch.await(5L, TimeUnit.SECONDS);
      tailer.stopAsync().get();
      assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }
}
