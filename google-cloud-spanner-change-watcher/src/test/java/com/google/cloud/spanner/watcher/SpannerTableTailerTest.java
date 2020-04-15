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
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SpannerTableTailerTest extends AbstractMockServerTest {
  @Test
  public void testReceiveChanges() throws Exception {
    DatabaseId db = DatabaseId.of("p", "i", "d");
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(spanner, TableId.of(db, "Foo"))
            .setPollInterval(Duration.ofSeconds(100L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, db)
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    final AtomicInteger receivedRows = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT);
    tailer.start(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            receivedRows.incrementAndGet();
            latch.countDown();
          }
        });
    latch.await(5L, TimeUnit.SECONDS);
    tailer.stopAsync().get();
    assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT);
  }

  private static final class TestChangeCallback implements RowChangeCallback {
    private final AtomicInteger receivedRows = new AtomicInteger();
    private CountDownLatch latch;
    private Timestamp lastSeenCommitTimestamp = Timestamp.MIN_VALUE;

    private TestChangeCallback(int initialCountDown) {
      latch = new CountDownLatch(initialCountDown);
    }

    private void setCountDown(int count) {
      latch = new CountDownLatch(count);
    }

    private CountDownLatch getLatch() {
      return latch;
    }

    @Override
    public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
      if (commitTimestamp.compareTo(lastSeenCommitTimestamp) > 0) {
        lastSeenCommitTimestamp = commitTimestamp;
      }
      receivedRows.incrementAndGet();
      latch.countDown();
    }
  }

  @Test
  public void testStressReceiveMultipleChanges() throws Exception {
    final Random random = new Random();
    for (int i = 0; i < 100; i++) {
      DatabaseId db = DatabaseId.of("p", "i", "d");
      SpannerTableTailer tailer =
          SpannerTableTailer.newBuilder(spanner, TableId.of(db, "Foo"))
              .setPollInterval(Duration.ofMillis(1L))
              .setCommitTimestampRepository(
                  SpannerCommitTimestampRepository.newBuilder(spanner, db)
                      .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                      .build())
              .build();
      TestChangeCallback callback = new TestChangeCallback(SELECT_FOO_ROW_COUNT);
      tailer.start(callback);
      CountDownLatch latch = callback.getLatch();
      latch.await(5L, TimeUnit.SECONDS);
      assertThat(callback.receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT);

      int expectedTotalChangeCount = SELECT_FOO_ROW_COUNT;
      for (int change = 0; change < 50; change++) {
        int numChanges = random.nextInt(10) + 1;
        expectedTotalChangeCount += numChanges;
        callback.setCountDown(numChanges);
        Timestamp lastSeenCommitTimestamp = callback.lastSeenCommitTimestamp;
        Timestamp nextCommitTimestamp =
            Timestamp.ofTimeSecondsAndNanos(
                lastSeenCommitTimestamp.getSeconds() + 1, lastSeenCommitTimestamp.getNanos());
        Statement pollStatement1 =
            SELECT_FOO_STATEMENT
                .toBuilder()
                .bind("prevCommitTimestamp")
                .to(lastSeenCommitTimestamp)
                .build();
        Statement pollStatement2 =
            SELECT_FOO_STATEMENT
                .toBuilder()
                .bind("prevCommitTimestamp")
                .to(nextCommitTimestamp)
                .build();
        mockSpanner.putStatementResults(
            StatementResult.query(
                pollStatement1,
                new RandomResultSetGenerator(numChanges)
                    .generateWithFixedCommitTimestamp(nextCommitTimestamp)),
            StatementResult.query(pollStatement2, new RandomResultSetGenerator(0).generate()));

        latch = callback.getLatch();
        latch.await(5L, TimeUnit.SECONDS);
        assertThat(callback.receivedRows.get()).isEqualTo(expectedTotalChangeCount);
      }
      tailer.stopAsync().get();
      // Restart mock server.
      stopServer();
      startStaticServer();
    }
  }
}
