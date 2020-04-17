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

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SpannerTableTailerTest extends AbstractMockServerTest {
  private static final int STRESS_TEST_RUNS = 1;

  @Test
  public void testReceiveChanges() throws Exception {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    final AtomicInteger receivedRows = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT);
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(spanner, TableId.of(db, "Foo"))
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, db)
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    tailer.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            receivedRows.incrementAndGet();
            latch.countDown();
          }
        });
    tailer.startAsync().awaitRunning();
    latch.await(5L, TimeUnit.SECONDS);
    tailer.stopAsync().awaitTerminated();
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
  public void testTableNotFoundDuringInitialization() throws Exception {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(spanner, TableId.of(db, "NonExistingTable")).build();
    SettableApiFuture<Boolean> res = SettableApiFuture.create();
    tailer.addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            if (from != State.STARTING) {
              res.setException(new AssertionError("expected from State to be STARTING"));
            }
            res.set(Boolean.TRUE);
          }
        },
        MoreExecutors.directExecutor());
    tailer.startAsync();
    assertThat(res.get(5L, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testTableDeleted() throws Exception {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    final AtomicInteger receivedRows = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT);
    SpannerTableTailer tailer =
        SpannerTableTailer.newBuilder(spanner, TableId.of(db, "Foo"))
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, db)
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    tailer.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            receivedRows.incrementAndGet();
            latch.countDown();
          }
        });
    SettableApiFuture<Boolean> res = SettableApiFuture.create();
    tailer.addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            if (from != State.RUNNING) {
              res.setException(new AssertionError("expected from State to be RUNNING"));
            }
            res.set(Boolean.TRUE);
          }
        },
        MoreExecutors.directExecutor());
    tailer.startAsync().awaitRunning();
    latch.await(5L, TimeUnit.SECONDS);
    assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT);
    // Now simulate that the table has been deleted.
    Level currentLevel = SpannerTableTailer.logger.getLevel();
    try {
      SpannerTableTailer.logger.setLevel(Level.OFF);
      mockSpanner.putStatementResult(
          StatementResult.exception(
              getCurrentFooPollStatement(),
              Status.NOT_FOUND.withDescription("Table not found").asRuntimeException()));
      assertThat(res.get(5L, TimeUnit.SECONDS)).isTrue();
    } finally {
      SpannerTableTailer.logger.setLevel(currentLevel);
    }
    assertThat(tailer.state()).isEqualTo(State.FAILED);
  }

  @Test
  public void testStressReceiveMultipleChanges() throws Exception {
    final Random random = new Random();
    for (int i = 0; i < STRESS_TEST_RUNS; i++) {
      Spanner spanner = getSpanner();
      DatabaseId db = DatabaseId.of("p", "i", "d");
      TestChangeCallback callback = new TestChangeCallback(SELECT_FOO_ROW_COUNT);
      SpannerTableTailer tailer =
          SpannerTableTailer.newBuilder(spanner, TableId.of(db, "Foo"))
              .setPollInterval(Duration.ofMillis(1L))
              .setCommitTimestampRepository(
                  SpannerCommitTimestampRepository.newBuilder(spanner, db)
                      .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                      .build())
              .build();
      tailer.addCallback(callback);
      tailer.startAsync();
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
      tailer.stopAsync().awaitTerminated();
      if (i < (STRESS_TEST_RUNS - 1)) {
        // Restart mock server.
        stopServer();
        startStaticServer();
        setupResults();
      }
    }
  }
}
