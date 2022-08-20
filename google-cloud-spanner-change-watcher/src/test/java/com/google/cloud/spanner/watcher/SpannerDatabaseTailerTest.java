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
import static org.junit.Assert.fail;

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Status;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SpannerDatabaseTailerTest extends AbstractMockServerTest {
  @Test
  public void testReceiveChanges() throws Exception {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, db)
            .allTables()
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, db)
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    final AtomicInteger receivedRows = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
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
    assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
  }

  @Test
  public void testTableNotFoundDuringInitialization() throws Exception {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, db)
            // Explicitly including a non-existing or invalid table will cause the
            // SpannerDatabaseTailer to fail during startup.
            .includeTables("Foo", "Bar", "NonExistingTable")
            .build();
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
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, db)
            .allTables()
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, db)
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    final AtomicInteger receivedRows = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
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
    latch.await(20L, TimeUnit.SECONDS);
    assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
    // Now simulate that the table has been deleted.
    Level currentLevel = SpannerTableTailer.logger.getLevel();
    try {
      SpannerTableTailer.logger.setLevel(Level.OFF);
      mockSpanner.putStatementResult(
          StatementResult.exception(
              getCurrentBarPollStatement(),
              Status.NOT_FOUND.withDescription("Table not found").asRuntimeException()));
      assertThat(res.get(5L, TimeUnit.SECONDS)).isTrue();
    } finally {
      SpannerTableTailer.logger.setLevel(currentLevel);
    }
    assertThat(tailer.state()).isEqualTo(State.FAILED);
  }

  @Test
  public void testCustomExecutor() throws Exception {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    // A single-threaded executor will cause the pollers for the different tables to be executed
    // sequentially. All changes should still be reported, but there could be a delay.
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, db)
            .allTables()
            .setExecutor(executor)
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, db)
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    final AtomicInteger receivedRows = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
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
    assertThat(receivedRows.get()).isEqualTo(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
    assertThat(executor.isShutdown()).isFalse();
    executor.shutdown();
  }

  @Test
  public void testBuildWithoutTables() {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    try {
      SpannerDatabaseTailer.newBuilder(spanner, db).includeTables(null).build();
      fail("missing expected exception");
    } catch (NullPointerException e) {
    }
  }

  @Test
  public void testTableNotFound() {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, db).includeTables("NonExistingTable").build();
    tailer.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            fail("Received unexpected row change");
          }
        });
    try {
      tailer.startAsync().awaitRunning();
      fail("missing expected exception");
    } catch (IllegalStateException e) {
      assertThat(e.getCause()).isInstanceOf(SpannerException.class);
      SpannerException se = (SpannerException) e.getCause();
      assertThat(se.getErrorCode()).isEqualTo(ErrorCode.NOT_FOUND);
    }
  }

  @Test
  public void testNoTablesFound() {
    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, db).allTables().except("Foo", "Bar").build();
    tailer.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            fail("Received unexpected row change");
          }
        });
    try {
      tailer.startAsync().awaitRunning();
      fail("missing expected exception");
    } catch (IllegalStateException e) {
      assertThat(e.getCause()).isInstanceOf(SpannerException.class);
      SpannerException se = (SpannerException) e.getCause();
      assertThat(se.getErrorCode()).isEqualTo(ErrorCode.NOT_FOUND);
    }
  }

  @Test
  public void testCustomCommitTimestampColumn() throws Exception {
    Timestamp ts = Timestamp.now();
    ResultSetMetadata metadata =
        RandomResultSetGenerator.METADATA.toBuilder()
            .setRowType(
                RandomResultSetGenerator.METADATA.getRowType().toBuilder()
                    .setFields(
                        RandomResultSetGenerator.METADATA.getRowType().getFieldsCount() - 1,
                        Field.newBuilder()
                            .setName("AlternativeCommitTS")
                            .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                            .build())
                    .build())
            .build();
    Statement statement =
        Statement.newBuilder(
                "SELECT *\n"
                    + "FROM `Foo`\n"
                    + "WHERE `AlternativeCommitTS`>@prevCommitTimestamp\n"
                    + "ORDER BY `AlternativeCommitTS`, `COL0`\n"
                    + "LIMIT @limit")
            .bind("limit")
            .to(SpannerTableTailer.DEFAULT_LIMIT)
            .bind("prevCommitTimestamp")
            .to(Timestamp.MIN_VALUE)
            .build();
    mockSpanner.putStatementResult(
        StatementResult.query(
            statement,
            new RandomResultSetGenerator(1)
                .generateWithFixedCommitTimestamp(ts).toBuilder().setMetadata(metadata).build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            statement.toBuilder().bind("prevCommitTimestamp").to(ts).build(),
            new RandomResultSetGenerator(0).generate().toBuilder().setMetadata(metadata).build()));

    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, db)
            .allTables()
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, db)
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .setCommitTimestampColumnFunction(
                (tableId) -> tableId.getTable().equals("Foo") ? "AlternativeCommitTS" : null)
            .build();
    final AtomicInteger receivedRows = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(1 + SELECT_BAR_ROW_COUNT);
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
    assertThat(receivedRows.get()).isEqualTo(1 + SELECT_BAR_ROW_COUNT);
  }
}
