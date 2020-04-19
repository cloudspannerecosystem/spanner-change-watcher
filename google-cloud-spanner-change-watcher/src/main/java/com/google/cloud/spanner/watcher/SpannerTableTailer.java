/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.watcher;

import com.google.api.core.AbstractApiService;
import com.google.api.core.ApiFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncResultSet.ReadyCallback;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.threeten.bp.Duration;

/**
 * Implementation of the {@link SpannerTableChangeWatcher} interface that continuously polls a table
 * for changes based on a commit timestamp column in the table.
 */
public class SpannerTableTailer extends AbstractApiService implements SpannerTableChangeWatcher {
  static final Logger logger = Logger.getLogger(SpannerTableTailer.class.getName());
  static final String POLL_QUERY =
      "SELECT *\n" + "FROM %s\n" + "WHERE `%s`>@prevCommitTimestamp ORDER BY `%s`";

  public static class Builder {
    private final Spanner spanner;
    private final TableId table;
    private CommitTimestampRepository commitTimestampRepository;
    private Duration pollInterval = Duration.ofSeconds(1L);
    private ScheduledExecutorService executor;

    private Builder(Spanner spanner, TableId table) {
      this.spanner = Preconditions.checkNotNull(spanner);
      this.table = Preconditions.checkNotNull(table);
      this.commitTimestampRepository =
          SpannerCommitTimestampRepository.newBuilder(spanner, table.getDatabaseId()).build();
    }

    public Builder setCommitTimestampRepository(CommitTimestampRepository repository) {
      this.commitTimestampRepository = Preconditions.checkNotNull(repository);
      return this;
    }

    public Builder setPollInterval(Duration interval) {
      this.pollInterval = Preconditions.checkNotNull(interval);
      return this;
    }

    public Builder setExecutor(ScheduledExecutorService executor) {
      this.executor = Preconditions.checkNotNull(executor);
      return this;
    }

    public SpannerTableTailer build() {
      return new SpannerTableTailer(this);
    }
  }

  public static Builder newBuilder(Spanner spanner, TableId table) {
    return new Builder(spanner, table);
  }

  // TODO: Check and warn if the commit timestamp column is not part of an index.

  private final Object lock = new Object();
  private ScheduledFuture<?> scheduled;
  private ApiFuture<Void> currentPollFuture;
  private final DatabaseClient client;
  private final TableId table;
  private final List<RowChangeCallback> callbacks = new LinkedList<>();
  private final CommitTimestampRepository commitTimestampRepository;
  private final Duration pollInterval;
  private final ScheduledExecutorService executor;
  private final boolean isOwnedExecutor;
  private Timestamp startedPollWithCommitTimestamp;
  private Timestamp lastSeenCommitTimestamp;

  private String commitTimestampColumn;
  private Statement.Builder pollStatementBuilder;

  private SpannerTableTailer(Builder builder) {
    this.client = builder.spanner.getDatabaseClient(builder.table.getDatabaseId());
    this.table = builder.table;
    this.commitTimestampRepository = builder.commitTimestampRepository;
    this.pollInterval = builder.pollInterval;
    this.executor =
        builder.executor == null
            ? Executors.newScheduledThreadPool(
                1,
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("spanner-tailer-" + table + "-%d")
                    .build())
            : builder.executor;
    this.isOwnedExecutor = builder.executor == null;
  }

  @Override
  public void addCallback(RowChangeCallback callback) {
    Preconditions.checkState(state() == State.NEW);
    callbacks.add(callback);
  }

  @Override
  public TableId getTable() {
    return table;
  }

  @Override
  protected void doStart() {
    ApiFuture<String> commitTimestampColFut = SpannerUtils.getTimestampColumn(client, table);
    commitTimestampColFut.addListener(
        new Runnable() {
          @Override
          public void run() {
            try {
              lastSeenCommitTimestamp = commitTimestampRepository.get(table);
              commitTimestampColumn = Futures.getUnchecked(commitTimestampColFut);
              pollStatementBuilder =
                  Statement.newBuilder(
                      String.format(
                          POLL_QUERY,
                          table.getSqlIdentifier(),
                          commitTimestampColumn,
                          commitTimestampColumn));
              notifyStarted();
              scheduled = executor.schedule(new SpannerTailerRunner(), 0L, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
              notifyFailed(t);
            }
          }
        },
        executor);
  }

  @Override
  protected void notifyFailed(Throwable cause) {
    synchronized (lock) {
      if (isOwnedExecutor) {
        executor.shutdown();
      }
    }
    super.notifyFailed(cause);
  }

  @Override
  protected void doStop() {
    synchronized (lock) {
      if (scheduled.cancel(false)) {
        if (isOwnedExecutor) {
          executor.shutdown();
        }
        // The tailer has stopped if canceling was successful. Otherwise, notifyStopped() will be
        // called by the runner.
        notifyStopped();
      }
    }
  }

  class SpannerTailerCallback implements ReadyCallback {
    private void scheduleNextPollOrStop() {
      // Store the last seen commit timestamp in the repository to ensure that the poller will pick
      // up at the right timestamp again if it is stopped or fails.
      if (lastSeenCommitTimestamp.compareTo(startedPollWithCommitTimestamp) > 0) {
        commitTimestampRepository.set(table, lastSeenCommitTimestamp);
      }
      synchronized (lock) {
        if (state() == State.RUNNING) {
          // Schedule a new poll once this poll has finished completely.
          currentPollFuture.addListener(
              new Runnable() {
                @Override
                public void run() {
                  scheduled =
                      executor.schedule(
                          new SpannerTailerRunner(),
                          pollInterval.toMillis(),
                          TimeUnit.MILLISECONDS);
                }
              },
              MoreExecutors.directExecutor());
        } else {
          if (isOwnedExecutor) {
            executor.shutdown();
          }
          if (state() == State.STOPPING) {
            currentPollFuture.addListener(
                new Runnable() {
                  @Override
                  public void run() {
                    notifyStopped();
                  }
                },
                MoreExecutors.directExecutor());
          }
        }
      }
    }

    @Override
    public CallbackResponse cursorReady(AsyncResultSet resultSet) {
      try {
        while (true) {
          synchronized (lock) {
            if (state() != State.RUNNING) {
              scheduleNextPollOrStop();
              return CallbackResponse.DONE;
            }
          }
          switch (resultSet.tryNext()) {
            case DONE:
              scheduleNextPollOrStop();
              return CallbackResponse.DONE;
            case NOT_READY:
              return CallbackResponse.CONTINUE;
            case OK:
              Timestamp ts = resultSet.getTimestamp(commitTimestampColumn);
              Row row = new RowImpl(resultSet);
              for (RowChangeCallback callback : callbacks) {
                callback.rowChange(table, row, ts);
              }
              if (ts.compareTo(lastSeenCommitTimestamp) > 0) {
                lastSeenCommitTimestamp = ts;
              }
              break;
          }
        }
      } catch (Throwable t) {
        logger.log(Level.WARNING, "Error processing change set", t);
        notifyFailed(t);
        scheduleNextPollOrStop();
        return CallbackResponse.DONE;
      }
    }
  }

  class SpannerTailerRunner implements Runnable {
    @Override
    public void run() {
      logger.log(
          Level.FINE,
          String.format(
              "Starting poll for commit timestamp %s", lastSeenCommitTimestamp.toString()));
      startedPollWithCommitTimestamp = lastSeenCommitTimestamp;
      try (AsyncResultSet rs =
          client
              .singleUse()
              .executeQueryAsync(
                  pollStatementBuilder
                      .bind("prevCommitTimestamp")
                      .to(lastSeenCommitTimestamp)
                      .build())) {
        currentPollFuture = rs.setCallback(executor, new SpannerTailerCallback());
      }
    }
  }
}
