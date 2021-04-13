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
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncResultSet.ReadyCallback;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerUtils.LogRecordBuilder;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.threeten.bp.Duration;

/**
 * Implementation of the {@link SpannerTableChangeWatcher} interface that continuously polls a table
 * for changes based on a commit timestamp column in the table.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * String instance = "my-instance";
 * String database = "my-database";
 * String table = "MY_TABLE";
 *
 * Spanner spanner = SpannerOptions.getDefaultInstance().getService();
 * TableId tableId =
 *     TableId.of(DatabaseId.of(SpannerOptions.getDefaultProjectId(), instance, database), table);
 * SpannerTableChangeWatcher watcher = SpannerTableTailer.newBuilder(spanner, tableId).build();
 * watcher.addCallback(
 *     new RowChangeCallback() {
 *       @Override
 *       public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
 *         System.out.printf(
 *             "Received change for table %s: %s%n", table, row.asStruct().toString());
 *       }
 *     });
 * watcher.startAsync().awaitRunning();
 * }</pre>
 */
public class SpannerTableTailer extends AbstractApiService implements SpannerTableChangeWatcher {
  static final Logger logger = Logger.getLogger(SpannerTableTailer.class.getName());
  static final long MAX_WITH_QUERY_LIMIT = 100_000L;
  static final int MAX_WITH_QUERY_SHARD_VALUES = 100;
  static final int DEFAULT_FALLBACK_TO_WITH_QUERY_SECONDS = 60;
  static final long DEFAULT_LIMIT = 10_000L;
  static final String POLL_QUERY = "SELECT *\nFROM %s\nWHERE `%s`%s@prevCommitTimestamp";
  static final String POLL_QUERY_ORDER_BY = "\nORDER BY `%s`, %s\nLIMIT @limit";

  /** Builder for a {@link SpannerTableTailer}. */
  public static class Builder {
    private final Spanner spanner;
    private final TableId table;
    private String tableHint = "";
    private long limit = DEFAULT_LIMIT;
    private int fallbackToWithQuerySeconds = DEFAULT_FALLBACK_TO_WITH_QUERY_SECONDS;
    private ShardProvider shardProvider;
    private CommitTimestampRepository commitTimestampRepository;
    private Duration pollInterval = Duration.ofSeconds(1L);
    private ScheduledExecutorService executor;
    private String commitTimestampColumn;

    private Builder(Spanner spanner, TableId table) {
      this.spanner = Preconditions.checkNotNull(spanner);
      this.table = Preconditions.checkNotNull(table);
      this.commitTimestampRepository =
          SpannerCommitTimestampRepository.newBuilder(spanner, table.getDatabaseId()).build();
    }

    /**
     * Sets an optional table hint to use for the poll query. This could for example be a
     * `@{FORCE_INDEX=Idx_SecondaryIndex}` hint to force the usage of a specific secondary index.
     */
    public Builder setTableHint(String tableHint) {
      this.tableHint = Preconditions.checkNotNull(tableHint);
      return this;
    }

    /**
     * Sets the maximum number of changes to fetch for each poll query. Default is 10,000. If <code>
     * limit</code> number of changes are found during a poll, a new poll will be scheduled directly
     * instead of after {@link #setPollInterval(Duration)}.
     *
     * <p>Setting this limit to a lower value instructs the {@link SpannerTableTailer} to execute
     * more poll queries more quickly if it is not able to keep up with the modifications in the
     * table that it is watching. Setting it too low can limit the overall throughput of the {@link
     * SpannerTableTailer} as it cannot fetch enough rows in each poll.
     *
     * <p>Recommended values are between 1,000 and 10,000.
     */
    public Builder setLimit(long limit) {
      this.limit = limit;
      return this;
    }

    /**
     * A {@link SpannerTableTailer} that uses a {@link FixedShardProvider} with an array of values
     * will by default execute a simple poll query that looks like this:
     *
     * <pre>{@code
     * SELECT *
     * FROM Table
     * WHERE LastModified > @lastSeenCommitTimestamp AND ShardId IS NOT NULL AND ShardId IN UNNEST(@shardIds)
     * ORDER BY LastModified, PrimaryKeyCol1, PrimaryKeyCol2, ...
     * LIMIT @limit
     * }</pre>
     *
     * <p>In some specific cases these queries can also automatically be re-written to a query that
     * creates a WITH clause for each shard id. This can be more efficient, especially if there are
     * many old mutations that have not yet been reported by the {@link SpannerTableTailer}. Using
     * such a query comes with the expense of a higher memory footprint on Cloud Spanner. This query
     * is therefore only used if the {@link SpannerTableTailer} is more than 60 seconds behind. This
     * default limit of 60 seconds can be increased or decreased with this setting.
     *
     * <p>Recommended values are between 10 seconds and 24 hours. Setting the limit to multiple
     * hours means that the WITH query should only be used when the {@link SpannerTableTailer} is
     * started after having been stopped for a while, while modifications to the watched table
     * continued to be applied.
     */
    public Builder setFallbackToWithQuerySeconds(int seconds) {
      this.fallbackToWithQuerySeconds = seconds;
      return this;
    }

    /** Sets the {@link ShardProvider} that this {@link SpannerTableTailer} should use. */
    public Builder setShardProvider(ShardProvider provider) {
      this.shardProvider = provider;
      return this;
    }

    /**
     * Sets the {@link CommitTimestampRepository} to use with this {@link SpannerTableTailer}.
     *
     * <p>If none is set, it will default to a {@link SpannerCommitTimestampRepository} which stores
     * the last seen commit timestamp in a table named LAST_SEEN_COMMIT_TIMESTAMPS. The table will
     * be created if it does not yet exist.
     */
    public Builder setCommitTimestampRepository(CommitTimestampRepository repository) {
      this.commitTimestampRepository = Preconditions.checkNotNull(repository);
      return this;
    }

    /** Sets the poll interval for the table. Defaults to 1 second. */
    public Builder setPollInterval(Duration interval) {
      this.pollInterval = Preconditions.checkNotNull(interval);
      return this;
    }

    /**
     * Sets the executor to use to poll the table and to execute the {@link RowChangeCallback}s.
     * Defaults to a single daemon threaded executor that is exclusively used for this {@link
     * SpannerTableTailer}.
     */
    public Builder setExecutor(ScheduledExecutorService executor) {
      this.executor = Preconditions.checkNotNull(executor);
      return this;
    }

    /**
     * <strong>This should only be set if your table contains more than one commit timestamp
     * column.</strong>
     *
     * <p>Sets the commit timestamp column to use. It is only necessary to set this property if the
     * table contains more than one column with the allow_commit_timestamp=true option. If the table
     * only contains one column that can hold a commit timestamp, the {@link SpannerTableTailer}
     * will find the column automatically.
     */
    public Builder setCommitTimestampColumn(String column) {
      this.commitTimestampColumn = column;
      return this;
    }

    /** Creates the {@link SpannerTableTailer}. */
    public SpannerTableTailer build() {
      return new SpannerTableTailer(this);
    }
  }

  public static Builder newBuilder(Spanner spanner, TableId table) {
    return new Builder(spanner, table);
  }

  private final Object lock = new Object();
  private ScheduledFuture<?> scheduled;
  private ApiFuture<Void> currentPollFuture;
  private Statement lastPollStatement;
  private final DatabaseClient client;
  private final TableId table;
  private final String tableHint;
  private final long limit;
  private final int fallbackToWithQuerySeconds;
  private final ShardProvider shardProvider;
  private final List<RowChangeCallback> callbacks = new LinkedList<>();
  private final CommitTimestampRepository commitTimestampRepository;
  private final Duration pollInterval;
  private final ScheduledExecutorService executor;
  private final boolean isOwnedExecutor;
  private Timestamp startedPollWithCommitTimestamp;
  private Timestamp lastSeenCommitTimestamp;
  private boolean lastPollReturnedChanges;
  private int mutationCountForLastCommitTimestamp;

  private String commitTimestampColumn;
  private List<String> primaryKeyColumns;
  private String primaryKeyColumnsList;
  private WithPollQueryBuilder withPollQueryBuilder;

  private SpannerTableTailer(Builder builder) {
    this.client = builder.spanner.getDatabaseClient(builder.table.getDatabaseId());
    this.table = builder.table;
    this.tableHint = Preconditions.checkNotNull(builder.tableHint);
    this.limit = builder.limit;
    this.fallbackToWithQuerySeconds = builder.fallbackToWithQuerySeconds;
    this.shardProvider = builder.shardProvider;
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
    if (builder.commitTimestampColumn != null) {
      this.commitTimestampColumn = builder.commitTimestampColumn;
    }
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

  /**
   * Returns the last poll statement of this tailer. This can be used to monitor the polling
   * activity by querying the SPANNER_SYS views for query statistics for this statement.
   */
  public Statement getLastPollStatement() {
    synchronized (lock) {
      return lastPollStatement;
    }
  }

  /**
   * Returns true if the last poll for changes actually returned changes. This can be used to
   * monitor the polling activity.
   */
  public boolean isLastPollReturnedChanges() {
    synchronized (lock) {
      return lastPollReturnedChanges;
    }
  }

  @Override
  protected void doStart() {
    logger.log(Level.INFO, "Starting watcher for table {0}", table);
    ApiFuture<String> commitTimestampColFut;
    if (commitTimestampColumn == null) {
      // Fetch the commit timestamp column from the database metadata.
      commitTimestampColFut = SpannerUtils.getTimestampColumn(client, table);
    } else {
      commitTimestampColFut = ApiFutures.immediateFuture(commitTimestampColumn);
    }
    commitTimestampColFut.addListener(
        new Runnable() {
          @Override
          public void run() {
            logger.log(Level.INFO, "Initializing watcher for table {0}", table);
            try {
              if (shardProvider == null) {
                lastSeenCommitTimestamp = commitTimestampRepository.get(table);
              } else {
                lastSeenCommitTimestamp =
                    commitTimestampRepository.get(table, shardProvider.getShardValue());
              }
              commitTimestampColumn = Futures.getUnchecked(commitTimestampColFut);
              primaryKeyColumns = SpannerUtils.getPrimaryKeyColumns(client, table).get();
              primaryKeyColumnsList = "`" + String.join("`, `", primaryKeyColumns) + "`";
              if (canUseWithQuery()) {
                withPollQueryBuilder = new WithPollQueryBuilder();
              }
              logger.log(Level.INFO, "Watcher started for table {0}", table);
              notifyStarted();
              scheduled =
                  executor.schedule(new SpannerTailerRunner(false), 0L, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
              logger.log(
                  LogRecordBuilder.of(
                      Level.WARNING, "Could not initialize watcher for table {0}", table, t));
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
      if (scheduled == null || scheduled.cancel(false)) {
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
    private final boolean scheduleNextPollDirectlyAfterThis;
    private int mutationCount;

    SpannerTailerCallback(boolean scheduleNextPollDirectlyAfterThis) {
      this.scheduleNextPollDirectlyAfterThis = scheduleNextPollDirectlyAfterThis;
    }

    private void scheduleNextPollOrStop(boolean reachedLimit) {
      // Store the last seen commit timestamp in the repository to ensure that the poller will pick
      // up at the right timestamp again if it is stopped or fails.
      if (lastSeenCommitTimestamp.compareTo(startedPollWithCommitTimestamp) > 0) {
        if (shardProvider == null) {
          commitTimestampRepository.set(table, lastSeenCommitTimestamp);
        } else {
          commitTimestampRepository.set(
              table, shardProvider.getShardValue(), lastSeenCommitTimestamp);
        }
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
                          new SpannerTailerRunner(reachedLimit),
                          reachedLimit || scheduleNextPollDirectlyAfterThis
                              ? 0
                              : pollInterval.toMillis(),
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
              scheduleNextPollOrStop(false);
              return CallbackResponse.DONE;
            }
          }
          switch (resultSet.tryNext()) {
            case DONE:
              synchronized (lock) {
                lastPollReturnedChanges = mutationCount > 0 || scheduleNextPollDirectlyAfterThis;
              }
              scheduleNextPollOrStop(mutationCount == limit);
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
                mutationCountForLastCommitTimestamp = 1;
              } else {
                mutationCountForLastCommitTimestamp++;
              }
              mutationCount++;
              break;
          }
        }
      } catch (Throwable t) {
        logger.log(
            LogRecordBuilder.of(
                Level.WARNING, "Error processing change set for table {0}", table, t));
        notifyFailed(t);
        scheduleNextPollOrStop(false);
        return CallbackResponse.DONE;
      }
    }
  }

  class SpannerTailerRunner implements Runnable {
    private final boolean finishCurrentCommitTimestamp;

    SpannerTailerRunner(boolean finishCurrentCommitTimestamp) {
      this.finishCurrentCommitTimestamp = finishCurrentCommitTimestamp;
    }

    @Override
    public void run() {
      logger.log(
          Level.FINE,
          String.format(
              "Starting poll for commit timestamp %s", lastSeenCommitTimestamp.toString()));
      startedPollWithCommitTimestamp = lastSeenCommitTimestamp;
      Statement.Builder statementBuilder;
      boolean shouldUseWithQuery;
      synchronized (lock) {
        shouldUseWithQuery =
            lastPollReturnedChanges
                && !finishCurrentCommitTimestamp
                && withPollQueryBuilder != null
                && (Timestamp.now().getSeconds() - lastSeenCommitTimestamp.getSeconds())
                    > fallbackToWithQuerySeconds;
      }
      if (shouldUseWithQuery) {
        statementBuilder = withPollQueryBuilder.withPollQuery;
      } else {
        String operator = finishCurrentCommitTimestamp ? "=" : ">";
        statementBuilder =
            Statement.newBuilder(
                String.format(
                    POLL_QUERY,
                    table.getSqlIdentifier() + tableHint,
                    commitTimestampColumn,
                    operator));
        if (shardProvider != null) {
          shardProvider.appendShardFilter(statementBuilder);
        }
        statementBuilder.append(
            String.format(POLL_QUERY_ORDER_BY, commitTimestampColumn, primaryKeyColumnsList));
        if (finishCurrentCommitTimestamp) {
          statementBuilder.append(String.format(" OFFSET %d", mutationCountForLastCommitTimestamp));
        }
      }
      Statement statement =
          statementBuilder
              .bind("prevCommitTimestamp")
              .to(lastSeenCommitTimestamp)
              .bind("limit")
              .to(limit)
              .build();
      if (!finishCurrentCommitTimestamp) {
        synchronized (lock) {
          lastPollStatement = statement;
        }
      }
      try (AsyncResultSet rs = client.singleUse().executeQueryAsync(statement)) {
        currentPollFuture =
            rs.setCallback(executor, new SpannerTailerCallback(finishCurrentCommitTimestamp));
      }
    }
  }

  /**
   * Returns true if this {@link SpannerTableTailer} can use a WITH query that creates a separate
   * SELECT statement for each shard that the tailer is watching.
   *
   * <p>A {@link SpannerTableTailer} can use a WITH query if it meets the following conditions:
   *
   * <ol>
   *   <li>It uses a {@link ShardProvider} that has a column name and an array as its (fixed) value.
   *   <li>The list of distinct shard values does not exceed 100.
   *   <li>It has a LIMIT value less than or equal to 10,000.
   * </ol>
   */
  boolean canUseWithQuery() {
    if (limit > MAX_WITH_QUERY_LIMIT) {
      return false;
    }
    if (shardProvider == null) {
      return false;
    }
    if (shardProvider.getColumnName() == null) {
      return false;
    }
    Value value = shardProvider.getShardValue();
    if (value == null) {
      return false;
    }
    if (value.getType().getCode() != Code.ARRAY) {
      return false;
    }
    List<ShardProvider> providers = splitArrayValueShardProvider(shardProvider);
    if (providers.size() > MAX_WITH_QUERY_SHARD_VALUES) {
      return false;
    }
    return true;
  }

  /**
   * {@link WithPollQueryBuilder} generates a WITH query that creates a separate SELECT statement
   * for each shard that the tailer is watching. Each SELECT statement will be held in memory by
   * Spanner. The SELECT statements in the WITH clauses therefore only contain the primary key and
   * commit timestamp values to keep the memory footprint as small as possible. An additional WITH
   * clause is added that contains the union of all the separate shards. The final query then
   * selects the union of all shards and joins this with the actual table.
   *
   * <p>Example:
   *
   * <pre>{@code
   * WITH
   * S0 AS (
   *   SELECT `SingerId`, `LastUpdated`
   *   FROM `Singers`@{FORCE_INDEX=`Idx_Singers_ShardId_LastUpdated`}
   *   WHERE `LastUpdated`>@lastSeenCommitTimestamp AND `ShardId`=@shard0
   *   ORDER BY `LastUpdated`
   *   LIMIT @limit
   * ),
   * S1 AS (
   *   SELECT `SingerId`, `LastUpdated`
   *   FROM `Singers`@{FORCE_INDEX=`Idx_Singers_ShardId_LastUpdated`}
   *   WHERE `LastUpdated`>@lastSeenCommitTimestamp AND `ShardId`=@shard1
   *   ORDER BY `LastUpdated`
   *   LIMIT @limit
   * ),
   * S2 AS (
   *   SELECT `SingerId`, `LastUpdated`
   *   FROM `Singers`@{FORCE_INDEX=`Idx_Singers_ShardId_LastUpdated`}
   *   WHERE `LastUpdated`>@lastSeenCommitTimestamp AND `ShardId`=@shard2
   *   ORDER BY `LastUpdated`
   *   LIMIT @limit
   * ),
   * AllShards AS (
   *   SELECT `SingerId`, FROM (
   *     SELECT *
   *     FROM S0
   *     UNION ALL
   *     SELECT *
   *     FROM S1
   *     UNION ALL
   *     SELECT *
   *     FROM S2
   *   )
   *   ORDER BY `LastUpdated`
   *   LIMIT @limit
   * )
   * SELECT `Singers`.*
   * FROM AllShards
   * INNER JOIN `Singers` ON `Singers`.`SingerId`=AllShards.`SingerId`
   * ORDER BY `LastUpdated`
   * LIMIT @limit
   * }</pre>
   */
  class WithPollQueryBuilder {
    final Statement.Builder withPollQuery;

    WithPollQueryBuilder() throws ExecutionException, InterruptedException {
      withPollQuery = generateWithPollQuery();
    }

    private Statement.Builder generateWithPollQuery() {
      List<ShardProvider> providers = splitArrayValueShardProvider(shardProvider);
      Statement.Builder builder = Statement.newBuilder("WITH\n");
      int index = 0;
      for (ShardProvider provider : providers) {
        builder.append("S").append(String.valueOf(index)).append(" AS (\n");
        builder.append("SELECT ").append(primaryKeyColumnsList).append(", ");
        builder.append(String.format("`%s`", commitTimestampColumn)).append("\n");
        builder.append("FROM ").append(table.getSqlIdentifier()).append(tableHint).append("\n");
        builder.append(String.format("WHERE `%s`>@prevCommitTimestamp", commitTimestampColumn));
        provider.appendShardFilter(builder);
        builder.append(
            String.format(POLL_QUERY_ORDER_BY, commitTimestampColumn, primaryKeyColumnsList));
        builder.append("\n),");
        index++;
      }

      builder.append("AllShards AS (\n");
      builder.append("SELECT ").append(primaryKeyColumnsList).append("\n");
      builder.append("FROM (\n");
      for (index = 0; index < providers.size(); index++) {
        if (index > 0) {
          builder.append("UNION ALL\n");
        }
        builder.append("SELECT *\n").append("FROM S").append(String.valueOf(index)).append("\n");
      }
      builder.append(")");
      builder.append(
          String.format(POLL_QUERY_ORDER_BY, commitTimestampColumn, primaryKeyColumnsList));
      builder.append("\n)\n");
      builder.append(String.format("SELECT %s.*\nFROM AllShards\n", table.getSqlIdentifier()));
      builder.append("INNER JOIN ").append(table.getSqlIdentifier());
      builder.append(" ON ");
      index = 0;
      for (String pk : primaryKeyColumns) {
        if (index > 0) {
          builder.append(" AND ");
        }
        builder.append(String.format("%s.`%s`=AllShards.`%s`", table.getSqlIdentifier(), pk, pk));
      }
      builder.append(
          String.format(POLL_QUERY_ORDER_BY, commitTimestampColumn, primaryKeyColumnsList));

      return builder;
    }
  }

  static List<ShardProvider> splitArrayValueShardProvider(ShardProvider shardProvider) {
    Value value = shardProvider.getShardValue();
    String column = shardProvider.getColumnName();
    Supplier<String> paramNameSupplier =
        new Supplier<String>() {
          int index = 0;

          @Override
          public String get() {
            return "param" + (index++);
          }
        };
    switch (value.getType().getArrayElementType().getCode()) {
      case BOOL:
        return value.getBoolArray().stream()
            .map(b -> new FixedShardProvider(column, Value.bool(b), paramNameSupplier.get()))
            .collect(Collectors.toList());
      case BYTES:
        return value.getBytesArray().stream()
            .map(b -> new FixedShardProvider(column, Value.bytes(b), paramNameSupplier.get()))
            .collect(Collectors.toList());
      case DATE:
        return value.getDateArray().stream()
            .map(b -> new FixedShardProvider(column, Value.date(b), paramNameSupplier.get()))
            .collect(Collectors.toList());
      case FLOAT64:
        return value.getFloat64Array().stream()
            .map(b -> new FixedShardProvider(column, Value.float64(b), paramNameSupplier.get()))
            .collect(Collectors.toList());
      case INT64:
        return value.getInt64Array().stream()
            .map(b -> new FixedShardProvider(column, Value.int64(b), paramNameSupplier.get()))
            .collect(Collectors.toList());
      case NUMERIC:
        return value.getNumericArray().stream()
            .map(b -> new FixedShardProvider(column, Value.numeric(b), paramNameSupplier.get()))
            .collect(Collectors.toList());
      case STRING:
        return value.getStringArray().stream()
            .map(b -> new FixedShardProvider(column, Value.string(b), paramNameSupplier.get()))
            .collect(Collectors.toList());
      case TIMESTAMP:
        return value.getTimestampArray().stream()
            .map(b -> new FixedShardProvider(column, Value.timestamp(b), paramNameSupplier.get()))
            .collect(Collectors.toList());
      case ARRAY:
      case STRUCT:
      default:
        throw new IllegalArgumentException();
    }
  }
}
