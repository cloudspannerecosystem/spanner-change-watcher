/*
 * Copyright 2021 Google LLC
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

package com.google.cloud.spanner.watcher.sample.benchmarkapp;

import com.google.api.core.ApiService;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.FixedShardProvider;
import com.google.cloud.spanner.watcher.NotNullShardProvider;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.sample.benchmarkapp.Main.BenchmarkOptions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.threeten.bp.Duration;

public class Watcher {
  private static final int NUM_SHARDS = 37;
  private static final String FETCH_CPU_USAGE_LAST_MINUTE_QUERY =
      "SELECT AVG_CPU_SECONDS, EXECUTION_COUNT\n"
          + "FROM SPANNER_SYS.QUERY_STATS_TOP_MINUTE\n"
          + "WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), INTERVAL_END, MINUTE) <= 2 AND TEXT=@query\n"
          + "ORDER BY INTERVAL_END DESC\n"
          + "LIMIT 1";
  private static final String FETCH_MAX_POLL_LATENCY_QUERY =
      "SELECT MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), LAST_SEEN_COMMIT_TIMESTAMP, SECOND)) MAX_LATENCY\n"
          + "FROM LAST_SEEN_COMMIT_TIMESTAMPS \n"
          + "WHERE TABLE_NAME = @table\n"
          + "AND SHARD_ID_STRING IS NOT NULL AND SHARD_ID_STRING IN UNNEST(@shardIds)";
  private static final String FETCH_MAX_POLL_LATENCY_WITH_SHARDING_DISABLED_QUERY =
      "SELECT MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), LAST_SEEN_COMMIT_TIMESTAMP, SECOND)) MAX_LATENCY\n"
          + "FROM LAST_SEEN_COMMIT_TIMESTAMPS \n"
          + "WHERE TABLE_NAME = @table\n"
          + "AND SHARD_ID_STRING IS NULL";
  private static final String RESET_LAST_SEEN_COMMIT_TIMESTAMP_STATEMENT =
      "DELETE FROM LAST_SEEN_COMMIT_TIMESTAMPS\n"
          + "WHERE TABLE_NAME = @table\n"
          + "AND SHARD_ID_STRING IS NOT NULL AND SHARD_ID_STRING IN UNNEST(@shardIds)";
  private static final String RESET_LAST_SEEN_COMMIT_TIMESTAMP_WITH_SHARDING_DISABLED_STATEMENT =
      "DELETE FROM LAST_SEEN_COMMIT_TIMESTAMPS\n"
          + "WHERE TABLE_NAME = @table\n"
          + "AND SHARD_ID_STRING IS NULL";

  private final BenchmarkOptions options;
  private final Spanner spanner;
  private final DatabaseClient client;
  private final TableId tableId;
  private final Statement.Builder fetchCpuTimeStatement;
  private final Statement fetchMaxPollLatencyStatement;
  private ImmutableList<SpannerTableTailer> watchers;
  final AtomicLong receivedChanges = new AtomicLong();

  public Watcher(BenchmarkOptions options) {
    this.options = options;
    this.spanner = SpannerOptions.newBuilder().build().getService();
    client =
        spanner.getDatabaseClient(
            DatabaseId.of(spanner.getOptions().getProjectId(), options.instance, options.database));
    DatabaseId databaseId =
        DatabaseId.of(spanner.getOptions().getProjectId(), options.instance, options.database);
    this.tableId = TableId.of(databaseId, options.table);
    this.fetchCpuTimeStatement = Statement.newBuilder(FETCH_CPU_USAGE_LAST_MINUTE_QUERY);
    if (options.disableSharding) {
      this.fetchMaxPollLatencyStatement =
          Statement.newBuilder(FETCH_MAX_POLL_LATENCY_WITH_SHARDING_DISABLED_QUERY)
              .bind("table")
              .to(options.table)
              .build();
    } else {
      this.fetchMaxPollLatencyStatement =
          Statement.newBuilder(FETCH_MAX_POLL_LATENCY_QUERY)
              .bind("table")
              .to(options.table)
              .bind("shardIds")
              .toStringArray(generateShardIdsForCommitTimestampTable(options))
              .build();
    }
  }

  private static List<String> generateShardIdsForCommitTimestampTable(BenchmarkOptions options) {
    List<String> result = new ArrayList<>();
    List<List<Long>> shards = generateShardIds(options);
    for (int i = 0; i < options.numWatchers; i++) {
      List<Long> shardIds = shards.get(i);
      Value value = Value.int64Array(shardIds);
      result.add(
          value.getInt64Array().stream().map(b -> b.toString()).collect(Collectors.joining(",")));
    }
    return result;
  }

  private static List<List<Long>> generateShardIds(BenchmarkOptions options) {
    // Generate a list of the numbers -18 to 18 (inclusive).
    List<Long> allShards =
        Stream.iterate(-18L, n -> n + 1L).limit(NUM_SHARDS).collect(Collectors.toList());
    int shardsPerWatcher = NUM_SHARDS / options.numWatchers;
    int oneMoreShardUntilIndex = NUM_SHARDS % options.numWatchers;
    int shardStartIndex = 0;
    List<Long> assignedShards = new ArrayList<>();

    List<List<Long>> result = new ArrayList<>();
    for (int i = 0; i < options.numWatchers; i++) {
      int shardCount = shardsPerWatcher + (i < oneMoreShardUntilIndex ? 1 : 0);
      List<Long> shardIds = allShards.subList(shardStartIndex, shardStartIndex + shardCount);
      shardStartIndex += shardCount;
      result.add(shardIds);

      assignedShards.addAll(shardIds);
    }
    return result;
  }

  public void run() {
    if (options.resetLastSeenCommitTimestamp) {
      client
          .readWriteTransaction()
          .run(
              tx -> {
                if (options.disableSharding) {
                  return tx.executeUpdate(
                      Statement.newBuilder(
                              RESET_LAST_SEEN_COMMIT_TIMESTAMP_WITH_SHARDING_DISABLED_STATEMENT)
                          .bind("table")
                          .to(options.table)
                          .build());
                } else {
                  return tx.executeUpdate(
                      Statement.newBuilder(RESET_LAST_SEEN_COMMIT_TIMESTAMP_STATEMENT)
                          .bind("table")
                          .to(options.table)
                          .bind("shardIds")
                          .toStringArray(generateShardIdsForCommitTimestampTable(options))
                          .build());
                }
              });
    }

    List<SpannerTableTailer> watchers = new ArrayList<>();
    List<List<Long>> shards = generateShardIds(options);

    boolean useTableHint = !options.disableTableHint && !Strings.isNullOrEmpty(options.index);
    for (int i = 0; i < options.numWatchers; i++) {
      List<Long> shardIds = shards.get(i);
      SpannerTableTailer watcher =
          SpannerTableTailer.newBuilder(spanner, tableId)
              .setShardProvider(
                  options.disableSharding
                      ? (useTableHint ? NotNullShardProvider.create("ShardId") : null)
                      : FixedShardProvider.create("ShardId", Value.int64Array(shardIds)))
              .setTableHint(useTableHint ? String.format("@{FORCE_INDEX=`%s`}", options.index) : "")
              .setLimit(options.limit)
              .setFallbackToWithQuerySeconds(options.fallbackToWithQuery)
              .setPollInterval(Duration.parse(options.pollInterval))
              .build();
      watcher.addCallback(
          new RowChangeCallback() {
            public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
              receivedChanges.incrementAndGet();
            }
          });
      watcher.addListener(
          new Listener() {
            @Override
            public void failed(State from, Throwable failure) {
              failure.printStackTrace();
            }
          },
          MoreExecutors.directExecutor());
      watcher.startAsync();
      watchers.add(watcher);
    }

    for (ApiService watcher : watchers) {
      watcher.awaitRunning();
    }
    this.watchers = ImmutableList.copyOf(watchers);
  }

  public static class CpuTime {
    public final double avg;
    public final long executionCount;
    public final double total;
    public final int percentage;

    public CpuTime() {
      this.avg = 0;
      this.executionCount = 0;
      this.total = 0;
      this.percentage = 0;
    }

    private CpuTime(double avg, long executionCount) {
      this.avg = avg;
      this.executionCount = executionCount;
      this.total = avg * executionCount;
      this.percentage = (int) ((total * 100.0d) / 60.0d);
    }
  }

  /** Returns the average and total CPU time needed by the backend to execute the poll queries */
  public CpuTime fetchCpuTimeLastMinute() {
    if (this.watchers != null && !this.watchers.isEmpty()) {
      Statement lastPollStatement = watchers.get(0).getLastPollStatement();
      if (lastPollStatement != null) {
        try (ResultSet rs =
            client
                .singleUse()
                .executeQuery(
                    fetchCpuTimeStatement.bind("query").to(lastPollStatement.getSql()).build())) {
          if (rs.next()) {
            return new CpuTime(rs.getDouble("AVG_CPU_SECONDS"), rs.getLong("EXECUTION_COUNT"));
          }
        }
      }
    }
    return null;
  }

  public long fetchMaxPollLatency() {
    if (this.watchers != null && !this.watchers.isEmpty()) {
      int numWatchersWithChanges = 0;
      for (SpannerTableTailer watcher : watchers) {
        numWatchersWithChanges += watcher.isLastPollReturnedChanges() ? 1 : 0;
      }
      if (numWatchersWithChanges <= this.watchers.size() / 2) {
        return 0L;
      }
    }
    try (ResultSet rs = client.singleUse().executeQuery(fetchMaxPollLatencyStatement)) {
      if (rs.next()) {
        if (rs.isNull(0)) {
          return 0L;
        }
        return rs.getLong(0);
      }
    }
    return 0L;
  }

  public boolean isUsingWithQuery() {
    if (this.watchers != null && !this.watchers.isEmpty()) {
      Statement lastPollStatement = watchers.get(0).getLastPollStatement();
      if (lastPollStatement != null) {
        return lastPollStatement.getSql().startsWith("WITH");
      }
    }
    return false;
  }
}
