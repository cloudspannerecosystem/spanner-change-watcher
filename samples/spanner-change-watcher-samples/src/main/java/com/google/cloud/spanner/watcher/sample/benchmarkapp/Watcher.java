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
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.FixedShardProvider;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.sample.benchmarkapp.Main.BenchmarkOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Watcher {
  private static final int NUM_SHARDS = 39;
  private final BenchmarkOptions options;
  private final Spanner spanner;
  private final TableId tableId;
  final AtomicLong receivedChanges = new AtomicLong();

  public Watcher(BenchmarkOptions options) {
    this.options = options;
    spanner = SpannerOptions.newBuilder().build().getService();
    DatabaseId databaseId =
        DatabaseId.of(spanner.getOptions().getProjectId(), options.instance, options.database);
    tableId = TableId.of(databaseId, options.table);
  }

  public void run() {
    // Generate a list of the numbers -18 to 18 (inclusive).
    List<Long> allShards =
        Stream.iterate(-18L, n -> n + 1L).limit(NUM_SHARDS).collect(Collectors.toList());
    int shardsPerWatcher = NUM_SHARDS / options.numWatchers;
    List<ApiService> watchers = new ArrayList<>();
    for (int i = 0; i < options.numWatchers; i++) {
      List<Long> shardIds =
          allShards.subList(i * shardsPerWatcher, i * shardsPerWatcher + shardsPerWatcher);
      SpannerTableChangeWatcher watcher =
          SpannerTableTailer.newBuilder(spanner, tableId)
              .setShardProvider(FixedShardProvider.create("ShardId", Value.int64Array(shardIds)))
              .setTableHint(String.format("@{FORCE_INDEX=`%s`}", options.index))
              // .setLimit(options.limit)
              .build();
      watcher.addCallback(
          new RowChangeCallback() {
            public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
              receivedChanges.incrementAndGet();
            }
          });
      watchers.add(watcher.startAsync());
    }
  }
}
