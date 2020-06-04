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

import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

/**
 * Implementation of {@link ShardProvider} that generates a {@link ShardId} based on the current
 * time.
 */
public final class TimeBasedShardProvider implements ShardProvider {
  static final String PREVIOUS_SHARD_FUNCTION_FORMAT =
      "TO_BASE64(SHA512(FORMAT_TIMESTAMP('%s', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d SECOND))))";
  static final String CURRENT_SHARD_FUNCTION_FORMAT =
      "TO_BASE64(SHA512(FORMAT_TIMESTAMP('%s', CURRENT_TIMESTAMP())))";

  public enum Interval {
    DAY("%F", false),
    WEEK("%Y-%W", false),
    MONTH("%Y-%m", false),
    YEAR("%Y", false),

    DAY_OF_WEEK("%u", true),
    DAY_OF_MONTH("%d", true),
    WEEK_OF_YEAR("%W", true),
    DAY_OF_YEAR("%j", true);

    private final String dateFormat;
    private final boolean cyclic;
    private final Statement currentShardIdStatement;

    private Interval(String dateFormat, boolean cyclic) {
      this.dateFormat = dateFormat;
      this.cyclic = cyclic;
      this.currentShardIdStatement =
          Statement.of("SELECT " + String.format(CURRENT_SHARD_FUNCTION_FORMAT, dateFormat));
    }

    public String getCurrentShardId(ReadContext readContext) {
      try (ResultSet rs = readContext.executeQuery(currentShardIdStatement)) {
        if (rs.next()) {
          return rs.getString(0);
        }
        throw new IllegalStateException("Shard expression did not return any results");
      }
    }

    String getDateFormat() {
      return dateFormat;
    }

    public boolean isCyclic() {
      return cyclic;
    }
  }

  public static ShardProvider create(String column, Interval interval) {
    return new TimeBasedShardProvider(column, interval);
  }

  private final String column;
  private final String shardExpression;
  private final String previousShardExpression;
  private final String sqlAppendment;

  TimeBasedShardProvider(String column, Interval interval) {
    this(column, interval, 60);
  }

  TimeBasedShardProvider(String column, Interval interval, int previousShardSeconds) {
    this.column = column;
    this.shardExpression = String.format(CURRENT_SHARD_FUNCTION_FORMAT, interval.dateFormat);
    this.previousShardExpression =
        String.format(PREVIOUS_SHARD_FUNCTION_FORMAT, interval.dateFormat, previousShardSeconds);
    this.sqlAppendment =
        String.format(
            " AND `%s` IN (%s, %s)",
            this.column, this.shardExpression, this.previousShardExpression);
  }

  @Override
  public void appendShardFilter(Statement.Builder statement) {
    statement.append(sqlAppendment);
  }
}
