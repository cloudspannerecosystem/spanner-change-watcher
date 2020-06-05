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
import com.google.cloud.spanner.Value;

/**
 * Implementation of {@link ShardProvider} that generates a {@link ShardId} based on the current
 * system time of Cloud Spanner. The generated shard id groups a set of commit timestamps together.
 * The {@link TimeBasedShardProvider} can only be used when all clients that write to the tables
 * that are being watched update the shard column with the most recent shard id for each update that
 * is written to the tables.
 *
 * @see Samples#watchTableWithTimebasedShardProviderExample(String, String, String, String) for an
 *     example on how to use this {@link ShardProvider}.
 */
public final class TimeBasedShardProvider implements ShardProvider {
  static final String PREVIOUS_SHARD_FUNCTION_FORMAT =
      "TO_BASE64(SHA512(FORMAT_TIMESTAMP('%s', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d SECOND), 'UTC')))";
  static final String CURRENT_SHARD_FUNCTION_FORMAT =
      "TO_BASE64(SHA512(FORMAT_TIMESTAMP('%s', CURRENT_TIMESTAMP(), 'UTC')))";

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
    private final String shardIdExpression;
    private final Statement currentShardIdStatement;

    private Interval(String dateFormat, boolean cyclic) {
      this.dateFormat = dateFormat;
      this.cyclic = cyclic;
      this.shardIdExpression = String.format(CURRENT_SHARD_FUNCTION_FORMAT, dateFormat);
      this.currentShardIdStatement = Statement.of("SELECT " + shardIdExpression);
    }

    /**
     * Fetches the current shard id from the database. It is the responsibility of all clients that
     * write data to tables that are being watched that the correct shard id is written to the
     * table. Failure to do so will cause the change to be missed by Spanner Change Watchers. The
     * shard id will remain constant during the complete interval that has been chosen. It is
     * therefore possible for clients to cache the current shard id value, as long as the client
     * renews the shard id once the interval has passed. The shard is calculated in UTC, which means
     * that for example a shard id based on {@link Interval#DAY} will change at the moment that the
     * current system time in UTC changes 23:59 to 00:00.
     */
    public String getCurrentShardId(ReadContext readContext) {
      try (ResultSet rs = readContext.executeQuery(currentShardIdStatement)) {
        if (rs.next()) {
          return rs.getString(0);
        }
        throw new IllegalStateException("Shard expression did not return any results");
      }
    }

    /**
     * Returns the SQL expression that calculates the current shard id. This expression can be
     * included in DML statements to automatically set the current shard id. See {@link
     * Samples#watchTableWithTimebasedShardProviderExample(String, String, String, String)} for an
     * example on how to use this.
     */
    public String getShardIdExpression() {
      return shardIdExpression;
    }

    String getDateFormat() {
      return dateFormat;
    }

    /**
     * Returns true if this interval is cyclic, i.e. the same shard id will be generated at later
     * moment again. {@link Interval#DAY_OF_WEEK}, {@link Interval#DAY_OF_MONTH} and {@link
     * Interval#DAY_OF_YEAR} are examples of cyclic intervals.
     */
    public boolean isCyclic() {
      return cyclic;
    }
  }

  /** Creates a {@link TimeBasedShardProvider} for the given database column and interval. */
  public static TimeBasedShardProvider create(String column, Interval interval) {
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

  @Override
  public Value getShardValue() {
    // A TimeBasedShardProvider does not have a fixed value that should be used to separate
    // different commit timestamps in the commit timestamp repository.
    return null;
  }
}
