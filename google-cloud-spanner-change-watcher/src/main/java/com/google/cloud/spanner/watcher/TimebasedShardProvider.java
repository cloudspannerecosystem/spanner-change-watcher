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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * Implementation of {@link ShardProvider} that generates a {@link ShardId} based on the current
 * system time of Cloud Spanner. The generated shard id groups a set of commit timestamps together.
 * The {@link TimebasedShardProvider} can only be used when all clients that write to the tables
 * that are being watched update the shard column with the most recent shard id for each update that
 * is written to the tables.
 *
 * @see Samples#watchTableWithTimebasedShardProviderExample(String, String, String, String) for an
 *     example on how to use this {@link ShardProvider}.
 */
public final class TimebasedShardProvider implements ShardProvider {
  static final String PREVIOUS_SHARD_FUNCTION_FORMAT =
      "TO_BASE64(SHA512(FORMAT_TIMESTAMP('%s', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d SECOND), 'UTC')))";
  static final String CURRENT_SHARD_FUNCTION_FORMAT =
      "TO_BASE64(SHA512(FORMAT_TIMESTAMP('%s', CURRENT_TIMESTAMP(), 'UTC')))";
  static final ZoneId UTC_ID = ZoneId.of("UTC");

  public enum Interval {
    /** Creates a shard for each unique date. */
    DAY("%F", false),
    /** Creates a shard for each week and year. */
    WEEK("%Y-%W", false),
    /** Creates a shard for each month and year. */
    MONTH("%yyyy-%m", false),
    /** Creates a shard for each year. */
    YEAR("%Y", false),

    /**
     * {@link #MINUTE_OF_HOUR} is mainly intended for testing purposes. Creates a cyclic shard for
     * each minute of an hour (0-59). Using {@link #MINUTE_OF_HOUR} means that the shard value will
     * change very frequently, and transactions that run longer than 1 minute will potentially
     * receive the wrong shard value.
     */
    MINUTE_OF_HOUR("%M", "mm", true, 45),
    /** Creates a cyclic shard for each hour of a day (0-23). */
    HOUR_OF_DAY("%H", "HH", true),

    /** Creates a cyclic shard for each weekday (1-7). */
    DAY_OF_WEEK("%u", true),
    /** Creates a cyclic shard for each day of month (1-31). */
    DAY_OF_MONTH("%d", true),
    /** Creates a cyclic shard for each week of year (1-53). */
    WEEK_OF_YEAR("%W", true),
    /** Creates a cyclic shard for each day of year (1-366). */
    DAY_OF_YEAR("%j", true);

    private final String cloudSpannerDateFormat;
    private final DateTimeFormatter refreshIntervalDateFormat;
    private final boolean cyclic;
    private final String shardIdExpression;
    private final Statement currentShardIdStatement;
    private int defaultPreviousShardSeconds;

    private Interval(String cloudSpannerDateFormat, boolean cyclic) {
      this(cloudSpannerDateFormat, "yyyy-MM-dd", cyclic, 60);
    }

    private Interval(String cloudSpannerDateFormat, String localDateFormat, boolean cyclic) {
      this(cloudSpannerDateFormat, localDateFormat, cyclic, 60);
    }

    private Interval(
        String cloudSpannerDateFormat,
        String javaDateFormat,
        boolean cyclic,
        int defaultPreviousShardSeconds) {
      this.cloudSpannerDateFormat = cloudSpannerDateFormat;
      this.refreshIntervalDateFormat = DateTimeFormatter.ofPattern(javaDateFormat, Locale.US);
      this.cyclic = cyclic;
      this.shardIdExpression = String.format(CURRENT_SHARD_FUNCTION_FORMAT, cloudSpannerDateFormat);
      this.currentShardIdStatement = Statement.of("SELECT " + shardIdExpression);
      this.defaultPreviousShardSeconds = defaultPreviousShardSeconds;
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
    public TimebasedShardId getCurrentShardId(ReadContext readContext) {
      LocalDateTime fetchTime = LocalDateTime.now(UTC_ID);
      try (ResultSet rs = readContext.executeQuery(currentShardIdStatement)) {
        if (rs.next()) {
          return new TimebasedShardId(Value.string(rs.getString(0)), this, fetchTime);
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
      return cloudSpannerDateFormat;
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

  /** A generated timebased shard id that has been fetched from Cloud Spanner. */
  public static final class TimebasedShardId {
    private final Value value;
    private final Interval interval;
    private final LocalDateTime fetchTime;

    private TimebasedShardId(Value value, Interval interval, LocalDateTime fetchTime) {
      this.value = value;
      this.interval = interval;
      this.fetchTime = fetchTime;
    }

    /**
     * Returns true if this {@link TimebasedShardId} should be refreshed based on the current system
     * time. This method depends on the current system time. If the system time is too much out of
     * sync with the actual time, it can return invalid values. A system time that is less than 10
     * seconds out of sync is not a problem.
     */
    public boolean shouldRefresh() {
      return !fetchTime
          .format(interval.refreshIntervalDateFormat)
          .equals(LocalDateTime.now(UTC_ID).format(interval.refreshIntervalDateFormat));
    }

    /**
     * Returns the {@link Value} that should be used to set on a {@link Mutation} when an
     * insert/update is sent to the database.
     */
    public Value getValue() {
      return value;
    }
  }

  /** Creates a {@link TimebasedShardProvider} for the given database column and interval. */
  public static TimebasedShardProvider create(String column, Interval interval) {
    return new TimebasedShardProvider(column, interval);
  }

  private final String column;
  private final String shardExpression;
  private final String previousShardExpression;
  private final String sqlAppendment;

  TimebasedShardProvider(String column, Interval interval) {
    this(column, interval, interval.defaultPreviousShardSeconds);
  }

  TimebasedShardProvider(String column, Interval interval, int previousShardSeconds) {
    this.column = column;
    this.shardExpression =
        String.format(CURRENT_SHARD_FUNCTION_FORMAT, interval.cloudSpannerDateFormat);
    this.previousShardExpression =
        String.format(
            PREVIOUS_SHARD_FUNCTION_FORMAT, interval.cloudSpannerDateFormat, previousShardSeconds);
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
  @Nullable
  public Value getShardValue() {
    // A TimeBasedShardProvider does not have a fixed value that should be used to separate
    // different commit timestamps in the commit timestamp repository.
    return null;
  }
}
