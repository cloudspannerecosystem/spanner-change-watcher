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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Value;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;

/**
 * Implementation of {@link ShardProvider} that returns a fixed shard id. This can be used in
 * combination with multiple change watchers, where each change watcher is responsible for watching
 * a specific segment of the table.
 *
 * <p>It can also be used to watch all segments of a table by setting the shard id to an array of
 * all possible values in the shard column.
 *
 * <p>Example usage in combination with a {@link SpannerTableTailer}:
 *
 * <pre>{@code
 * String shards = new String[] {"EAST", "WEST"};
 * for (String shard : shards) {
 *   SpannerTableTailer tailer =
 *       SpannerTableTailer.newBuilder(
 *               spanner, TableId.of(databaseId, "TABLE_NAME"))
 *           .setShardProvider(FixedShardProvider.create("SHARD_COLUMN", shard))
 *           .build();
 * }
 * }</pre>
 */
public class FixedShardProvider implements ShardProvider {
  private final String column;
  private final String parameterName;
  private final Value value;
  private final String sqlAppendment;

  public static FixedShardProvider create(String column, boolean value) {
    return new FixedShardProvider(column, Value.bool(value));
  }

  public static FixedShardProvider create(String column, ByteArray value) {
    return new FixedShardProvider(column, Value.bytes(value));
  }

  public static FixedShardProvider create(String column, Date value) {
    return new FixedShardProvider(column, Value.date(value));
  }

  public static FixedShardProvider create(String column, double value) {
    return new FixedShardProvider(column, Value.float64(value));
  }

  public static FixedShardProvider create(String column, long value) {
    return new FixedShardProvider(column, Value.int64(value));
  }

  public static FixedShardProvider create(String column, BigDecimal value) {
    return new FixedShardProvider(column, Value.numeric(value));
  }

  public static FixedShardProvider create(String column, String value) {
    return new FixedShardProvider(column, Value.string(value));
  }

  public static FixedShardProvider create(String column, Timestamp value) {
    return new FixedShardProvider(column, Value.timestamp(value));
  }

  public static FixedShardProvider create(String column, Value value) {
    return new FixedShardProvider(column, value);
  }

  FixedShardProvider(String column, Value value) {
    this(column, value, "shard");
  }

  FixedShardProvider(String column, Value value, String parameterName) {
    this.column = Preconditions.checkNotNull(column);
    this.parameterName = Preconditions.checkNotNull(parameterName);
    this.value = Preconditions.checkNotNull(value);
    if (value.getType().getCode() == Code.ARRAY) {
      this.sqlAppendment =
          String.format(
              " AND `%s` IS NOT NULL AND `%s` IN UNNEST(@%s)", column, column, parameterName);
    } else {
      this.sqlAppendment = String.format(" AND `%s`=@%s", column, parameterName);
    }
  }

  @Override
  public void appendShardFilter(Builder statementBuilder) {
    statementBuilder.append(sqlAppendment);
    statementBuilder.bind(parameterName).to(value);
  }

  @Override
  public Value getShardValue() {
    return value;
  }

  @Override
  public String getColumnName() {
    return column;
  }
}
