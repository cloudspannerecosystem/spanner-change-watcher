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
import com.google.cloud.spanner.Type;

/**
 * Implementation of {@link ShardProvider} that returns a fixed shard id. This can be used in
 * combination with multiple change watchers, where each change watcher is responsible for watching
 * a specific segment of the table. Each watcher should also have its own {@link
 * CommitTimestampRepository}.
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
 *           .setCommitTimestampRepository(
 *               SpannerCommitTimestampRepository.newBuilder(spanner, databaseId)
 *                   .setCommitTimestampsTable("LAST_SEEN_COMMIT_TIMESTAMPS_" + shard)
 *                   .build())
 *           .build();
 * }
 * }</pre>
 */
public final class FixedShardProvider implements ShardProvider {
  private final Type.Code type;
  private final Object value;
  private final String sqlAppendment;

  public static FixedShardProvider create(String column, boolean value) {
    return new FixedShardProvider(column, Type.Code.BOOL, value);
  }

  public static FixedShardProvider create(String column, ByteArray value) {
    return new FixedShardProvider(column, Type.Code.BYTES, value);
  }

  public static FixedShardProvider create(String column, Date value) {
    return new FixedShardProvider(column, Type.Code.DATE, value);
  }

  public static FixedShardProvider create(String column, Double value) {
    return new FixedShardProvider(column, Type.Code.FLOAT64, value);
  }

  public static FixedShardProvider create(String column, long value) {
    return new FixedShardProvider(column, Type.Code.INT64, value);
  }

  public static FixedShardProvider create(String column, String value) {
    return new FixedShardProvider(column, Type.Code.STRING, value);
  }

  public static FixedShardProvider create(String column, Timestamp value) {
    return new FixedShardProvider(column, Type.Code.TIMESTAMP, value);
  }

  private FixedShardProvider(String column, Type.Code type, Object value) {
    this.type = type;
    this.value = value;
    this.sqlAppendment = String.format(" AND `%s`=@shard", column);
  }

  @Override
  public void appendShardFilter(Builder statementBuilder) {
    statementBuilder.append(sqlAppendment);
    switch (type) {
      case BOOL:
        statementBuilder.bind("shard").to((Boolean) value);
        break;
      case BYTES:
        statementBuilder.bind("shard").to((ByteArray) value);
        break;
      case DATE:
        statementBuilder.bind("shard").to((Date) value);
        break;
      case FLOAT64:
        statementBuilder.bind("shard").to((Double) value);
        break;
      case INT64:
        statementBuilder.bind("shard").to((Long) value);
        break;
      case STRING:
        statementBuilder.bind("shard").to((String) value);
        break;
      case TIMESTAMP:
        statementBuilder.bind("shard").to((Timestamp) value);
        break;
      case ARRAY:
      case STRUCT:
      default:
        throw new IllegalStateException("Unknown or unsupported type: " + type);
    }
  }
}
