/*
 * Copyright 2021 Google LLC
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

import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Value;
import com.google.common.base.Preconditions;

/**
 * A {@link ShardProvider} that will add a <code>WHERE ShardId IS NOT NULL</code> clause to the
 * filter of a polling operation.
 *
 * <p>This {@link ShardProvider} can be used in combination with a NULL_FILTERED secondary index on
 * the ShardId and the commit timestamp columns. The value inserted into the ShardId column can be
 * anything as long as it is not null.
 *
 * <p>It is recommended to use this {@link ShardProvider} in combination with a table hint that
 * forces the use of the secondary index during polling. See below for an example.
 *
 * <p>The value of ShardId can be set to null by another process for rows that have not been updated
 * recently. This will keep the NULL_FILTERED secondary index small as all entries with a null value
 * in one of the index columns will be left out of the secondary index. See
 * https://cloud.google.com/spanner/docs/secondary-indexes#null-indexing-disable for more
 * information on NULL_FILTERED secondary indexes.
 *
 * <p>Example usage in combination with a {@link SpannerTableTailer}:
 *
 * <pre>{@code
 * SpannerTableTailer tailer =
 *     SpannerTableTailer.newBuilder(
 *             spanner, TableId.of(databaseId, "TABLE_NAME"))
 *         .setShardProvider(NotNullShardProvider.create("SHARD_COLUMN"))
 *         .setTableHint("@{FORCE_INDEX=IDX_SHARD_COLUMN_COMMIT_TIMESTAMP}")
 *         .build();
 * }</pre>
 */
public class NotNullShardProvider implements ShardProvider {
  private final String sqlAppendment;

  /** Creates a {@link NotNullShardProvider} that will filter on the given column. */
  public static NotNullShardProvider create(String column) {
    return new NotNullShardProvider(column);
  }

  private NotNullShardProvider(String column) {
    Preconditions.checkNotNull(column);
    this.sqlAppendment = String.format(" AND `%s` IS NOT NULL", column);
  }

  @Override
  public void appendShardFilter(Builder statementBuilder) {
    statementBuilder.append(sqlAppendment);
  }

  /**
   * Always returns <code>null</code> as the watcher will always search for changes in all shards.
   */
  @Override
  public Value getShardValue() {
    return null;
  }
}
