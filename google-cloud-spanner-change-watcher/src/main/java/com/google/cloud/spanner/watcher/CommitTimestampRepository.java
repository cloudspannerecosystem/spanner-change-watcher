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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import javax.annotation.Nullable;

/**
 * Interface for storing the last seen commit timestamp by a {@link SpannerTableChangeWatcher} to a
 * persistent repository.
 */
public interface CommitTimestampRepository {

  /** Returns the last seen commit timestamp for the given table. */
  Timestamp get(TableId table);

  /**
   * Returns the last seen commit timestamp for the given table and shard value. This method is
   * optional and the default implementation will throw {@link UnsupportedOperationException}.
   */
  default Timestamp get(TableId table, @Nullable Value shardValue)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  /** Sets the last seen commit timestamp for the given table. */
  void set(TableId table, Timestamp commitTimestamp);

  /**
   * Sets the last seen commit timestamp for the given table and shard value. This method is
   * optional and the default implementation will throw {@link UnsupportedOperationException}.
   */
  default void set(TableId table, @Nullable Value shardValue, Timestamp timestamp)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }
}
