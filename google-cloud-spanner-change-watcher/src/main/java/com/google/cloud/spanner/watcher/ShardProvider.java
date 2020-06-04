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

import com.google.cloud.spanner.Statement;

/**
 * Interface for providing a shard id for Spanner Table Change watchers. The shard id will be used
 * by the change watcher when querying the table for the most recent changes. This can be used to
 * prevent full table scans when polling a table.
 */
public interface ShardProvider {
  /**
   * Appends the required sharding filter to the given statement. This could be an SQL fragment or
   * one or more parameters or a combination of both.
   */
  void appendShardFilter(Statement.Builder statementBuilder);
}
