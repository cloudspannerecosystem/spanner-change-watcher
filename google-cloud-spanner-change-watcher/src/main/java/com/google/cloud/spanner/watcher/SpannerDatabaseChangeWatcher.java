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

import com.google.api.core.ApiService;
import com.google.api.core.ApiService.State;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import java.util.Collection;

/** Interface for capturing changes to a set of tables in a Spanner database. */
public interface SpannerDatabaseChangeWatcher extends ApiService {
  /** Returns the id of the database that is being monitored for changes. */
  DatabaseId getDatabaseId();

  /**
   * Returns the ids of the tables that are monitored by this watcher. This call can require the
   * {@link SpannerDatabaseChangeWatcher} to make a round-trip to the database to determine the
   * actual tables that are being monitored.
   */
  Collection<TableId> getTables();

  /**
   * Adds a {@link RowChangeCallback} for this {@link SpannerDatabaseChangeWatcher}. Callbacks may
   * only be added when the {@link #state()} of this {@link SpannerDatabaseChangeWatcher} is {@link
   * State#NEW}. Callbacks for one table will always be in order of commit timestamp, and only one
   * callback will be active at any time for a table. Callbacks for different tables may be called
   * in parallel, and there is no guarantee to the ordering of callbacks over multiple tables.
   */
  void addCallback(RowChangeCallback callback);
}
