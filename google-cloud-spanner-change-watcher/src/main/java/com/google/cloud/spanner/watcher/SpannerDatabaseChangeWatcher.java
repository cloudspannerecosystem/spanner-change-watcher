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

import com.google.api.core.ApiFuture;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import java.util.Collection;

/** Interface for capturing changes to a set of tables in a Spanner database. */
public interface SpannerDatabaseChangeWatcher {
  /** Returns the ids of the tables that are monitored by this watcher. */
  Collection<TableId> getTables();

  /** Returns the watcher for the given table. */
  SpannerTableChangeWatcher getCapturer(TableId table);

  /** Run this watcher and report all changed rows to the given {@link RowChangeCallback}. */
  void start(RowChangeCallback callback);

  /** Stop this watcher. */
  ApiFuture<Void> stopAsync();
}
