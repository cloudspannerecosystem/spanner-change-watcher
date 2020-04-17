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
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;

/** Interface for capturing changes to a single Spanner table. */
public interface SpannerTableChangeWatcher extends ApiService {

  /** Returns the id of the table that is monitored by this watcher. */
  TableId getTable();

  /** Row is passed in to the change callback and allows access to the most recent data. */
  interface Row extends StructReader {
    /** Convert the row to a {@link Struct}. */
    Struct asStruct();
  }

  /** Interface for receiving asynchronous callbacks when a row has been inserted or updated. */
  interface RowChangeCallback {
    /**
     * Called once for each detected insert or update of a row.
     *
     * @param table The table where the data was inserted or updated.
     * @param row The updated data of the row that was inserted or updated.
     * @param commitTimestamp The commit timestamp of the transaction that inserted or updated the
     *     row.
     */
    void rowChange(TableId table, Row row, Timestamp commitTimestamp);
  }

  /**
   * Adds a {@link RowChangeCallback} for this {@link SpannerTableChangeWatcher}. Callbacks may only
   * be added when the {@link #state()} of this {@link SpannerTableChangeWatcher} is {@link
   * State#NEW}
   */
  void addCallback(RowChangeCallback callback);
}
