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
import com.google.api.core.SettableApiFuture;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncResultSet.ReadyCallback;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

/** Utils for getting commonly needed schema information from a Spanner database. */
public class SpannerUtils {
  /** Query for getting the column of a table that holds the commit timestamp. */
  @VisibleForTesting
  public static final String FIND_COMMIT_TIMESTAMP_COLUMN_QUERY =
      "SELECT COLUMN_NAME, OPTION_NAME, OPTION_VALUE\n"
          + "FROM INFORMATION_SCHEMA.COLUMN_OPTIONS\n"
          + "WHERE TABLE_CATALOG = @catalog\n"
          + "AND TABLE_SCHEMA = @schema\n"
          + "AND TABLE_NAME = @table";

  /** Returns the name of the commit timestamp column of the given table. */
  public static ApiFuture<String> getTimestampColumn(DatabaseClient client, TableId table) {
    final SettableApiFuture<String> res = SettableApiFuture.create();
    try (AsyncResultSet rs =
        client
            .singleUse()
            .executeQueryAsync(
                Statement.newBuilder(FIND_COMMIT_TIMESTAMP_COLUMN_QUERY)
                    .bind("catalog")
                    .to(table.getCatalog())
                    .bind("schema")
                    .to(table.getSchema())
                    .bind("table")
                    .to(table.getTable())
                    .build())) {
      rs.setCallback(
          MoreExecutors.directExecutor(),
          new ReadyCallback() {
            @Override
            public CallbackResponse cursorReady(AsyncResultSet rs) {
              try {
                switch (rs.tryNext()) {
                  case NOT_READY:
                    return CallbackResponse.CONTINUE;
                  case OK:
                    if (rs.getString("OPTION_NAME").equals("allow_commit_timestamp")) {
                      if (rs.getString("OPTION_VALUE").equals("TRUE")) {
                        res.set(rs.getString("COLUMN_NAME"));
                        return CallbackResponse.DONE;
                      }
                    }
                  case DONE:
                }
              } catch (Throwable t) {
                res.setException(t);
                return CallbackResponse.DONE;
              }
              res.setException(
                  SpannerExceptionFactory.newSpannerException(
                      ErrorCode.INVALID_ARGUMENT,
                      String.format(
                          "Table %s does not contain a column with option allow_commit_timestamp=true",
                          table)));
              return CallbackResponse.DONE;
            }
          });
    }
    return res;
  }
}
