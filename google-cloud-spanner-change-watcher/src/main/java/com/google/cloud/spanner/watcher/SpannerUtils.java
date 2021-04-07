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
import com.google.api.core.InternalApi;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncResultSet.ReadyCallback;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.StructReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/** Utils for getting commonly needed schema information from a Spanner database. */
@InternalApi(
    "Public visibility for re-use by other spanner-change-watcher libraries. API-breaking changes without prior notice is possible.")
public class SpannerUtils {
  public static class LogRecordBuilder {
    public static LogRecord of(Level level, String msg, Throwable thrown) {
      LogRecord res = new LogRecord(level, msg);
      res.setThrown(thrown);
      return res;
    }

    public static LogRecord of(Level level, String msg, Object param, Throwable thrown) {
      LogRecord res = new LogRecord(level, msg);
      res.setThrown(thrown);
      res.setParameters(new Object[] {param});
      return res;
    }

    public static LogRecord of(Level level, String msg, Object param1, Object param2) {
      LogRecord res = new LogRecord(level, msg);
      res.setParameters(new Object[] {param1, param2});
      return res;
    }
  }

  /** Query for getting the column of a table that holds the commit timestamp. */
  @VisibleForTesting
  public static final String FIND_COMMIT_TIMESTAMP_COLUMN_QUERY =
      "SELECT COLUMN_NAME, OPTION_NAME, OPTION_VALUE\n"
          + "FROM INFORMATION_SCHEMA.COLUMN_OPTIONS\n"
          + "WHERE TABLE_CATALOG = @catalog\n"
          + "AND TABLE_SCHEMA = @schema\n"
          + "AND TABLE_NAME = @table";

  @VisibleForTesting
  static final String PK_QUERY =
      "SELECT COLUMN_NAME\n"
          + "FROM INFORMATION_SCHEMA.INDEX_COLUMNS\n"
          + "INNER JOIN INFORMATION_SCHEMA.INDEXES ON\n"
          + "      INDEX_COLUMNS.TABLE_CATALOG=INDEXES.TABLE_CATALOG AND INDEX_COLUMNS.TABLE_SCHEMA=INDEX_COLUMNS.TABLE_SCHEMA AND INDEX_COLUMNS.TABLE_NAME=INDEXES.TABLE_NAME\n"
          + "  AND INDEX_COLUMNS.INDEX_NAME=INDEXES.INDEX_NAME AND INDEXES.INDEX_TYPE='PRIMARY_KEY'\n"
          + "WHERE INDEX_COLUMNS.TABLE_CATALOG = @catalog\n"
          + "AND INDEX_COLUMNS.TABLE_SCHEMA = @schema\n"
          + "AND INDEX_COLUMNS.TABLE_NAME = @table\n"
          + "ORDER BY INDEX_COLUMNS.ORDINAL_POSITION";

  /** Returns the name of the commit timestamp column of the given table. */
  @InternalApi
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

  /** Returns the primary key columns of a table. */
  @InternalApi
  public static ApiFuture<List<String>> getPrimaryKeyColumns(DatabaseClient client, TableId table) {
    try (AsyncResultSet rs =
        client
            .singleUse()
            .executeQueryAsync(
                Statement.newBuilder(PK_QUERY)
                    .bind("catalog")
                    .to(table.getCatalog())
                    .bind("schema")
                    .to(table.getSchema())
                    .bind("table")
                    .to(table.getTable())
                    .build())) {
      return rs.toListAsync(
          new Function<StructReader, String>() {
            @Override
            public String apply(StructReader input) {
              return input.getString(0);
            }
          },
          MoreExecutors.directExecutor());
    }
  }

  /**
   * Creates a {@link Key} instance from an {@link Iterable} of primary key columns and a {@link
   * ResultSet} containing the data to use for the key.
   */
  @InternalApi
  public static Key buildKey(Iterable<String> pkColumns, ResultSet rs) {
    Key.Builder kb = Key.newBuilder();
    for (String pkCol : pkColumns) {
      switch (rs.getColumnType(pkCol).getCode()) {
        case BOOL:
          kb.append(rs.getBoolean(pkCol));
          break;
        case BYTES:
          kb.append(rs.getBytes(pkCol));
          break;
        case DATE:
          kb.append(rs.getDate(pkCol));
          break;
        case FLOAT64:
          kb.append(rs.getDouble(pkCol));
          break;
        case INT64:
          kb.append(rs.getLong(pkCol));
          break;
        case STRING:
          kb.append(rs.getString(pkCol));
          break;
        case TIMESTAMP:
          kb.append(rs.getTimestamp(pkCol));
          break;
        case STRUCT:
        case ARRAY:
          throw new IllegalArgumentException(
              "Invalid type for primary key: " + rs.getColumnType(pkCol));
        default:
          throw new IllegalArgumentException("Unknown type: " + rs.getColumnType(pkCol));
      }
    }
    return kb.build();
  }
}
