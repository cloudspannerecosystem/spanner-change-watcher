/*
 * Copyright 2020 Google LLC
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

import com.google.api.core.AbstractApiService;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;
import org.threeten.bp.Duration;

/**
 * Implementation of the {@link SpannerTableChangeWatcher} interface that continuously polls a
 * change set table for new rows, and then polls an actual data table for any changes that was part
 * of the last seen change set. This requires all client applications to insert a new row to a
 * CHANGE_SETS table for each read/write transaction that is executed. While this does add an extra
 * write operation to each read/write transaction, it has two advantages:
 *
 * <ol>
 *   <li>It does not require each data table to have a column that stores the last commit timestamp.
 *       Commit timestamp columns that are filled using DML will prevent the same transaction from
 *       reading anything from the same table for the remainder of the transaction. See
 *       https://cloud.google.com/spanner/docs/commit-timestamp#dml.
 *   <li>Instead of having a commit timestamp column in each data table, the data tables contain a
 *       CHANGE_SET_ID column. This column can be of any type and can contain a random value. This
 *       makes the column easier to index than a commit timestamp column, as an index on a truly
 *       random id will not suffer from hotspots
 *       (https://cloud.google.com/spanner/docs/schema-design#primary-key-prevent-hotspots).
 * </ol>
 *
 * <p>Usage:
 *
 * <pre>{@code
 * String instance = "my-instance";
 * String database = "my-database";
 * String table = "MY_TABLE";
 *
 * Spanner spanner = SpannerOptions.getDefaultInstance().getService();
 * TableId tableId =
 *     TableId.of(DatabaseId.of(SpannerOptions.getDefaultProjectId(), instance, database), table);
 * SpannerTableChangeSetPoller watcher = SpannerTableChangeSetPoller.newBuilder(spanner, tableId).build();
 * watcher.addCallback(
 *     new RowChangeCallback() {
 *       @Override
 *       public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
 *         System.out.printf(
 *             "Received change for table %s: %s%n", table, row.asStruct().toString());
 *       }
 *     });
 * watcher.startAsync().awaitRunning();
 * }</pre>
 */
public class SpannerTableChangeSetPoller extends AbstractApiService
    implements SpannerTableChangeWatcher {
  static final Logger logger = Logger.getLogger(SpannerTableChangeSetPoller.class.getName());
  static final String POLL_QUERY = "SELECT *\nFROM %s\nWHERE `%s`=@changeSet";
  static final String DEFAULT_CHANGE_SET_TABLE_NAME = "CHANGE_SETS";
  static final String DEFAULT_CHANGE_SET_ID_COLUMN = "CHANGE_SET_ID";
  static final String DEFAULT_CHANGE_SET_COMMIT_TS_COLUMN = "COMMIT_TIMESTAMP";

  /** Builder for a {@link SpannerTableChangeSetPoller}. */
  public static class Builder {
    private final SpannerTableTailer.Builder tailerBuilder;
    private final Spanner spanner;
    private final TableId table;
    private String dataTableChangeSetIdColumn = DEFAULT_CHANGE_SET_ID_COLUMN;
    private String changeSetTableIdColumn = DEFAULT_CHANGE_SET_ID_COLUMN;

    Builder(Spanner spanner, TableId changeSetsTable, TableId table) {
      this.tailerBuilder =
          SpannerTableTailer.newBuilder(spanner, changeSetsTable)
              .setShardProvider(
                  new ShardProvider() {
                    @Override
                    public Value getShardValue() {
                      return Value.string(table.getFullName());
                    }

                    @Override
                    public void appendShardFilter(
                        com.google.cloud.spanner.Statement.Builder statementBuilder) {
                      // We do not filter the CHANGE_SETS poll queries.
                    }
                  });
      this.spanner = spanner;
      this.table = table;
    }

    /**
     * Sets the name of the column that contains the change set id in the data table (the table that
     * is being watched for changes).
     */
    public Builder setDataTableChangeSetIdColumn(String column) {
      this.dataTableChangeSetIdColumn = Preconditions.checkNotNull(column);
      return this;
    }

    /**
     * Sets the name of the column that contains the change set id in the change set table (the
     * table registering all transactions that should be watched).
     */
    public Builder setChangeSetTableIdColumn(String column) {
      this.changeSetTableIdColumn = Preconditions.checkNotNull(column);
      return this;
    }

    /**
     * Sets the {@link CommitTimestampRepository} to use for the {@link SpannerTableTailer} that is
     * watching the change set table.
     *
     * <p>If none is set, it will default to a {@link SpannerCommitTimestampRepository} which stores
     * the last seen commit timestamp in a table named LAST_SEEN_COMMIT_TIMESTAMPS. The table will
     * be created if it does not yet exist.
     */
    public Builder setCommitTimestampRepository(CommitTimestampRepository repository) {
      this.tailerBuilder.setCommitTimestampRepository(repository);
      return this;
    }

    /** Sets the poll interval for the table. Defaults to 1 second. */
    public Builder setPollInterval(Duration interval) {
      this.tailerBuilder.setPollInterval(interval);
      return this;
    }

    /** Sets the executor to use for polling for changes. */
    public Builder setExecutor(ScheduledExecutorService executor) {
      this.tailerBuilder.setExecutor(executor);
      return this;
    }

    /** Creates the {@link SpannerTableChangeSetPoller}. */
    public SpannerTableChangeSetPoller build() {
      return new SpannerTableChangeSetPoller(this);
    }
  }

  /**
   * Creates a new {@link SpannerTableChangeSetPoller.Builder} for the given table. The poller will
   * assume that all change sets are stored in a table called CHANGE_SETS.
   */
  public static Builder newBuilder(Spanner spanner, TableId table) {
    return new Builder(
        spanner, TableId.of(table.getDatabaseId(), DEFAULT_CHANGE_SET_TABLE_NAME), table);
  }

  /**
   * Creates a new {@link SpannerTableChangeSetPoller.Builder} for the given table and using the
   * specified change set table.
   */
  public static Builder newBuilder(Spanner spanner, TableId changeSetTable, TableId table) {
    return new Builder(spanner, changeSetTable, table);
  }

  private final SpannerTableChangeWatcher changeSetWatcher;
  private final TableId table;
  private final String dataTableChangeSetIdColumn;
  private final String changeSetTableIdColumn;
  private final List<RowChangeCallback> callbacks = new LinkedList<>();
  private final DatabaseClient client;

  private SpannerTableChangeSetPoller(Builder builder) {
    this.changeSetWatcher = builder.tailerBuilder.build();
    this.table = builder.table;
    this.dataTableChangeSetIdColumn = builder.dataTableChangeSetIdColumn;
    this.changeSetTableIdColumn = builder.changeSetTableIdColumn;
    this.client = builder.spanner.getDatabaseClient(builder.table.getDatabaseId());
  }

  @Override
  public TableId getTable() {
    return changeSetWatcher.getTable();
  }

  @Override
  public void addCallback(RowChangeCallback callback) {
    Preconditions.checkState(state() == State.NEW);
    callbacks.add(callback);
  }

  @Override
  protected void doStart() {
    changeSetWatcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            pollTableForChanges(row.getString(changeSetTableIdColumn), commitTimestamp);
          }
        });
    changeSetWatcher.addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            SpannerTableChangeSetPoller.this.notifyFailed(failure);
          }

          @Override
          public void running() {
            SpannerTableChangeSetPoller.this.notifyStarted();
          }

          @Override
          public void terminated(State from) {
            SpannerTableChangeSetPoller.this.notifyStopped();
          }
        },
        MoreExecutors.directExecutor());
    changeSetWatcher.startAsync();
  }

  void pollTableForChanges(String changeSet, Timestamp commitTimestamp) {
    Statement.Builder statementBuilder =
        Statement.newBuilder(
            String.format(POLL_QUERY, table.getSqlIdentifier(), dataTableChangeSetIdColumn));
    try (ResultSet rs =
        client.singleUse().executeQuery(statementBuilder.bind("changeSet").to(changeSet).build())) {
      while (rs.next()) {
        for (RowChangeCallback callback : callbacks) {
          callback.rowChange(table, new RowImpl(rs), commitTimestamp);
        }
      }
    }
  }

  @Override
  protected void doStop() {
    changeSetWatcher.stopAsync();
  }
}
