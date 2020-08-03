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

import com.google.api.client.util.Preconditions;
import com.google.api.core.AbstractApiService;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeSetPoller.Builder.TableExcluder;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeSetPoller.Builder.TableSelecter;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerUtils.LogRecordBuilder;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.threeten.bp.Duration;

/**
 * Implementation of the {@link SpannerDatabaseChangeWatcher} interface that continuously polls a
 * change set table for new rows, and then polls the actual data tables for any changes that was
 * part of the last seen change set. This requires all client applications to insert a new row to a
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
 * <p>Example usage for watching all tables in a database:
 *
 * <pre>{@code
 * String instance = "my-instance";
 * String database = "my-database";
 *
 * Spanner spanner = SpannerOptions.getDefaultInstance().getService();
 * DatabaseId databaseId = DatabaseId.of(SpannerOptions.getDefaultProjectId(), instance, database);
 * SpannerDatabaseChangeWatcher watcher =
 *     SpannerDatabaseChangeSetPoller.newBuilder(spanner, databaseId).allTables().build();
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
public class SpannerDatabaseChangeSetPoller extends AbstractApiService
    implements SpannerDatabaseChangeWatcher {
  private static final Logger logger =
      Logger.getLogger(SpannerDatabaseChangeSetPoller.class.getName());

  /** Builder for a {@link SpannerDatabaseChangeSetPoller}. */
  public interface Builder {
    /**
     * Sets a specific {@link CommitTimestampRepository} to use for the {@link SpannerTableTailer}s
     * that are watching the change set table.
     *
     * <p>If none is set, it will default to a {@link SpannerCommitTimestampRepository} which stores
     * the last seen commit timestamp in a table named LAST_SEEN_COMMIT_TIMESTAMPS. The table will
     * be created if it does not yet exist.
     */
    public Builder setCommitTimestampRepository(CommitTimestampRepository repository);

    /**
     * Sets the poll interval to use for this {@link SpannerDatabaseChangeSetPoller}. The default is
     * 1 second.
     */
    public Builder setPollInterval(Duration interval);

    /**
     * Sets a specific {@link ScheduledExecutorService} to use for this {@link
     * SpannerDatabaseChangeSetPoller}. This executor will be used to execute the poll queries on
     * the tables and to call the {@link RowChangeCallback}s. The default will use a {@link
     * ScheduledThreadPoolExecutor} with a core size equal to the number of tables that is being
     * monitored.
     */
    public Builder setExecutor(ScheduledExecutorService executor);

    /**
     * Interface for selecting the tables that should be monitored by a {@link
     * SpannerDatabaseChangeSetPoller}.
     */
    public interface TableSelecter {
      /**
       * Instructs the {@link SpannerDatabaseChangeSetPoller} to only emit changes for these
       * specific tables.
       */
      Builder includeTables(String firstTable, String... furtherTables);

      /**
       * Instructs the {@link SpannerDatabaseChangeSetPoller} to emit change events for all tables
       * in the database. Tables can be excluded by calling {@link TableExcluder#except(String...)}.
       */
      TableExcluder allTables();
    }

    /** Interface for excluding specific tables from a {@link SpannerDatabaseChangeSetPoller}. */
    public interface TableExcluder extends Builder {
      /**
       * Instructs the {@link SpannerDatabaseChangeSetPoller} to exclude these tables from change
       * events. This option can be used in combination with {@link TableSelecter#allTables()} to
       * include all tables except for a specfic list.
       */
      Builder except(String... tables);
    }

    /** Creates a {@link SpannerDatabaseChangeSetPoller} from this builder. */
    public SpannerDatabaseChangeSetPoller build();
  }

  /** Lists all tables with a change set id column. */
  static final String LIST_TABLE_NAMES_STATEMENT =
      "SELECT TABLE_NAME\n"
          + "FROM INFORMATION_SCHEMA.TABLES\n"
          + "WHERE TABLE_NAME NOT IN UNNEST(@excluded)\n"
          + "AND (@allTables=TRUE OR TABLE_NAME IN UNNEST(@included))\n"
          + "AND TABLE_CATALOG = @catalog\n"
          + "AND TABLE_SCHEMA = @schema\n"
          + "AND TABLE_NAME IN (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME=@changeSetIdColumn)";

  static class BuilderImpl implements TableSelecter, TableExcluder, Builder {
    private final Spanner spanner;
    private final DatabaseId databaseId;
    private final TableId changeSetTable;
    private String catalog = "";
    private String schema = "";
    private String dataTableChangeSetIdColumn =
        SpannerTableChangeSetPoller.DEFAULT_CHANGE_SET_ID_COLUMN;
    private String changeSetTableIdColumn =
        SpannerTableChangeSetPoller.DEFAULT_CHANGE_SET_ID_COLUMN;
    private boolean allTables = false;
    private List<String> includedTables = new ArrayList<>();
    private List<String> excludedTables = new ArrayList<>();
    private CommitTimestampRepository commitTimestampRepository;
    private Duration pollInterval = Duration.ofSeconds(1L);
    private ScheduledExecutorService executor;

    private BuilderImpl(Spanner spanner, DatabaseId databaseId, TableId changeSetTable) {
      this.spanner = Preconditions.checkNotNull(spanner);
      this.databaseId = Preconditions.checkNotNull(databaseId);
      this.changeSetTable = Preconditions.checkNotNull(changeSetTable);
      this.commitTimestampRepository =
          SpannerCommitTimestampRepository.newBuilder(spanner, databaseId).build();
    }

    @Override
    public TableExcluder allTables() {
      Preconditions.checkState(
          includedTables.isEmpty(), "Cannot include specific tables in combination with allTables");
      this.allTables = true;
      return this;
    }

    @Override
    public Builder includeTables(String firstTable, String... otherTables) {
      Preconditions.checkNotNull(firstTable);
      Preconditions.checkState(
          !allTables, "Cannot include specific tables in combination with allTables");
      includedTables.add(firstTable);
      includedTables.addAll(Arrays.asList(otherTables));
      return this;
    }

    @Override
    public Builder except(String... excludedTables) {
      this.excludedTables.addAll(Arrays.asList(excludedTables));
      return this;
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

    @Override
    public Builder setCommitTimestampRepository(CommitTimestampRepository repository) {
      this.commitTimestampRepository = Preconditions.checkNotNull(repository);
      return this;
    }

    /**
     * Sets the poll interval to use for this {@link SpannerDatabaseChangeSetPoller}. The default is
     * 1 second.
     */
    @Override
    public Builder setPollInterval(Duration interval) {
      this.pollInterval = Preconditions.checkNotNull(interval);
      return this;
    }

    @Override
    public Builder setExecutor(ScheduledExecutorService executor) {
      this.executor = Preconditions.checkNotNull(executor);
      return this;
    }

    /** Creates a {@link SpannerDatabaseChangeSetPoller} from this builder. */
    @Override
    public SpannerDatabaseChangeSetPoller build() {
      return new SpannerDatabaseChangeSetPoller(this);
    }
  }

  /** Creates a builder for a {@link SpannerDatabaseChangeSetPoller}. */
  public static TableSelecter newBuilder(Spanner spanner, DatabaseId databaseId) {
    return new BuilderImpl(
        spanner,
        databaseId,
        TableId.of(databaseId, SpannerTableChangeSetPoller.DEFAULT_CHANGE_SET_TABLE_NAME));
  }

  /** Creates a builder for a {@link SpannerDatabaseChangeSetPoller}. */
  public static TableSelecter newBuilder(
      Spanner spanner, DatabaseId databaseId, TableId changeSetTable) {
    return new BuilderImpl(spanner, databaseId, changeSetTable);
  }

  private final Object lock = new Object();
  private final Spanner spanner;
  private final DatabaseId databaseId;
  private final TableId changeSetTable;
  private final String catalog;
  private final String schema;
  private final boolean allTables;
  private final ImmutableList<String> includedTables;
  private final ImmutableList<String> excludedTables;
  private final String dataTableChangeSetIdColumn;
  private final String changeSetTableIdColumn;
  private final CommitTimestampRepository commitTimestampRepository;
  private final Duration pollInterval;
  private final ScheduledExecutorService executor;
  private final boolean isOwnedExecutor;
  private ImmutableList<TableId> tables;
  private Map<TableId, SpannerTableChangeWatcher> watchers;
  private final List<RowChangeCallback> callbacks = new LinkedList<>();

  private SpannerDatabaseChangeSetPoller(BuilderImpl builder) {
    this.spanner = builder.spanner;
    this.databaseId = builder.databaseId;
    this.changeSetTable = builder.changeSetTable;
    this.catalog = builder.catalog;
    this.schema = builder.schema;
    this.allTables = builder.allTables;
    this.includedTables = ImmutableList.copyOf(builder.includedTables);
    this.excludedTables =
        ImmutableList.<String>builder()
            .addAll(builder.excludedTables)
            .add(builder.changeSetTable.getTable())
            .build();
    this.dataTableChangeSetIdColumn = builder.dataTableChangeSetIdColumn;
    this.changeSetTableIdColumn = builder.changeSetTableIdColumn;
    this.commitTimestampRepository = builder.commitTimestampRepository;
    this.pollInterval = builder.pollInterval;
    if (builder.executor == null) {
      isOwnedExecutor = true;
      executor = new ScheduledThreadPoolExecutor(1);
    } else {
      isOwnedExecutor = false;
      executor = builder.executor;
    }
  }

  private ImmutableList<TableId> findTableNames(DatabaseClient client) {
    Statement statement =
        Statement.newBuilder(LIST_TABLE_NAMES_STATEMENT)
            .bind("excluded")
            .toStringArray(excludedTables)
            .bind("allTables")
            .to(allTables)
            .bind("included")
            .toStringArray(includedTables)
            .bind("schema")
            .to(schema)
            .bind("catalog")
            .to(catalog)
            .bind("changeSetIdColumn")
            .to(dataTableChangeSetIdColumn)
            .build();
    ImmutableList<TableId> tables =
        client
            .singleUse()
            .executeQueryAsync(statement)
            .toList(
                new Function<StructReader, TableId>() {
                  @Override
                  public TableId apply(StructReader input) {
                    return TableId.newBuilder(databaseId, input.getString(0))
                        .setCatalog(catalog)
                        .setSchema(schema)
                        .build();
                  }
                });
    // Check that all tables that were explicitly included were returned by allTableNames
    // and are valid to use with this poller, i.e. they have a column with the name of the
    // dataTableChangeSetIdColumn.
    for (String includedTable : includedTables) {
      if (!tables.contains(
          TableId.newBuilder(databaseId, includedTable)
              .setCatalog(catalog)
              .setSchema(schema)
              .build())) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.NOT_FOUND,
            String.format(
                "Table `%s` was explicitly included for this SpannerDatabaseChangeSetPoller, but either the table was not found or it does not contain a column with the name %s.",
                includedTable, dataTableChangeSetIdColumn));
      }
    }
    return tables;
  }

  @Override
  public void addCallback(RowChangeCallback callback) {
    Preconditions.checkState(state() == State.NEW);
    callbacks.add(callback);
  }

  @Override
  protected void doStart() {
    executor.execute(
        new Runnable() {
          @Override
          public void run() {
            try {
              ImmutableList<TableId> tables = getTables();
              if (tables.isEmpty()) {
                throw SpannerExceptionFactory.newSpannerException(
                    ErrorCode.NOT_FOUND,
                    String.format(
                        "No suitable tables found for watcher for database %s", databaseId));
              }
              if (isOwnedExecutor) {
                ((ScheduledThreadPoolExecutor) executor).setCorePoolSize(tables.size());
              }
              synchronized (lock) {
                if (watchers == null) {
                  initWatchersLocked();
                }
                for (SpannerTableChangeWatcher watcher : watchers.values()) {
                  watcher.startAsync();
                }
              }
            } catch (Throwable t) {
              logger.log(
                  LogRecordBuilder.of(
                      Level.WARNING, "Failed to start watcher for database {0}", databaseId, t));
              notifyFailed(t);
            }
          }
        });
  }

  @Override
  protected void doStop() {
    synchronized (lock) {
      for (SpannerTableChangeWatcher c : watchers.values()) {
        c.stopAsync();
      }
    }
  }

  @Override
  public DatabaseId getDatabaseId() {
    return databaseId;
  }

  @Override
  public ImmutableList<TableId> getTables() {
    synchronized (lock) {
      if (tables == null) {
        tables = findTableNames(spanner.getDatabaseClient(databaseId));
      }
      return tables;
    }
  }

  private void initWatchersLocked() {
    watchers = new HashMap<>(tables.size());
    for (TableId table : tables) {
      SpannerTableChangeWatcher watcher =
          SpannerTableChangeSetPoller.newBuilder(spanner, changeSetTable, table)
              .setDataTableChangeSetIdColumn(dataTableChangeSetIdColumn)
              .setChangeSetTableIdColumn(changeSetTableIdColumn)
              .setCommitTimestampRepository(commitTimestampRepository)
              .setPollInterval(pollInterval)
              .setExecutor(executor)
              .build();
      for (RowChangeCallback callback : callbacks) {
        watcher.addCallback(callback);
      }
      watcher.addListener(
          new Listener() {
            @Override
            public void failed(State from, Throwable failure) {
              synchronized (lock) {
                // Try to stop all watchers that are still running.
                for (SpannerTableChangeWatcher c : watchers.values()) {
                  if (c.state() != State.FAILED) {
                    c.stopAsync();
                  }
                }
                for (SpannerTableChangeWatcher c : watchers.values()) {
                  if (c.state() == State.STOPPING) {
                    c.awaitTerminated();
                  }
                }
                if (isOwnedExecutor) {
                  executor.shutdown();
                }
                logger.log(
                    LogRecordBuilder.of(
                        Level.WARNING,
                        "Watcher failed to start for database {0}",
                        databaseId,
                        failure));
                notifyFailed(failure);
              }
            }

            @Override
            public void running() {
              synchronized (lock) {
                if (state() == State.RUNNING) {
                  return;
                }
                for (SpannerTableChangeWatcher c : watchers.values()) {
                  if (c.state() != State.RUNNING) {
                    return;
                  }
                }
                logger.log(Level.INFO, "Watcher started successfully for database {0}", databaseId);
                notifyStarted();
              }
            }

            @Override
            public void terminated(State from) {
              synchronized (lock) {
                if (state() == State.TERMINATED) {
                  return;
                }
                for (SpannerTableChangeWatcher c : watchers.values()) {
                  if (c.state() != State.TERMINATED) {
                    return;
                  }
                }
                if (isOwnedExecutor) {
                  executor.shutdown();
                }
                logger.log(Level.INFO, "Watcher terminated for database {0}", databaseId);
                notifyStopped();
              }
            }
          },
          MoreExecutors.directExecutor());
      watchers.put(table, watcher);
    }
  }
}
