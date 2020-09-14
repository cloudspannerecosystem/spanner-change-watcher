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

import com.google.api.client.util.Preconditions;
import com.google.api.core.AbstractApiService;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.StructReader;
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
 * set of tables for changes based on commit timestamp columns in the tables.
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
 *     SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();
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
public class SpannerDatabaseTailer extends AbstractApiService
    implements SpannerDatabaseChangeWatcher {
  private static final Logger logger = Logger.getLogger(SpannerDatabaseTailer.class.getName());

  /** Builder for a {@link SpannerDatabaseTailer}. */
  public interface Builder {
    /**
     * Sets a specific {@link CommitTimestampRepository} to use for the {@link
     * SpannerDatabaseTailer}.
     *
     * <p>The default will use a {@link SpannerCommitTimestampRepository} which stores the last seen
     * commit timestamp in a table named LAST_SEEN_COMMIT_TIMESTAMPS in the Spanner database that
     * this {@link SpannerDatabaseTailer} is monitoring. The table will be created if it does not
     * already exist.
     */
    public Builder setCommitTimestampRepository(CommitTimestampRepository repository);

    /**
     * Sets the {@link ShardProvider} that this {@link SpannerDatabaseTailer} should use for all
     * tables that do not have a specific {@link ShardProvider} set through {@link
     * #setShardProviders(Map)}.
     */
    public Builder setShardProvider(ShardProvider shardProvider);

    /**
     * Sets the {@link ShardProvider}s to use for specific tables. Any {@link ShardProvider} that is
     * supplied through this method will override any global {@link ShardProvider} that has been set
     * using {@link #setShardProvider(ShardProvider)}.
     */
    public Builder setShardProviders(Map<TableId, ShardProvider> shardProviders);

    /**
     * Sets the poll interval to use for this {@link SpannerDatabaseTailer}. The default is 1
     * second.
     */
    public Builder setPollInterval(Duration interval);

    /**
     * Sets a specific {@link ScheduledExecutorService} to use for this {@link
     * SpannerDatabaseTailer}. This executor will be used to execute the poll queries on the tables
     * and to call the {@link RowChangeCallback}s. The default will use a {@link
     * ScheduledThreadPoolExecutor} with a core size equal to the number of tables that is being
     * monitored.
     */
    public Builder setExecutor(ScheduledExecutorService executor);

    /**
     * <strong>This should only be set if your tables contain more than one commit timestamp
     * column.</strong>
     *
     * <p>Sets a function that returns the commit timestamp column to use for a specific table. This
     * is only needed in case your tables contain more than one commit timestamp column. {@link
     * SpannerDatabaseTailer} can automatically find the commit timestamp column for tables that
     * only contain one column with allow_commit_timestamp=true.
     *
     * @param commitTimestampFunction The function to use to determine which commit timestamp column
     *     should be used for a specific table. If no function has been specified, or if the
     *     function returns <code>null</code> for a given {@link TableId}, the {@link
     *     SpannerDatabaseTailer} will automatically use the first column of the table that has the
     *     option allow_commit_timestamp=true.
     * @return The Builder.
     */
    public Builder setCommitTimestampColumnFunction(
        java.util.function.Function<TableId, String> commitTimestampFunction);

    /** Creates a {@link SpannerDatabaseTailer} from this builder. */
    public SpannerDatabaseTailer build();
  }

  /**
   * Interface for selecting the tables that should be monitored by a {@link SpannerDatabaseTailer}.
   */
  public interface TableSelecter {
    /**
     * Instructs the {@link SpannerDatabaseTailer} to only emit changes for these specific tables.
     */
    Builder includeTables(String firstTable, String... furtherTables);

    /**
     * Instructs the {@link SpannerDatabaseTailer} to emit change events for all tables in the
     * database that have a column with option ALLOW_COMMIT_TIMESTAMP=TRUE. Tables that don't have a
     * commit timestamp column are automatically ignored. Additional tables can be excluded by
     * calling {@link TableExcluder#except(String...)}.
     */
    TableExcluder allTables();
  }

  /** Interface for excluding specific tables from a {@link SpannerDatabaseTailer}. */
  public interface TableExcluder extends Builder {
    /**
     * Instructs the {@link SpannerDatabaseTailer} to exclude these tables from change events. This
     * option can be used in combination with {@link TableSelecter#allTables()} to include all
     * tables except for a specfic list.
     */
    Builder except(String... tables);
  }

  /** Lists all tables with a commit timestamp column. */
  static final String LIST_TABLE_NAMES_STATEMENT =
      "SELECT TABLE_NAME\n"
          + "FROM INFORMATION_SCHEMA.TABLES\n"
          + "WHERE TABLE_NAME NOT IN UNNEST(@excluded)\n"
          + "AND (@allTables=TRUE OR TABLE_NAME IN UNNEST(@included))\n"
          + "AND TABLE_CATALOG = @catalog\n"
          + "AND TABLE_SCHEMA = @schema\n"
          + "AND TABLE_NAME IN (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMN_OPTIONS WHERE OPTION_NAME='allow_commit_timestamp' AND OPTION_VALUE='TRUE')";

  static class BuilderImpl implements TableSelecter, TableExcluder, Builder {
    private final Spanner spanner;
    private final DatabaseId databaseId;
    private String catalog = "";
    private String schema = "";
    private ShardProvider shardProvider;
    private Map<TableId, ShardProvider> shardProviders;
    private boolean allTables = false;
    private List<String> includedTables = new ArrayList<>();
    private List<String> excludedTables = new ArrayList<>();
    private CommitTimestampRepository commitTimestampRepository;
    private Duration pollInterval = Duration.ofSeconds(1L);
    private ScheduledExecutorService executor;
    private java.util.function.Function<TableId, String> commitTimestampColumnFunction;

    private BuilderImpl(Spanner spanner, DatabaseId databaseId) {
      this.spanner = Preconditions.checkNotNull(spanner);
      this.databaseId = Preconditions.checkNotNull(databaseId);
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
     * Sets a specific {@link CommitTimestampRepository} to use for the {@link
     * SpannerDatabaseTailer}.
     *
     * <p>The default will use a {@link SpannerCommitTimestampRepository} which stores the last seen
     * commit timestamp in a table named LAST_SEEN_COMMIT_TIMESTAMPS in the Spanner database that
     * this {@link SpannerDatabaseTailer} is monitoring. The table will be created if it does not
     * already exist.
     */
    @Override
    public Builder setCommitTimestampRepository(CommitTimestampRepository repository) {
      this.commitTimestampRepository = Preconditions.checkNotNull(repository);
      return this;
    }

    @Override
    public Builder setShardProvider(ShardProvider shardProvider) {
      this.shardProvider = shardProvider;
      return this;
    }

    @Override
    public Builder setShardProviders(Map<TableId, ShardProvider> shardProviders) {
      this.shardProviders = shardProviders;
      return this;
    }

    /**
     * Sets the poll interval to use for this {@link SpannerDatabaseTailer}. The default is 1
     * second.
     */
    @Override
    public Builder setPollInterval(Duration interval) {
      this.pollInterval = Preconditions.checkNotNull(interval);
      return this;
    }

    /**
     * Sets a specific {@link ScheduledExecutorService} to use for this {@link
     * SpannerDatabaseTailer}. This executor will be used to execute the poll queries on the tables
     * and to call the {@link RowChangeCallback}s. The default will use a {@link
     * ScheduledThreadPoolExecutor} with a core size equal to the number of tables that is being
     * monitored.
     */
    @Override
    public Builder setExecutor(ScheduledExecutorService executor) {
      this.executor = Preconditions.checkNotNull(executor);
      return this;
    }

    public Builder setCommitTimestampColumnFunction(
        java.util.function.Function<TableId, String> commitTimestampColumnFunction) {
      this.commitTimestampColumnFunction = commitTimestampColumnFunction;
      return this;
    }

    /** Creates a {@link SpannerDatabaseTailer} from this builder. */
    @Override
    public SpannerDatabaseTailer build() {
      return new SpannerDatabaseTailer(this);
    }
  }

  /** Creates a builder for a {@link SpannerDatabaseTailer}. */
  public static TableSelecter newBuilder(Spanner spanner, DatabaseId databaseId) {
    return new BuilderImpl(spanner, databaseId);
  }

  private final Object lock = new Object();
  private final Spanner spanner;
  private final DatabaseId databaseId;
  private final String catalog;
  private final String schema;
  private final ShardProvider shardProvider;
  private final Map<TableId, ShardProvider> shardProviders;
  private final boolean allTables;
  private final ImmutableList<String> includedTables;
  private final ImmutableList<String> excludedTables;
  private final CommitTimestampRepository commitTimestampRepository;
  private final Duration pollInterval;
  private final ScheduledExecutorService executor;
  private final boolean isOwnedExecutor;
  private final java.util.function.Function<TableId, String> commitTimestampColumnFunction;
  private List<TableId> tables;
  private Map<TableId, SpannerTableChangeWatcher> watchers;
  private final List<RowChangeCallback> callbacks = new LinkedList<>();

  private SpannerDatabaseTailer(BuilderImpl builder) {
    this.spanner = builder.spanner;
    this.databaseId = builder.databaseId;
    this.catalog = builder.catalog;
    this.schema = builder.schema;
    this.shardProvider = builder.shardProvider;
    this.shardProviders = builder.shardProviders;
    this.allTables = builder.allTables;
    this.includedTables = ImmutableList.copyOf(builder.includedTables);
    this.excludedTables = ImmutableList.copyOf(builder.excludedTables);
    this.commitTimestampRepository = builder.commitTimestampRepository;
    this.pollInterval = builder.pollInterval;
    if (builder.executor == null) {
      isOwnedExecutor = true;
      executor = new ScheduledThreadPoolExecutor(1);
    } else {
      isOwnedExecutor = false;
      executor = builder.executor;
    }
    this.commitTimestampColumnFunction = builder.commitTimestampColumnFunction;
  }

  private List<TableId> findTableNames(DatabaseClient client) {
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
            .build();
    List<TableId> tables =
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
    // and are valid to use with this tailer, i.e. they have a column containing a commit
    // timestamp.
    for (String includedTable : includedTables) {
      if (!tables.contains(
          TableId.newBuilder(databaseId, includedTable)
              .setCatalog(catalog)
              .setSchema(schema)
              .build())) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.NOT_FOUND,
            String.format(
                "Table `%s` was explicitly included for this SpannerDatabaseTailer, but either the table was not found or it does not contain a column with the option allow_commit_timestamp=true.",
                includedTable));
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
              List<TableId> tables = getTables();
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
  public List<TableId> getTables() {
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
      ShardProvider tableShardProvider = null;
      if (this.shardProviders != null) {
        tableShardProvider = this.shardProviders.get(table);
      }
      if (tableShardProvider == null) {
        tableShardProvider = this.shardProvider;
      }
      SpannerTableTailer watcher =
          SpannerTableTailer.newBuilder(spanner, table)
              .setCommitTimestampRepository(commitTimestampRepository)
              .setShardProvider(tableShardProvider)
              .setPollInterval(pollInterval)
              .setExecutor(executor)
              .setCommitTimestampColumn(
                  commitTimestampColumnFunction == null
                      ? null
                      : commitTimestampColumnFunction.apply(table))
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
