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

  /** Lists all tables with a commit timestamp column. */
  static final String LIST_TABLE_NAMES_STATEMENT =
      "SELECT TABLE_NAME\n"
          + "FROM INFORMATION_SCHEMA.TABLES\n"
          + "WHERE TABLE_NAME NOT IN UNNEST(@excluded)\n"
          + "AND (@allTables=TRUE OR TABLE_NAME IN UNNEST(@included))\n"
          + "AND TABLE_CATALOG = @catalog\n"
          + "AND TABLE_SCHEMA = @schema\n"
          + "AND TABLE_NAME IN (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMN_OPTIONS WHERE OPTION_NAME='allow_commit_timestamp' AND OPTION_VALUE='TRUE')";

  /** Builder for a {@link SpannerDatabaseTailer}. */
  public static class Builder {
    private final Spanner spanner;
    private final DatabaseId databaseId;
    private String catalog = "";
    private String schema = "";
    private boolean allTables = false;
    private List<String> includedTables = new ArrayList<>();
    private List<String> excludedTables = new ArrayList<>();
    private CommitTimestampRepository commitTimestampRepository;
    private Duration pollInterval = Duration.ofSeconds(1L);
    private ScheduledExecutorService executor;

    private Builder(Spanner spanner, DatabaseId databaseId) {
      this.spanner = Preconditions.checkNotNull(spanner);
      this.databaseId = Preconditions.checkNotNull(databaseId);
      this.commitTimestampRepository =
          SpannerCommitTimestampRepository.newBuilder(spanner, databaseId).build();
    }

    /**
     * Instructs the {@link SpannerDatabaseTailer} to emit change events for all tables in the
     * database that have a column with option ALLOW_COMMIT_TIMESTAMP=TRUE. Tables that don't have a
     * commit timestamp column are automatically ignored. Additional tables can be excluded by
     * calling {@link #excludeTables(String...)}.
     */
    public Builder allTables() {
      Preconditions.checkState(
          includedTables.isEmpty(), "Cannot include specific tables in combination with allTables");
      this.allTables = true;
      return this;
    }

    /**
     * Instructs the {@link SpannerDatabaseTailer} to only emit changes for these specific tables.
     * This option cannot be used in combination with {@link #allTables()}. If you both exclude and
     * include the same table, the exclusion will get priority and the table will not be included.
     */
    public Builder includeTables(String... tables) {
      Preconditions.checkState(
          !allTables, "Cannot include specific tables in combination with allTables");
      includedTables.addAll(Arrays.asList(tables));
      return this;
    }

    /**
     * Instructs the {@link SpannerDatabaseTailer} to exclude these tables from change events. This
     * option can be used in combination with {@link #allTables()} to include all tables except for
     * a specfic list.
     */
    public Builder excludeTables(String... excludedTables) {
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
    public Builder setCommitTimestampRepository(CommitTimestampRepository repository) {
      this.commitTimestampRepository = Preconditions.checkNotNull(repository);
      return this;
    }

    /**
     * Sets the poll interval to use for this {@link SpannerDatabaseTailer}. The default is 1
     * second.
     */
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
    public Builder setExecutor(ScheduledExecutorService executor) {
      this.executor = Preconditions.checkNotNull(executor);
      return this;
    }

    /** Creates a {@link SpannerDatabaseTailer} from this builder. */
    public SpannerDatabaseTailer build() {
      return new SpannerDatabaseTailer(this);
    }
  }

  /** Creates a builder for a {@link SpannerDatabaseTailer}. */
  public static Builder newBuilder(Spanner spanner, DatabaseId databaseId) {
    return new Builder(spanner, databaseId);
  }

  private final Object lock = new Object();
  private final Builder builder;
  private final Spanner spanner;
  private final DatabaseId databaseId;
  private final String catalog;
  private final String schema;
  private final boolean allTables;
  private final ImmutableList<String> includedTables;
  private final ImmutableList<String> excludedTables;
  private ScheduledExecutorService executor;
  private boolean isOwnedExecutor;
  private ImmutableList<TableId> tables;
  private Map<TableId, SpannerTableChangeWatcher> watchers;
  private final List<RowChangeCallback> callbacks = new LinkedList<>();

  private SpannerDatabaseTailer(Builder builder) {
    this.builder = builder;
    this.spanner = builder.spanner;
    this.databaseId = builder.databaseId;
    this.catalog = builder.catalog;
    this.schema = builder.schema;
    this.allTables = builder.allTables;
    this.includedTables = ImmutableList.copyOf(builder.includedTables);
    this.excludedTables = ImmutableList.copyOf(builder.excludedTables);
    if (builder.executor == null) {
      isOwnedExecutor = true;
      executor = new ScheduledThreadPoolExecutor(1);
    } else {
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
    // and are valid to use with this tailer, i.e. they have a column containing a commit
    // timestamp.
    for (String includedTable : builder.includedTables) {
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
              ImmutableList<TableId> tables = getTables();
              if (isOwnedExecutor && !tables.isEmpty()) {
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
              logger.log(Level.WARNING, "Failed to start watcher", t);
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
      SpannerTableTailer watcher =
          SpannerTableTailer.newBuilder(spanner, table)
              .setCommitTimestampRepository(builder.commitTimestampRepository)
              .setPollInterval(builder.pollInterval)
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
                logger.log(Level.WARNING, "Watcher failed to start", failure);
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
                logger.info("Watcher started successfully");
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
                logger.info("Watcher terminated");
                notifyStopped();
              }
            }
          },
          MoreExecutors.directExecutor());
      watchers.put(table, watcher);
    }
  }
}
