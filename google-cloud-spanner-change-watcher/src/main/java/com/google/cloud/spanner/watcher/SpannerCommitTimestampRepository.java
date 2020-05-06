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
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.watcher.SpannerUtils.LogRecordBuilder;
import com.google.common.base.MoreObjects;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * {@link CommitTimestampRepository} that stores the last seen commit timestamp for a table in a
 * Cloud Spanner database table. The default table definition to use is
 *
 * <pre>{@code
 * CREATE TABLE LAST_SEEN_COMMIT_TIMESTAMPS (
 *        DATABASE_NAME STRING(MAX) NOT NULL,
 *        TABLE_CATALOG STRING(MAX) NOT NULL,
 *        TABLE_SCHEMA STRING(MAX) NOT NULL,
 *        TABLE_NAME STRING(MAX) NOT NULL,
 *        LAST_SEEN_COMMIT_TIMESTAMP TIMESTAMP NOT NULL
 * ) PRIMARY KEY (DATABASE_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME)
 * }</pre>
 *
 * The table name and column names are configurable.
 */
public class SpannerCommitTimestampRepository implements CommitTimestampRepository {
  private static final Logger logger =
      Logger.getLogger(SpannerCommitTimestampRepository.class.getName());

  static final String DEFAULT_TABLE_CATALOG = "";
  static final String DEFAULT_TABLE_SCHEMA = "";
  static final String DEFAULT_TABLE_NAME = "LAST_SEEN_COMMIT_TIMESTAMPS";
  static final String DEFAULT_DATABASE_NAME_COLUMN_NAME = "DATABASE_NAME";
  static final String DEFAULT_TABLE_CATALOG_COLUMN_NAME = "TABLE_CATALOG";
  static final String DEFAULT_TABLE_SCHEMA_COLUMN_NAME = "TABLE_SCHEMA";
  static final String DEFAULT_TABLE_NAME_COLUMN_NAME = "TABLE_NAME";
  static final String DEFAULT_COMMIT_TIMESTAMP_COLUMN_NAME = "LAST_SEEN_COMMIT_TIMESTAMP";
  static final String FIND_TABLE_STATEMENT =
      "SELECT TABLE_NAME\n"
          + "FROM INFORMATION_SCHEMA.TABLES\n"
          + "WHERE TABLE_CATALOG=@catalog\n"
          + "AND TABLE_SCHEMA=@schema\n"
          + "AND TABLE_NAME=@table";
  static final String FIND_COLUMNS_STATEMENT =
      "SELECT COLUMN_NAME, SPANNER_TYPE\n"
          + "FROM INFORMATION_SCHEMA.COLUMNS\n"
          + "WHERE TABLE_CATALOG=@catalog\n"
          + "AND TABLE_SCHEMA=@schema\n"
          + "AND TABLE_NAME=@table\n"
          + "ORDER BY ORDINAL_POSITION";
  static final String FIND_PK_COLUMNS_STATEMENT =
      "SELECT INDEX_COLUMNS.COLUMN_NAME\n"
          + "FROM INFORMATION_SCHEMA.INDEXES\n"
          + "INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS \n"
          + "            ON  INDEXES.TABLE_CATALOG=INDEX_COLUMNS.TABLE_CATALOG\n"
          + "            AND INDEXES.TABLE_SCHEMA=INDEX_COLUMNS.TABLE_SCHEMA\n"
          + "            AND INDEXES.TABLE_NAME=INDEX_COLUMNS.TABLE_NAME\n"
          + "            AND INDEXES.INDEX_NAME=INDEX_COLUMNS.INDEX_NAME\n"
          + "WHERE INDEXES.TABLE_CATALOG=@catalog\n"
          + "AND INDEXES.TABLE_SCHEMA=@schema\n"
          + "AND INDEXES.TABLE_NAME=@table\n"
          + "AND INDEXES.INDEX_TYPE='PRIMARY_KEY'\n"
          + "ORDER BY INDEX_COLUMNS.ORDINAL_POSITION";
  static final String CREATE_TABLE_STATEMENT =
      "CREATE TABLE `%s` (\n"
          + "        `%s` STRING(MAX) NOT NULL,\n" // DATABASE_NAME
          + "        `%s` STRING(MAX) NOT NULL,\n" // TABLE_CATALOG
          + "        `%s` STRING(MAX) NOT NULL,\n" // TABLE_SCHEMA
          + "        `%s` STRING(MAX) NOT NULL,\n" // TABLE_NAME
          + "        `%s` TIMESTAMP NOT NULL\n" // LAST_SEEN_COMMIT_TIMESTAMP
          + ") PRIMARY KEY (`%s`, `%s`, `%s`, `%s`)";
  private static final Set<String> RUNNING_CREATE_TABLE_STATEMENTS = new HashSet<>();

  /** Builder for a {@link SpannerCommitTimestampRepository}. */
  public static class Builder {
    private final Spanner spanner;
    private final DatabaseId databaseId;
    private boolean createTableIfNotExists = true;
    private String commitTimestampsCatalog = DEFAULT_TABLE_CATALOG;
    private String commitTimestampsSchema = DEFAULT_TABLE_SCHEMA;
    private String commitTimestampsTable = DEFAULT_TABLE_NAME;
    private String databaseCol = DEFAULT_DATABASE_NAME_COLUMN_NAME;
    private String catalogCol = DEFAULT_TABLE_CATALOG_COLUMN_NAME;
    private String schemaCol = DEFAULT_TABLE_SCHEMA_COLUMN_NAME;
    private String tableCol = DEFAULT_TABLE_NAME_COLUMN_NAME;
    private String tsCol = DEFAULT_COMMIT_TIMESTAMP_COLUMN_NAME;
    private Timestamp initialCommitTimestamp;

    private Builder(Spanner spanner, DatabaseId databaseId) {
      this.spanner = Preconditions.checkNotNull(spanner);
      this.databaseId = Preconditions.checkNotNull(databaseId);
    }

    /**
     * Instructs the {@link SpannerCommitTimestampRepository} to automatically create the required
     * LAST_SEEN_COMMIT_TIMESTAMPS table. Defaults to true.
     */
    public Builder setCreateTableIfNotExists(boolean create) {
      this.createTableIfNotExists = create;
      return this;
    }

    /**
     * Sets the name of the table to use to store the last seen commit timestamp. Defaults to
     * LAST_SEEN_COMMIT_TIMESTAMPS.
     */
    public Builder setCommitTimestampsTable(String table) {
      this.commitTimestampsTable = Preconditions.checkNotNull(table);
      return this;
    }

    public Builder setDatabaseNameColumn(String column) {
      this.databaseCol = Preconditions.checkNotNull(column);
      return this;
    }

    public Builder setCatalogNameColumn(String column) {
      this.catalogCol = Preconditions.checkNotNull(column);
      return this;
    }

    public Builder setSchemaNameColumn(String column) {
      this.schemaCol = Preconditions.checkNotNull(column);
      return this;
    }

    public Builder setTableNameColumn(String column) {
      this.tableCol = Preconditions.checkNotNull(column);
      return this;
    }

    public Builder setCommitTimestampColumn(String column) {
      this.tsCol = Preconditions.checkNotNull(column);
      return this;
    }

    /**
     * Sets the initial commit timestamp to use for tables that are not yet known to this
     * repository. Defaults to the current time of the local system, which means that the {@link
     * SpannerTableChangeWatcher} will only report changes that are created after this initial
     * registration. Setting this value to {@link Timestamp#MIN_VALUE} will make the {@link
     * SpannerTableChangeWatcher} consider all existing rows in the table as changed and emit change
     * events all existing records.
     */
    public Builder setInitialCommitTimestamp(@Nullable Timestamp initial) {
      this.initialCommitTimestamp = initial;
      return this;
    }

    /** Builds the {@link SpannerCommitTimestampRepository}. */
    public SpannerCommitTimestampRepository build() {
      return new SpannerCommitTimestampRepository(this);
    }
  }

  public static Builder newBuilder(Spanner spanner, DatabaseId databaseId) {
    return new Builder(spanner, databaseId);
  }

  private final DatabaseId databaseId;
  private final DatabaseClient client;
  private final DatabaseAdminClient adminClient;
  private final boolean createTableIfNotExists;
  private final String commitTimestampsCatalog;
  private final String commitTimestampsSchema;
  private final String commitTimestampsTable;
  private final String databaseCol;
  private final String catalogCol;
  private final String schemaCol;
  private final String tableCol;
  private final String tsCol;
  private final Timestamp initialCommitTimestamp;
  private final Iterable<String> tsColumns;
  private boolean initialized = false;

  private SpannerCommitTimestampRepository(Builder builder) {
    this.databaseId = builder.databaseId;
    this.client = builder.spanner.getDatabaseClient(builder.databaseId);
    this.adminClient = builder.spanner.getDatabaseAdminClient();
    this.createTableIfNotExists = builder.createTableIfNotExists;
    this.commitTimestampsCatalog = builder.commitTimestampsCatalog;
    this.commitTimestampsSchema = builder.commitTimestampsSchema;
    this.commitTimestampsTable = builder.commitTimestampsTable;
    this.databaseCol = builder.databaseCol;
    this.catalogCol = builder.catalogCol;
    this.schemaCol = builder.schemaCol;
    this.tableCol = builder.tableCol;
    this.tsCol = builder.tsCol;
    this.initialCommitTimestamp =
        MoreObjects.firstNonNull(builder.initialCommitTimestamp, Timestamp.now());
    this.tsColumns = Collections.singleton(builder.tsCol);
  }

  /** Checks that the table is present and contains the actually expected columns. */
  private void initialize() {
    Statement statement =
        Statement.newBuilder(FIND_TABLE_STATEMENT)
            .bind("catalog")
            .to(commitTimestampsCatalog)
            .bind("schema")
            .to(commitTimestampsSchema)
            .bind("table")
            .to(commitTimestampsTable)
            .build();
    try (ResultSet rs = client.singleUse().executeQuery(statement)) {
      if (!rs.next()) {
        if (createTableIfNotExists) {
          createTable();
          initialized = true;
          return;
        } else {
          logger.log(
              Level.WARNING,
              "Commit timestamps table {0} not found",
              TableId.of(databaseId, commitTimestampsTable));
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.NOT_FOUND, String.format("Table %s not found", commitTimestampsTable));
        }
      }
    }
    // Table exists, check that it contains the expected columns.
    try {
      verifyTable();
    } catch (Throwable t) {
      logger.log(
          LogRecordBuilder.of(
              Level.WARNING,
              "Verification of commit timestamps table {0} failed",
              TableId.of(databaseId, commitTimestampsTable),
              t));
      throw t;
    }
    initialized = true;
  }

  private void createTable() {
    logger.log(
        Level.INFO,
        "Creating commit timestamps table {0}",
        TableId.of(databaseId, commitTimestampsTable));
    String createTable =
        String.format(
            CREATE_TABLE_STATEMENT,
            commitTimestampsTable,
            databaseCol,
            catalogCol,
            schemaCol,
            tableCol,
            tsCol,
            databaseCol,
            catalogCol,
            schemaCol,
            tableCol);
    boolean tableAlreadyBeingCreated = false;
    synchronized (RUNNING_CREATE_TABLE_STATEMENTS) {
      tableAlreadyBeingCreated = RUNNING_CREATE_TABLE_STATEMENTS.contains(createTable);
      if (!tableAlreadyBeingCreated) {
        RUNNING_CREATE_TABLE_STATEMENTS.add(createTable);
      }
    }
    if (tableAlreadyBeingCreated) {
      waitForTableCreationToFinish(createTable);
      return;
    }
    try {
      OperationFuture<Void, UpdateDatabaseDdlMetadata> fut =
          adminClient.updateDatabaseDdl(
              databaseId.getInstanceId().getInstance(),
              databaseId.getDatabase(),
              Collections.singleton(createTable),
              null);
      try {
        fut.get();
        logger.log(
            Level.INFO,
            "Created commit timestamps table {0}",
            TableId.of(databaseId, commitTimestampsTable));
      } catch (ExecutionException e) {
        logger.log(
            LogRecordBuilder.of(
                Level.WARNING,
                "Could not create commit timestamps table {0}",
                TableId.of(databaseId, commitTimestampsTable),
                e));
        SpannerExceptionFactory.newSpannerException(e.getCause());
      } catch (InterruptedException e) {
        logger.log(
            LogRecordBuilder.of(
                Level.WARNING,
                "Create commit timestamps table {0} interrupted",
                TableId.of(databaseId, commitTimestampsTable),
                e));
        SpannerExceptionFactory.propagateInterrupt(e);
      }
    } finally {
      RUNNING_CREATE_TABLE_STATEMENTS.remove(createTable);
    }
  }

  private void waitForTableCreationToFinish(String createTable) {
    while (true) {
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException e) {
        SpannerExceptionFactory.propagateInterrupt(e);
      }
      synchronized (RUNNING_CREATE_TABLE_STATEMENTS) {
        if (!RUNNING_CREATE_TABLE_STATEMENTS.contains(createTable)) {
          return;
        }
      }
    }
  }

  private void verifyTable() {
    Statement columnsStatement =
        Statement.newBuilder(FIND_COLUMNS_STATEMENT)
            .bind("catalog")
            .to(commitTimestampsCatalog)
            .bind("schema")
            .to(commitTimestampsSchema)
            .bind("table")
            .to(commitTimestampsTable)
            .build();
    boolean foundDatabaseCol = false;
    boolean foundCatalogCol = false;
    boolean foundSchemaCol = false;
    boolean foundTableCol = false;
    boolean foundTsCol = false;
    try (ResultSet rs = client.singleUse().executeQuery(columnsStatement)) {
      while (rs.next()) {
        String col = rs.getString("COLUMN_NAME");
        if (col.equalsIgnoreCase(databaseCol)
            || col.equalsIgnoreCase(catalogCol)
            || col.equalsIgnoreCase(schemaCol)
            || col.equalsIgnoreCase(tableCol)) {
          if (!rs.getString("SPANNER_TYPE").startsWith("STRING")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Column %s is not of type STRING, but of type %s. Name columns must be of type STRING.",
                    col, rs.getString("SPANNER_TYPE")));
          }
          foundDatabaseCol = foundDatabaseCol || col.equalsIgnoreCase(databaseCol);
          foundCatalogCol = foundCatalogCol || col.equalsIgnoreCase(catalogCol);
          foundSchemaCol = foundSchemaCol || col.equalsIgnoreCase(schemaCol);
          foundTableCol = foundTableCol || col.equalsIgnoreCase(tableCol);
        } else if (col.equalsIgnoreCase(tsCol)) {
          if (!rs.getString("SPANNER_TYPE").equals("TIMESTAMP")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Commit timestamp column %s is not of type TIMESTAMP, but of type %s",
                    tsCol, rs.getString("SPANNER_TYPE")));
          }
          foundTsCol = true;
        }
      }
    }
    if (!foundDatabaseCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Database name column %s not found", databaseCol));
    }
    if (!foundCatalogCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Catalog name column %s not found", catalogCol));
    }
    if (!foundSchemaCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Schema name column %s not found", schemaCol));
    }
    if (!foundTableCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Table name column %s not found", tableCol));
    }
    if (!foundTsCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Commit timestamp column %s not found", tsCol));
    }

    // Verify that the table name column is the primary key.
    Statement pkStatement =
        Statement.newBuilder(FIND_PK_COLUMNS_STATEMENT)
            .bind("catalog")
            .to(commitTimestampsCatalog)
            .bind("schema")
            .to(commitTimestampsSchema)
            .bind("table")
            .to(commitTimestampsTable)
            .build();
    String[] expectedCols = new String[] {databaseCol, catalogCol, schemaCol, tableCol};
    int index = 0;
    try (ResultSet rs = client.singleUse().executeQuery(pkStatement)) {
      while (rs.next()) {
        if (!expectedCols[index].equalsIgnoreCase(rs.getString(0))) {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT,
              String.format(
                  "Expected column `%s` as column number %d of the primary key of the table `%s`, but instead column `%s` was found as column number %d of the primary key.",
                  expectedCols[index],
                  index + 1,
                  commitTimestampsTable,
                  rs.getString(0),
                  index + 1));
        }
        index++;
      }
      if (index < expectedCols.length) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.NOT_FOUND,
            String.format(
                "Table %s does has a primary key with too few columns. The primary key of the table must be (`%s`, `%s`, `%s`, `%s`).",
                commitTimestampsTable, databaseCol, catalogCol, schemaCol, tableCol));
      }
    }
  }

  @Override
  public Timestamp get(TableId table) {
    Preconditions.checkNotNull(table);
    if (!initialized) {
      initialize();
    }
    Struct row =
        client
            .singleUse()
            .readRow(
                commitTimestampsTable,
                Key.of(
                    table.getDatabaseId().getName(),
                    table.getCatalog(),
                    table.getSchema(),
                    table.getTable()),
                tsColumns);
    if (row == null) {
      return initialCommitTimestamp;
    }
    return row.getTimestamp(0);
  }

  @Override
  public void set(TableId table, Timestamp commitTimestamp) {
    Preconditions.checkNotNull(table);
    Preconditions.checkNotNull(commitTimestamp);
    if (!initialized) {
      initialize();
    }
    client.writeAtLeastOnce(
        Collections.singleton(
            Mutation.newInsertOrUpdateBuilder(commitTimestampsTable)
                .set(databaseCol)
                .to(table.getDatabaseId().getName())
                .set(catalogCol)
                .to(table.getCatalog())
                .set(schemaCol)
                .to(table.getSchema())
                .set(tableCol)
                .to(table.getTable())
                .set(tsCol)
                .to(commitTimestamp)
                .build()));
  }
}
