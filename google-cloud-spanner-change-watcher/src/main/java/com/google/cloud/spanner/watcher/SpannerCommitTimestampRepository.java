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
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.SpannerUtils.LogRecordBuilder;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * {@link CommitTimestampRepository} that stores the last seen commit timestamp for a table in a
 * Cloud Spanner database table. The default table definition to use is
 *
 * <pre>{@code
 * CREATE TABLE LAST_SEEN_COMMIT_TIMESTAMPS (
 *        DATABASE_NAME              STRING(MAX) NOT NULL,
 *        TABLE_CATALOG              STRING(MAX) NOT NULL,
 *        TABLE_SCHEMA               STRING(MAX) NOT NULL,
 *        TABLE_NAME                 STRING(MAX) NOT NULL,
 *        SHARD_ID_BOOL              BOOL,
 *        SHARD_ID_BYTES             BYTES(MAX),
 *        SHARD_ID_DATE              DATE,
 *        SHARD_ID_FLOAT64           FLOAT64,
 *        SHARD_ID_INT64             INT64,
 *        SHARD_ID_STRING            STRING(MAX),
 *        SHARD_ID_TIMESTAMP         TIMESTAMP,
 *        LAST_SEEN_COMMIT_TIMESTAMP TIMESTAMP NOT NULL
 * ) PRIMARY KEY (DATABASE_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, SHARD_ID_BOOL, SHARD_ID_BYTES, SHARD_ID_DATE, SHARD_ID_FLOAT64, SHARD_ID_INT64, SHARD_ID_STRING, SHARD_ID_TIMESTAMP)
 * }</pre>
 *
 * The table name and column names are configurable.
 */
public class SpannerCommitTimestampRepository implements CommitTimestampRepository {
  private static final Logger logger =
      Logger.getLogger(SpannerCommitTimestampRepository.class.getName());
  private static final Charset UTF8 = Charset.forName("UTF8");

  static final String DEFAULT_TABLE_CATALOG = "";
  static final String DEFAULT_TABLE_SCHEMA = "";
  static final String DEFAULT_TABLE_NAME = "LAST_SEEN_COMMIT_TIMESTAMPS";
  static final String DEFAULT_DATABASE_NAME_COLUMN_NAME = "DATABASE_NAME";
  static final String DEFAULT_TABLE_CATALOG_COLUMN_NAME = "TABLE_CATALOG";
  static final String DEFAULT_TABLE_SCHEMA_COLUMN_NAME = "TABLE_SCHEMA";
  static final String DEFAULT_TABLE_NAME_COLUMN_NAME = "TABLE_NAME";
  static final String DEFAULT_SHARD_ID_BOOL_COLUMN_NAME = "SHARD_ID_BOOL";
  static final String DEFAULT_SHARD_ID_BYTES_COLUMN_NAME = "SHARD_ID_BYTES";
  static final String DEFAULT_SHARD_ID_DATE_COLUMN_NAME = "SHARD_ID_DATE";
  static final String DEFAULT_SHARD_ID_FLOAT64_COLUMN_NAME = "SHARD_ID_FLOAT64";
  static final String DEFAULT_SHARD_ID_INT64_COLUMN_NAME = "SHARD_ID_INT64";
  static final String DEFAULT_SHARD_ID_STRING_COLUMN_NAME = "SHARD_ID_STRING";
  static final String DEFAULT_SHARD_ID_TIMESTAMP_COLUMN_NAME = "SHARD_ID_TIMESTAMP";
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
          + "        `%s` BOOL,\n" // SHARD_ID_BOOL
          + "        `%s` BYTES(MAX),\n" // SHARD_ID_BYTES
          + "        `%s` DATE,\n" // SHARD_ID_DATE
          + "        `%s` FLOAT64,\n" // SHARD_ID_FLOAT64
          + "        `%s` INT64,\n" // SHARD_ID_INT64
          + "        `%s` STRING(MAX),\n" // SHARD_ID_STRING
          + "        `%s` TIMESTAMP,\n" // SHARD_ID_TIMESTAMP
          + "        `%s` TIMESTAMP NOT NULL\n" // LAST_SEEN_COMMIT_TIMESTAMP
          + ") PRIMARY KEY (`%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`)";
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
    private String shardIdBoolCol = DEFAULT_SHARD_ID_BOOL_COLUMN_NAME;
    private String shardIdBytesCol = DEFAULT_SHARD_ID_BYTES_COLUMN_NAME;
    private String shardIdDateCol = DEFAULT_SHARD_ID_DATE_COLUMN_NAME;
    private String shardIdFloat64Col = DEFAULT_SHARD_ID_FLOAT64_COLUMN_NAME;
    private String shardIdInt64Col = DEFAULT_SHARD_ID_INT64_COLUMN_NAME;
    private String shardIdStringCol = DEFAULT_SHARD_ID_STRING_COLUMN_NAME;
    private String shardIdTimestampCol = DEFAULT_SHARD_ID_TIMESTAMP_COLUMN_NAME;
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

    public Builder setShardIdBoolColumn(String column) {
      this.shardIdBoolCol = column;
      return this;
    }

    public Builder setShardIdBytesColumn(String column) {
      this.shardIdBytesCol = column;
      return this;
    }

    public Builder setShardIdDateColumn(String column) {
      this.shardIdDateCol = column;
      return this;
    }

    public Builder setShardIdFloat64Column(String column) {
      this.shardIdFloat64Col = column;
      return this;
    }

    public Builder setShardIdInt64Column(String column) {
      this.shardIdInt64Col = column;
      return this;
    }

    public Builder setShardIdStringColumn(String column) {
      this.shardIdStringCol = column;
      return this;
    }

    public Builder setShardIdTimestampColumn(String column) {
      this.shardIdTimestampCol = column;
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
  private final String shardIdBoolCol;
  private final String shardIdBytesCol;
  private final String shardIdDateCol;
  private final String shardIdFloat64Col;
  private final String shardIdInt64Col;
  private final String shardIdStringCol;
  private final String shardIdTimestampCol;
  private final String tsCol;
  private final Iterable<String> tsColumns;
  private boolean initialized = false;
  private Timestamp initialCommitTimestamp;

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
    this.shardIdBoolCol = builder.shardIdBoolCol;
    this.shardIdBytesCol = builder.shardIdBytesCol;
    this.shardIdDateCol = builder.shardIdDateCol;
    this.shardIdFloat64Col = builder.shardIdFloat64Col;
    this.shardIdInt64Col = builder.shardIdInt64Col;
    this.shardIdStringCol = builder.shardIdStringCol;
    this.shardIdTimestampCol = builder.shardIdTimestampCol;
    this.tsCol = builder.tsCol;
    this.initialCommitTimestamp = builder.initialCommitTimestamp;
    this.tsColumns = Collections.singleton(builder.tsCol);
  }

  /** Checks that the table is present and contains the actually expected columns. */
  private void initialize() {
    if (initialCommitTimestamp == null) {
      try (ResultSet rs =
          client.singleUse().executeQuery(Statement.of("SELECT CURRENT_TIMESTAMP"))) {
        while (rs.next()) {
          initialCommitTimestamp = rs.getTimestamp(0);
        }
      }
    }
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
            shardIdBoolCol,
            shardIdBytesCol,
            shardIdDateCol,
            shardIdFloat64Col,
            shardIdInt64Col,
            shardIdStringCol,
            shardIdTimestampCol,
            tsCol,
            databaseCol,
            catalogCol,
            schemaCol,
            tableCol,
            shardIdBoolCol,
            shardIdBytesCol,
            shardIdDateCol,
            shardIdFloat64Col,
            shardIdInt64Col,
            shardIdStringCol,
            shardIdTimestampCol);
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
    boolean foundShardIdBoolCol = false;
    boolean foundShardIdBytesCol = false;
    boolean foundShardIdDateCol = false;
    boolean foundShardIdFloat64Col = false;
    boolean foundShardIdInt64Col = false;
    boolean foundShardIdStringCol = false;
    boolean foundShardIdTimestampCol = false;
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
        } else if (col.equalsIgnoreCase(shardIdBoolCol)) {
          if (!rs.getString("SPANNER_TYPE").equals("BOOL")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Bool shard column %s is not of type BOOL, but of type %s",
                    shardIdBoolCol, rs.getString("SPANNER_TYPE")));
          }
          foundShardIdBoolCol = true;
        } else if (col.equalsIgnoreCase(shardIdBytesCol)) {
          if (!rs.getString("SPANNER_TYPE").startsWith("BYTES")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Bytes shard column %s is not of type BYTES, but of type %s",
                    shardIdBytesCol, rs.getString("SPANNER_TYPE")));
          }
          foundShardIdBytesCol = true;
        } else if (col.equalsIgnoreCase(shardIdDateCol)) {
          if (!rs.getString("SPANNER_TYPE").equals("DATE")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Date shard column %s is not of type DATE, but of type %s",
                    shardIdDateCol, rs.getString("SPANNER_TYPE")));
          }
          foundShardIdDateCol = true;
        } else if (col.equalsIgnoreCase(shardIdFloat64Col)) {
          if (!rs.getString("SPANNER_TYPE").equals("FLOAT64")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Float64 shard column %s is not of type FLOAT64, but of type %s",
                    shardIdFloat64Col, rs.getString("SPANNER_TYPE")));
          }
          foundShardIdFloat64Col = true;
        } else if (col.equalsIgnoreCase(shardIdInt64Col)) {
          if (!rs.getString("SPANNER_TYPE").equals("INT64")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Int64 shard column %s is not of type INT64, but of type %s",
                    shardIdInt64Col, rs.getString("SPANNER_TYPE")));
          }
          foundShardIdInt64Col = true;
        } else if (col.equalsIgnoreCase(shardIdStringCol)) {
          if (!rs.getString("SPANNER_TYPE").startsWith("STRING")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "String shard column %s is not of type STRING, but of type %s",
                    shardIdStringCol, rs.getString("SPANNER_TYPE")));
          }
          foundShardIdStringCol = true;
        } else if (col.equalsIgnoreCase(shardIdTimestampCol)) {
          if (!rs.getString("SPANNER_TYPE").equals("TIMESTAMP")) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                String.format(
                    "Timestamp shard column %s is not of type TIMESTAMP, but of type %s",
                    shardIdTimestampCol, rs.getString("SPANNER_TYPE")));
          }
          foundShardIdTimestampCol = true;
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
    if (!foundShardIdBoolCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Bool shard column %s not found", shardIdBoolCol));
    }
    if (!foundShardIdBytesCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Bytes shard column %s not found", shardIdBytesCol));
    }
    if (!foundShardIdDateCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Date shard column %s not found", shardIdDateCol));
    }
    if (!foundShardIdFloat64Col) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND,
          String.format("Float64 shard column %s not found", shardIdFloat64Col));
    }
    if (!foundShardIdInt64Col) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("Int64 shard column %s not found", shardIdInt64Col));
    }
    if (!foundShardIdStringCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND, String.format("String shard column %s not found", shardIdStringCol));
    }
    if (!foundShardIdTimestampCol) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.NOT_FOUND,
          String.format("Timestamp shard column %s not found", shardIdTimestampCol));
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
    String[] expectedCols =
        new String[] {
          databaseCol,
          catalogCol,
          schemaCol,
          tableCol,
          shardIdBoolCol,
          shardIdBytesCol,
          shardIdDateCol,
          shardIdFloat64Col,
          shardIdInt64Col,
          shardIdStringCol,
          shardIdTimestampCol
        };
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
                "Table %s does has a primary key with too few columns. The primary key of the table must be (`%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`).",
                commitTimestampsTable,
                databaseCol,
                catalogCol,
                schemaCol,
                tableCol,
                shardIdBoolCol,
                shardIdBytesCol,
                shardIdDateCol,
                shardIdFloat64Col,
                shardIdInt64Col,
                shardIdStringCol,
                shardIdTimestampCol));
      }
    }
  }

  @Override
  public Timestamp get(TableId table) {
    return internalGet(table, null);
  }

  @Override
  public Timestamp get(TableId table, Value shardValue) {
    return internalGet(table, shardValue);
  }

  private Timestamp internalGet(TableId table, Value shardValue) {
    Preconditions.checkNotNull(table);
    if (!initialized) {
      initialize();
    }
    Type.Code t = shardValue == null ? null : shardValue.getType().getCode();
    Struct row =
        client
            .singleUse()
            .readRow(
                commitTimestampsTable,
                Key.of(
                    table.getDatabaseId().getName(),
                    table.getCatalog(),
                    table.getSchema(),
                    table.getTable(),
                    t == Code.BOOL ? shardValue.getBool() : null,
                    t == Code.BYTES ? shardValue.getBytes() : null,
                    t == Code.DATE ? shardValue.getDate() : null,
                    t == Code.FLOAT64 ? shardValue.getFloat64() : null,
                    t == Code.INT64 ? shardValue.getInt64() : null,
                    shardValueToString(t, shardValue),
                    t == Code.TIMESTAMP ? shardValue.getTimestamp() : null),
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
    internalSet(table, null, commitTimestamp);
  }

  @Override
  public void set(TableId table, Value shardValue, Timestamp commitTimestamp) {
    internalSet(table, shardValue, commitTimestamp);
  }

  private void internalSet(TableId table, Value shardValue, Timestamp commitTimestamp) {
    Preconditions.checkNotNull(table);
    Preconditions.checkNotNull(commitTimestamp);
    if (!initialized) {
      initialize();
    }
    Type.Code t = shardValue == null ? null : shardValue.getType().getCode();
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
                .set(shardIdBoolCol)
                .to(t == Code.BOOL ? shardValue.getBool() : null)
                .set(shardIdBytesCol)
                .to(t == Code.BYTES ? shardValue.getBytes() : null)
                .set(shardIdDateCol)
                .to(t == Code.DATE ? shardValue.getDate() : null)
                .set(shardIdFloat64Col)
                .to(t == Code.FLOAT64 ? shardValue.getFloat64() : null)
                .set(shardIdInt64Col)
                .to(t == Code.INT64 ? shardValue.getInt64() : null)
                .set(shardIdStringCol)
                .to(shardValueToString(t, shardValue))
                .set(shardIdTimestampCol)
                .to(t == Code.TIMESTAMP ? shardValue.getTimestamp() : null)
                .set(tsCol)
                .to(commitTimestamp)
                .build()));
  }

  /** Calculates the shard string value for the given {@link Value}. */
  static String shardValueToString(Type.Code type, Value shardValue) {
    if (type == null) {
      return null;
    }
    switch (type) {
      case ARRAY:
        return arrayToString(shardValue);
      case NUMERIC:
        return shardValue.getNumeric().toString();
      case JSON:
        return shardValue.getJson();
      case STRING:
        return shardValue.getString();

        // The following types have their own specific columns.
      case BOOL:
      case BYTES:
      case DATE:
      case FLOAT64:
      case INT64:
      case STRUCT:
      case TIMESTAMP:
      default:
        return null;
    }
  }

  static String arrayToString(Value value) {
    Preconditions.checkNotNull(value);
    Preconditions.checkArgument(value.getType().getCode() == Type.Code.ARRAY);
    switch (value.getType().getArrayElementType().getCode()) {
      case BOOL:
        return value.getBoolArray().stream()
            .map(b -> b.toString())
            .collect(Collectors.joining(","));
      case BYTES:
        return value.getBytesArray().stream()
            .map(b -> b.toBase64())
            .collect(Collectors.joining(","));
      case DATE:
        return value.getDateArray().stream()
            .map(b -> b.toString())
            .collect(Collectors.joining(","));
      case FLOAT64:
        return value.getFloat64Array().stream()
            .map(b -> b.toString())
            .collect(Collectors.joining(","));
      case INT64:
        return value.getInt64Array().stream()
            .map(b -> b.toString())
            .collect(Collectors.joining(","));
      case JSON:
        return value.getJsonArray().stream()
            .map(b -> Base64.getEncoder().encodeToString(b.getBytes(UTF8)))
            .collect(Collectors.joining(","));
      case NUMERIC:
        return value.getNumericArray().stream()
            .map(b -> b.toString())
            .collect(Collectors.joining(","));
      case STRING:
        return value.getStringArray().stream()
            .map(b -> Base64.getEncoder().encodeToString(b.getBytes(UTF8)))
            .collect(Collectors.joining(","));
      case TIMESTAMP:
        return value.getTimestampArray().stream()
            .map(b -> b.toString())
            .collect(Collectors.joining(","));
      case ARRAY:
      case STRUCT:
      default:
        return null;
    }
  }
}
