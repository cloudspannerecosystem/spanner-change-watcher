/*
 * Copyright 2021 Google LLC
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

package com.google.cloud.spanner.watcher.sample.benchmarkapp;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Strings;
import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;
import com.google.devtools.common.options.OptionsParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

/**
 * This sample application can be used to benchmark and test different settings for Spanner Change
 * Watcher. The example in this application can be seen as a best-practice for applications that
 * have a high write rate and need to be able to watch and report a large number of changes
 * efficiently.
 *
 * <p>The sample application uses two classes:
 *
 * <p>1. An Updater: An Updater executes write transactions that write random sample data to the
 * sample table. The rate of transactions per second and the number of mutations per transaction can
 * be configured.
 *
 * <p>2. One or more Watchers: A Watcher watches the sample table for mutations and writes these to
 * the standard console.
 */
public class Main {
  public static class BenchmarkOptions extends OptionsBase {
    @Option(
        name = "instance",
        abbrev = 'i',
        help =
            "Required: The name of the Cloud Spanner instance to use. This instance must already exist.",
        defaultValue = "")
    public String instance;

    @Option(
        name = "database",
        abbrev = 'd',
        help =
            "Required: The name of the Cloud Spanner database to use. The database will be created if it does not already exist.",
        defaultValue = "")
    public String database;

    @Option(
        name = "table",
        abbrev = 't',
        help =
            "The name of the table to update and watch. Defaults to Singers. The table will be created if it does not already exist.",
        defaultValue = "Singers")
    public String table;

    @Option(
        name = "index",
        abbrev = 'x',
        help =
            "The name of the secondary index to define on the table and to use for the poll query. Defaults to Idx_Singers_ShardId_LastUpdated. The index will be created if it does not already exist. Set this option to an empty string to disable the creation and usage of a secondary index. This will have a negative impact on the performance of the poll query if the table contains a large number of rows.",
        defaultValue = "Idx_Singers_ShardId_LastUpdated")
    public String index;

    @Option(
        name = "writeTransactionsPerSecond",
        abbrev = 'w',
        help = "The number of write transactions per second to execute",
        defaultValue = "1")
    public double writeTransactionsPerSecond;

    @Option(
        name = "mutationsPerTransaction",
        abbrev = 'm',
        help =
            "The number of mutations to execute per write transaction. The total number of mutations per second is transactionsPerSecond*mutationsPerTransaction.",
        defaultValue = "5")
    public int mutationsPerTransaction;

    @Option(
        name = "updateParallelism",
        abbrev = 'u',
        help = "The number of parallel threads to use to execute write transactions.",
        defaultValue = "32")
    public int updateParallelism;

    @Option(
        name = "numWatchers",
        abbrev = 'n',
        help =
            "The number of watchers to start. The available shards will be evenly distributed among the watchers.",
        defaultValue = "1")
    public int numWatchers;

    @Option(
        name = "disableShardProvider",
        help =
            "Disables the use of a shard provider. This will make the poll query significantly less efficient for large tables. Set this option to true to compare the performance with/without a shard provider. This option can only be used with numWatchers=1.",
        defaultValue = "false")
    public boolean disableSharding;

    @Option(
        name = "disableTableHint",
        help =
            "Disables the use of a table hint. This can make the poll query significantly less efficient for large tables. Set this option to true to compare the performance with/without a table hint.",
        defaultValue = "false")
    public boolean disableTableHint;

    @Option(
        name = "pollInterval",
        abbrev = 'p',
        help =
            "The poll interval to use for the watchers. Defaults to 1 second (PT1S). Duration should be specified in ISO-8601 duration format (PnDTnHnMn.nS).",
        defaultValue = "PT1S")
    public String pollInterval;

    @Option(
        name = "limit",
        abbrev = 'l',
        help = "The maximum number of changes that a watcher should fetch during a poll.",
        defaultValue = "10000")
    public long limit;

    @Option(
        name = "fallbackToWithQuery",
        abbrev = 'f',
        help = "The number of seconds poll latency that should trigger a switch to a WITH query.",
        defaultValue = "60")
    public int fallbackToWithQuery;

    @Option(
        name = "resetLastSeenCommitTimestamp",
        abbrev = 'r',
        help =
            "Resets the last seen commit timestamp of the Watcher to the current time. Use this option if previous benchmark runs have caused the watcher to lag far behind the latest commit timestamp.",
        defaultValue = "false")
    public boolean resetLastSeenCommitTimestamp;

    @Option(
        name = "dropExistingTable",
        help =
            "Drops and re-creates the data table if it already exists. CAUTION: DO NOT USE THIS OPTION ON PRODUCTION DATA.",
        defaultValue = "false")
    public boolean dropExistingTable;

    @Option(
        name = "simple",
        abbrev = 's',
        help =
            "Print single line status without colors. Use this if your console does not support control characters.",
        defaultValue = "false")
    public boolean simpleStatus;
  }

  /**
   * Main method for the sample application. See {@link BenchmarkOptions} for supported arguments.
   */
  public static void main(String[] args) {
    OptionsParser parser = OptionsParser.newOptionsParser(BenchmarkOptions.class);
    parser.parseAndExitUponError(args);
    BenchmarkOptions options = parser.getOptions(BenchmarkOptions.class);
    if (Strings.isNullOrEmpty(options.instance)
        || Strings.isNullOrEmpty(options.database)
        || Strings.isNullOrEmpty(options.table)
        || options.writeTransactionsPerSecond < 0
        || options.mutationsPerTransaction < 1
        || options.updateParallelism < 1
        || options.numWatchers < 1
        || options.limit <= 0L
        || options.fallbackToWithQuery <= 0) {
      printUsage(parser);
      return;
    }
    if (options.disableSharding && options.numWatchers != 1) {
      throw new IllegalArgumentException(
          "disableSharding=true can only be used in combination with numWatchers=1");
    }
    // Fallback to simple status output when there's no supported console available.
    if (System.console() == null || System.getenv().get("TERM") == null) {
      options.simpleStatus = true;
    }

    // Create table and index if any of these do not already exist.
    try {
      createTableAndIndexIfNotExists(options);
    } catch (InterruptedException e) {
      System.err.println("Table and/or index creation was interrupted.");
      return;
    } catch (ExecutionException e) {
      System.err.println("Table and/or index creation failed:");
      e.printStackTrace(System.err);
      return;
    }

    // Start the benchmark application.
    try {
      System.out.printf("Starting benchmark application\n");
      BenchmarkApplication application = new BenchmarkApplication(options);
      application.run();
    } catch (IOException e) {
      System.err.println("Starting the benchmark application failed:");
      e.printStackTrace(System.err);
      return;
    }
  }

  /** The example table that is used by this benchmark application. */
  private static final String TABLE_DDL =
      "CREATE TABLE `%s` (\n"
          + "  SingerId     INT64 NOT NULL,\n"
          + "  FirstName    STRING(200),\n"
          + "  LastName     STRING(200) NOT NULL,\n"
          + "  BirthDate    DATE,\n"
          + "  Picture      BYTES(MAX),\n"
          + "  LastUpdated  TIMESTAMP OPTIONS (allow_commit_timestamp=true),\n"
          + "  Processed    BOOL,\n"
          + "  ShardId      INT64 AS (\n"
          + "    CASE\n"
          + "      WHEN Processed THEN NULL\n"
          + "      ELSE MOD(FARM_FINGERPRINT(COALESCE(FirstName || ' ', '') || LastName), 19)\n"
          + "    END) STORED,\n"
          + ") PRIMARY KEY (SingerId)";

  /** The example index that is used by this benchmark application. */
  private static final String INDEX_DDL =
      "CREATE NULL_FILTERED INDEX `%s`\n" + "  ON `%s` (ShardId, LastUpdated)";

  private static void createTableAndIndexIfNotExists(BenchmarkOptions options)
      throws InterruptedException, ExecutionException {
    System.out.println("Checking whether database, table and/or index needs to be created");

    try (Spanner spanner = SpannerOptions.newBuilder().build().getService()) {
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
      try {
        dbAdminClient.getDatabase(options.instance, options.database);
        System.out.printf("Database `%s` found.\n", options.database);
      } catch (DatabaseNotFoundException e) {
        dbAdminClient
            .createDatabase(options.instance, options.database, Collections.emptyList())
            .get();
      }

      DatabaseClient client =
          spanner.getDatabaseClient(
              DatabaseId.of(
                  spanner.getOptions().getProjectId(), options.instance, options.database));
      List<String> statements = new ArrayList<>();
      try (ResultSet rs =
          client
              .singleUse()
              .executeQuery(
                  Statement.newBuilder(
                          "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=@name")
                      .bind("name")
                      .to(options.table)
                      .build())) {
        if (!rs.next()) {
          System.out.printf("Table `%s` not found and needs to be created.\n", options.table);
          statements.add(String.format(TABLE_DDL, options.table));
        } else {
          System.out.printf("Table `%s` found.\n", options.table);
          if (options.dropExistingTable) {
            if (System.console() == null) {
              System.out.printf(
                  "The application will drop the existing table `%s` in 10 seconds. Terminate the application now if that is not the intention.\n",
                  options.table);
              Thread.sleep(10_000L);
            } else {
              System.out.printf(
                  "The application is about to drop the existing table `%s`. Press ENTER to continue.\n");
              try (Scanner scanner = new Scanner(System.in)) {
                scanner.nextLine();
              }
            }

            System.out.printf("Dropping and re-creating table `%s`.\n", options.table);
            for (String index : getExistingIndexes(client, options.table)) {
              statements.add(String.format("DROP INDEX `%s`", index));
            }
            statements.add(String.format("DROP TABLE `%s`", options.table));
            statements.add(String.format(TABLE_DDL, options.table));
          }
        }
      }
      if (!Strings.isNullOrEmpty(options.index)) {
        try (ResultSet rs =
            client
                .singleUse()
                .executeQuery(
                    Statement.newBuilder(
                            "SELECT 1 FROM INFORMATION_SCHEMA.INDEXES WHERE INDEX_NAME=@indexName AND TABLE_NAME=@tableName")
                        .bind("indexName")
                        .to(options.index)
                        .bind("tableName")
                        .to(options.table)
                        .build())) {
          if (!rs.next()) {
            System.out.printf("Index `%s` not found and needs to be created.\n", options.index);
            statements.add(String.format(INDEX_DDL, options.index, options.table));
          } else {
            System.out.printf("Index `%s` found.\n", options.index);
          }
        }
      }
      if (!statements.isEmpty()) {
        System.out.printf("Executing CREATE statements...\n");
        for (String sql : statements) {
          System.out.printf("%s\n\n", sql);
        }
        spanner
            .getDatabaseAdminClient()
            .updateDatabaseDdl(options.instance, options.database, statements, null)
            .get();
        System.out.printf("Finished executing CREATE statements\n");
      }
    }
  }

  private static List<String> getExistingIndexes(DatabaseClient client, String table) {
    List<String> indexes = new ArrayList<>();
    try (ResultSet rs =
        client
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME=@tableName")
                    .bind("tableName")
                    .to(table)
                    .build())) {
      while (rs.next()) {
        indexes.add(rs.getString(0));
      }
    }
    return indexes;
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: java -jar benchmark-application.jar OPTIONS");
    System.out.println(
        parser.describeOptions(
            Collections.<String, String>emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }
}
