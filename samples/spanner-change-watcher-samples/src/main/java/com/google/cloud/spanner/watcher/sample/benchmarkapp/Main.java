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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
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
    @Option(name = "help", abbrev = 'h', help = "Prints usage info.", defaultValue = "true")
    public boolean help;

    @Option(
        name = "instance",
        abbrev = 'i',
        help = "Required: The name of the Cloud Spanner instance to use.",
        defaultValue = "")
    public String instance;

    @Option(
        name = "database",
        abbrev = 'd',
        help = "Required: The name of the Cloud Spanner database to use.",
        defaultValue = "")
    public String database;

    @Option(
        name = "table",
        abbrev = 't',
        help = "The name of the table to update and watch. Defaults to Singers.",
        defaultValue = "Singers")
    public String table;

    @Option(
        name = "index",
        abbrev = 'x',
        help =
            "The name of the secondary index to define on the table and to use for the poll query. Defaults to Idx_Singers_ShardId_LastModified.",
        defaultValue = "Idx_Singers_ShardId_LastModified")
    public String index;

    @Option(
        name = "writeTransactionsPerSecond",
        abbrev = 'w',
        help = "The number of write transactions per second to execute",
        defaultValue = "1")
    public int writeTransactionsPerSecond;

    @Option(
        name = "mutationsPerTransaction",
        abbrev = 'm',
        help =
            "The number of mutations to execute per write transaction. The total number of mutations per second is transactionsPerSecond*mutationsPerTransaction.",
        defaultValue = "5")
    public int mutationsPerTransaction;

    @Option(
        name = "updateParallelism",
        abbrev = 'p',
        help = "The number of parallel threads to use to execute write transactions.",
        defaultValue = "32")
    public int updateParallelism;

    @Option(
        name = "numWatchers",
        abbrev = 'n',
        help =
            "The number of watchers to start. The available shards will be evenly distributed among the watchers",
        defaultValue = "1")
    public int numWatchers;

    @Option(
        name = "limit",
        abbrev = 'l',
        help =
            "The maximum number of changes that a watcher should fetch during a poll. A zero or negative value means no limit.",
        defaultValue = "0")
    public long limit;
  }

  /**
   * Main method for the sample application. Supports the following arguments:
   *
   * <ul>
   *   <li>-i, --instance: (Required) The name of the Cloud Spanner instance to use.
   *   <li>-d, --database: (Required) The name of the Cloud Spanner database to use.
   *   <li>-t, --table: The name of the table to update and watch. Defaults to Singers.
   *   <li>-w, --writeTransactionPerSecond: The number of write transactions per second that the
   *       updater should execute. Defaults to 1.
   *   <li>-m, --mutationsPerTransaction: The number of mutations that each transaction should
   *       contain on average. The number of mutations per second is equal to tps * mpt.
   *   <li>-p, --updateParallelism: The number of threads to use for the updater. Defaults to 32.
   *   <li>-n, --numWatchers: The number of watchers that should be started. Defaults to 1. The
   *       available shards will be evenly distributed over the watchers.
   *   <li>-l, --limit: The maximum number of changes that a watcher should fetch during a poll.
   *       Defaults to no limit.
   * </ul>
   */
  public static void main(String[] args) {
    OptionsParser parser = OptionsParser.newOptionsParser(BenchmarkOptions.class);
    parser.parseAndExitUponError(args);
    BenchmarkOptions options = parser.getOptions(BenchmarkOptions.class);
    if (Strings.isNullOrEmpty(options.instance)
        || Strings.isNullOrEmpty(options.database)
        || Strings.isNullOrEmpty(options.table)
        || options.writeTransactionsPerSecond < 1
        || options.mutationsPerTransaction < 1
        || options.updateParallelism < 1
        || options.numWatchers < 1) {
      printUsage(parser);
      return;
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

  private static final String TABLE_DDL =
      "CREATE TABLE `%s` (\n"
          + "  SingerId     INT64 NOT NULL,\n"
          + "  FirstName    STRING(200),\n"
          + "  LastName     STRING(200) NOT NULL,\n"
          + "  LastModified TIMESTAMP OPTIONS (allow_commit_timestamp=true),\n"
          + "  Processed    BOOL,\n"
          + "  ShardId      INT64 AS (\n"
          + "    CASE\n"
          + "      WHEN Processed THEN NULL\n"
          + "      ELSE MOD(FARM_FINGERPRINT(COALESCE(FirstName || ' ', '') || LastName), 19)\n"
          + "    END) STORED,\n"
          + ") PRIMARY KEY (SingerId)";

  private static final String INDEX_DDL =
      "CREATE NULL_FILTERED INDEX `%s`\n" + "  ON `%s` (ShardId, LastModified)";

  private static void createTableAndIndexIfNotExists(BenchmarkOptions options)
      throws InterruptedException, ExecutionException {
    System.out.println("Checking whether table and/or index needs to be created");
    try (Spanner spanner = SpannerOptions.newBuilder().build().getService()) {
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
        }
      }
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

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: java -jar benchmark-application.jar OPTIONS");
    System.out.println(
        parser.describeOptions(
            Collections.<String, String>emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }
}
