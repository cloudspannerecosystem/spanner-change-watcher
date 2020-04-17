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

import com.google.api.core.ApiFunction;
import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public abstract class AbstractMockServerTest {
  // Statements for SpannerCommitTimestampRepository
  private static final Statement FIND_LAST_SEEN_COMMIT_TIMESTAMPS_TABLE_STATEMENT =
      Statement.newBuilder(SpannerCommitTimestampRepository.FIND_TABLE_STATEMENT)
          .bind("catalog")
          .to(SpannerCommitTimestampRepository.DEFAULT_TABLE_CATALOG)
          .bind("schema")
          .to(SpannerCommitTimestampRepository.DEFAULT_TABLE_SCHEMA)
          .bind("table")
          .to(SpannerCommitTimestampRepository.DEFAULT_TABLE_NAME)
          .build();
  private static final Statement FIND_LAST_SEEN_COMMIT_TIMESTAMPS_COLUMNS_STATEMENT =
      Statement.newBuilder(SpannerCommitTimestampRepository.FIND_COLUMNS_STATEMENT)
          .bind("catalog")
          .to(SpannerCommitTimestampRepository.DEFAULT_TABLE_CATALOG)
          .bind("schema")
          .to(SpannerCommitTimestampRepository.DEFAULT_TABLE_SCHEMA)
          .bind("table")
          .to(SpannerCommitTimestampRepository.DEFAULT_TABLE_NAME)
          .build();
  private static final Statement FIND_LAST_SEEN_COMMIT_TIMESTAMPS_PK_STATEMENT =
      Statement.newBuilder(SpannerCommitTimestampRepository.FIND_PK_COLUMNS_STATEMENT)
          .bind("catalog")
          .to(SpannerCommitTimestampRepository.DEFAULT_TABLE_CATALOG)
          .bind("schema")
          .to(SpannerCommitTimestampRepository.DEFAULT_TABLE_SCHEMA)
          .bind("table")
          .to(SpannerCommitTimestampRepository.DEFAULT_TABLE_NAME)
          .build();
  private static final Statement GET_LAST_COMMIT_TIMESTAMP_STATEMENT =
      Statement.of(
          String.format(
              "SELECT %s FROM %s WHERE ID=1",
              SpannerCommitTimestampRepository.DEFAULT_COMMIT_TIMESTAMP_COLUMN_NAME,
              SpannerCommitTimestampRepository.DEFAULT_TABLE_NAME));
  private static final ResultSetMetadata FIND_LAST_SEEN_COMMIT_TIMESTAMPS_TABLE_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("TABLE_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet
      FIND_LAST_SEEN_COMMIT_TIMESTAMPS_TABLE_RESULTS =
          com.google.spanner.v1.ResultSet.newBuilder()
              .addRows(
                  ListValue.newBuilder()
                      .addValues(
                          Value.newBuilder()
                              .setStringValue(SpannerCommitTimestampRepository.DEFAULT_TABLE_NAME)
                              .build())
                      .build())
              .setMetadata(FIND_LAST_SEEN_COMMIT_TIMESTAMPS_TABLE_METADATA)
              .build();
  private static final ResultSetMetadata FIND_LAST_SEEN_COMMIT_TIMESTAMPS_COLUMNS_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("COLUMN_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("SPANNER_TYPE")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet
      FIND_LAST_SEEN_COMMIT_TIMESTAMPS_COLUMNS_RESULTS =
          com.google.spanner.v1.ResultSet.newBuilder()
              .addRows(
                  ListValue.newBuilder()
                      .addValues(
                          Value.newBuilder()
                              .setStringValue(
                                  SpannerCommitTimestampRepository
                                      .DEFAULT_DATABASE_NAME_COLUMN_NAME)
                              .build())
                      .addValues(Value.newBuilder().setStringValue("STRING(MAX)").build())
                      .build())
              .addRows(
                  ListValue.newBuilder()
                      .addValues(
                          Value.newBuilder()
                              .setStringValue(
                                  SpannerCommitTimestampRepository
                                      .DEFAULT_TABLE_CATALOG_COLUMN_NAME)
                              .build())
                      .addValues(Value.newBuilder().setStringValue("STRING(MAX)").build())
                      .build())
              .addRows(
                  ListValue.newBuilder()
                      .addValues(
                          Value.newBuilder()
                              .setStringValue(
                                  SpannerCommitTimestampRepository.DEFAULT_TABLE_SCHEMA_COLUMN_NAME)
                              .build())
                      .addValues(Value.newBuilder().setStringValue("STRING(MAX)").build())
                      .build())
              .addRows(
                  ListValue.newBuilder()
                      .addValues(
                          Value.newBuilder()
                              .setStringValue(
                                  SpannerCommitTimestampRepository.DEFAULT_TABLE_NAME_COLUMN_NAME)
                              .build())
                      .addValues(Value.newBuilder().setStringValue("STRING(MAX)").build())
                      .build())
              .addRows(
                  ListValue.newBuilder()
                      .addValues(
                          Value.newBuilder()
                              .setStringValue(
                                  SpannerCommitTimestampRepository
                                      .DEFAULT_COMMIT_TIMESTAMP_COLUMN_NAME)
                              .build())
                      .addValues(Value.newBuilder().setStringValue("TIMESTAMP").build())
                      .build())
              .setMetadata(FIND_LAST_SEEN_COMMIT_TIMESTAMPS_COLUMNS_METADATA)
              .build();
  private static final ResultSetMetadata FIND_LAST_SEEN_COMMIT_TIMESTAMPS_PK_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("COLUMN_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet FIND_LAST_SEEN_COMMIT_TIMESTAMPS_PK_RESULTS =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(
                              SpannerCommitTimestampRepository.DEFAULT_DATABASE_NAME_COLUMN_NAME)
                          .build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(
                              SpannerCommitTimestampRepository.DEFAULT_TABLE_CATALOG_COLUMN_NAME)
                          .build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(
                              SpannerCommitTimestampRepository.DEFAULT_TABLE_SCHEMA_COLUMN_NAME)
                          .build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(
                              SpannerCommitTimestampRepository.DEFAULT_TABLE_NAME_COLUMN_NAME)
                          .build())
                  .build())
          .setMetadata(FIND_LAST_SEEN_COMMIT_TIMESTAMPS_PK_METADATA)
          .build();
  private static final ResultSetMetadata GET_LAST_SEEN_COMMIT_TIMESTAMP_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName(
                              SpannerCommitTimestampRepository.DEFAULT_COMMIT_TIMESTAMP_COLUMN_NAME)
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet GET_LAST_SEEN_COMMIT_TIMESTAMP_RESULTS =
      com.google.spanner.v1.ResultSet.newBuilder()
          .setMetadata(GET_LAST_SEEN_COMMIT_TIMESTAMP_METADATA)
          .build();

  // Statements for SpannerDatabaseTailer
  private static final Statement FIND_ALL_TABLES_STATEMENT =
      Statement.newBuilder(SpannerDatabaseTailer.LIST_TABLE_NAMES_STATEMENT)
          .bind("excluded")
          .toStringArray(Collections.<String>emptyList())
          .bind("allTables")
          .to(true)
          .bind("included")
          .toStringArray(Collections.<String>emptyList())
          .bind("schema")
          .to("")
          .bind("catalog")
          .to("")
          .build();
  private static final ResultSetMetadata FIND_TABLES_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("TABLE_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet FIND_ALL_TABLES_RESULT =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("Foo").build())
                  .build())
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("Bar").build())
                  .build())
          .setMetadata(FIND_TABLES_METADATA)
          .build();
  private static final Statement FIND_FOO_BAR_NON_EXISTING_TABLE_TABLES_STATEMENT =
      Statement.newBuilder(SpannerDatabaseTailer.LIST_TABLE_NAMES_STATEMENT)
          .bind("excluded")
          .toStringArray(Collections.<String>emptyList())
          .bind("allTables")
          .to(false)
          .bind("included")
          .toStringArray(Arrays.asList("Foo", "Bar", "NonExistingTable"))
          .bind("schema")
          .to("")
          .bind("catalog")
          .to("")
          .build();
  private static final ResultSetMetadata COLUMNS_OPTIONS_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("COLUMN_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("OPTION_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("OPTION_VALUE")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();

  // Poll Foo statements.
  private static final Statement COLUMN_OPTIONS_FOO_STATEMENT =
      Statement.newBuilder(SpannerUtils.FIND_COMMIT_TIMESTAMP_COLUMN_QUERY)
          .bind("catalog")
          .to("")
          .bind("schema")
          .to("")
          .bind("table")
          .to("Foo")
          .build();
  public static final Statement SELECT_FOO_STATEMENT =
      Statement.newBuilder(
              String.format(SpannerTableTailer.POLL_QUERY, "`Foo`", "LastModified", "LastModified"))
          .bind("prevCommitTimestamp")
          .to(Timestamp.MIN_VALUE)
          .build();
  public static final int SELECT_FOO_ROW_COUNT = 10;
  private static final com.google.spanner.v1.ResultSet COLUMNS_OPTIONS_FOO_RESULT =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("LastModified").build())
                  .addValues(Value.newBuilder().setStringValue("allow_commit_timestamp").build())
                  .addValues(Value.newBuilder().setStringValue("TRUE").build())
                  .build())
          .setMetadata(COLUMNS_OPTIONS_METADATA)
          .build();

  // Poll Bar statements.
  private static final Statement COLUMN_OPTIONS_BAR_STATEMENT =
      Statement.newBuilder(SpannerUtils.FIND_COMMIT_TIMESTAMP_COLUMN_QUERY)
          .bind("catalog")
          .to("")
          .bind("schema")
          .to("")
          .bind("table")
          .to("Bar")
          .build();
  public static final Statement SELECT_BAR_STATEMENT =
      Statement.newBuilder(
              String.format(SpannerTableTailer.POLL_QUERY, "`Bar`", "LastModified", "LastModified"))
          .bind("prevCommitTimestamp")
          .to(Timestamp.MIN_VALUE)
          .build();
  public static final int SELECT_BAR_ROW_COUNT = 20;
  private static final com.google.spanner.v1.ResultSet COLUMNS_OPTIONS_BAR_RESULT =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("LastModified").build())
                  .addValues(Value.newBuilder().setStringValue("allow_commit_timestamp").build())
                  .addValues(Value.newBuilder().setStringValue("TRUE").build())
                  .build())
          .setMetadata(COLUMNS_OPTIONS_METADATA)
          .build();

  // Poll NonExistentTable results.
  private static final Statement COLUMN_OPTIONS_NON_EXISTING_TABLE_STATEMENT =
      Statement.newBuilder(SpannerUtils.FIND_COMMIT_TIMESTAMP_COLUMN_QUERY)
          .bind("catalog")
          .to("")
          .bind("schema")
          .to("")
          .bind("table")
          .to("NonExistingTable")
          .build();
  private static final com.google.spanner.v1.ResultSet COLUMNS_OPTIONS_NON_EXISTING_TABLE_RESULT =
      com.google.spanner.v1.ResultSet.newBuilder().setMetadata(COLUMNS_OPTIONS_METADATA).build();

  protected static MockSpannerServiceImpl mockSpanner;
  private static Server server;
  private static InetSocketAddress address;
  private Spanner spanner;
  private Statement currentFooPollStatement;
  private Statement currentBarPollStatement;

  @BeforeClass
  public static void startStaticServer() throws IOException {
    mockSpanner = new MockSpannerServiceImpl();
    mockSpanner.setAbortProbability(0.0D); // We don't want any unpredictable aborted transactions.
    address = new InetSocketAddress("localhost", 0);
    server = NettyServerBuilder.forAddress(address).addService(mockSpanner).build().start();
  }

  @AfterClass
  public static void stopServer() throws Exception {
    server.shutdown();
    server.awaitTermination();
  }

  @After
  public void reset() {
    spanner.close();
    mockSpanner.reset();
  }

  @Before
  public void setupResults() {
    // SpannerCommitTimestampRepository results.
    mockSpanner.putStatementResult(
        StatementResult.query(
            FIND_LAST_SEEN_COMMIT_TIMESTAMPS_TABLE_STATEMENT,
            FIND_LAST_SEEN_COMMIT_TIMESTAMPS_TABLE_RESULTS));
    mockSpanner.putStatementResult(
        StatementResult.query(
            FIND_LAST_SEEN_COMMIT_TIMESTAMPS_COLUMNS_STATEMENT,
            FIND_LAST_SEEN_COMMIT_TIMESTAMPS_COLUMNS_RESULTS));
    mockSpanner.putStatementResult(
        StatementResult.query(
            FIND_LAST_SEEN_COMMIT_TIMESTAMPS_PK_STATEMENT,
            FIND_LAST_SEEN_COMMIT_TIMESTAMPS_PK_RESULTS));
    mockSpanner.putStatementResult(
        StatementResult.query(
            GET_LAST_COMMIT_TIMESTAMP_STATEMENT, GET_LAST_SEEN_COMMIT_TIMESTAMP_RESULTS));

    // SpannerDatabaseTailer results.
    mockSpanner.putStatementResult(
        StatementResult.query(FIND_ALL_TABLES_STATEMENT, FIND_ALL_TABLES_RESULT));
    mockSpanner.putStatementResult(
        StatementResult.query(
            FIND_FOO_BAR_NON_EXISTING_TABLE_TABLES_STATEMENT, FIND_ALL_TABLES_RESULT));

    // Poll Foo results.
    mockSpanner.putStatementResult(
        StatementResult.query(COLUMN_OPTIONS_FOO_STATEMENT, COLUMNS_OPTIONS_FOO_RESULT));
    ResultSet fooResults = new RandomResultSetGenerator(SELECT_FOO_ROW_COUNT).generate();
    Timestamp maxFooCommitTimestamp = RandomResultSetGenerator.getMaxCommitTimestamp(fooResults);
    currentFooPollStatement =
        SELECT_FOO_STATEMENT
            .toBuilder()
            .bind("prevCommitTimestamp")
            .to(maxFooCommitTimestamp)
            .build();
    mockSpanner.putStatementResults(
        StatementResult.query(SELECT_FOO_STATEMENT, fooResults),
        StatementResult.query(currentFooPollStatement, new RandomResultSetGenerator(0).generate()));
    // Poll Bar results.
    mockSpanner.putStatementResult(
        StatementResult.query(COLUMN_OPTIONS_BAR_STATEMENT, COLUMNS_OPTIONS_BAR_RESULT));
    ResultSet barResults = new RandomResultSetGenerator(SELECT_BAR_ROW_COUNT).generate();
    Timestamp maxBarCommitTimestamp = RandomResultSetGenerator.getMaxCommitTimestamp(barResults);
    currentBarPollStatement =
        SELECT_BAR_STATEMENT
            .toBuilder()
            .bind("prevCommitTimestamp")
            .to(maxBarCommitTimestamp)
            .build();
    mockSpanner.putStatementResults(
        StatementResult.query(SELECT_BAR_STATEMENT, barResults),
        StatementResult.query(currentBarPollStatement, new RandomResultSetGenerator(0).generate()));
    // Poll NonExistentTable results.
    mockSpanner.putStatementResult(
        StatementResult.query(
            COLUMN_OPTIONS_NON_EXISTING_TABLE_STATEMENT,
            COLUMNS_OPTIONS_NON_EXISTING_TABLE_RESULT));

    spanner = createSpanner();
  }

  @SuppressWarnings("rawtypes")
  private static Spanner createSpanner() {
    return SpannerOptions.newBuilder()
        .setProjectId("project-id")
        .setCredentials(NoCredentials.getInstance())
        .setHost("http://localhost:" + server.getPort())
        .setChannelConfigurator(
            new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
              @Override
              public ManagedChannelBuilder apply(ManagedChannelBuilder input) {
                return input.usePlaintext();
              }
            })
        .build()
        .getService();
  }

  protected Spanner getSpanner() {
    return spanner;
  }

  protected Statement getCurrentFooPollStatement() {
    return currentFooPollStatement;
  }

  protected Statement getCurrentBarPollStatement() {
    return currentBarPollStatement;
  }
}
