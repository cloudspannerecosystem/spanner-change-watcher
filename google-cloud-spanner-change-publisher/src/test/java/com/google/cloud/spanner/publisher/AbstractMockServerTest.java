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

package com.google.cloud.spanner.publisher;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.watcher.RandomResultSetGenerator;
import org.junit.Before;

public abstract class AbstractMockServerTest
    extends com.google.cloud.spanner.watcher.AbstractMockServerTest {
  // SpannerToAvro statements.
  private static final Statement SPANNER_TO_AVRO_SCHEMA_FOO_STATEMENT =
      Statement.newBuilder(SpannerToAvroFactory.SCHEMA_QUERY)
          .bind("catalog")
          .to("")
          .bind("schema")
          .to("")
          .bind("table")
          .to("Foo")
          .build();
  private static final Statement SPANNER_TO_AVRO_SCHEMA_BAR_STATEMENT =
      Statement.newBuilder(SpannerToAvroFactory.SCHEMA_QUERY)
          .bind("catalog")
          .to("")
          .bind("schema")
          .to("")
          .bind("table")
          .to("Bar")
          .build();

  @Before
  public void setupAvroResults() {
    // SpannerToAvro results.
    mockSpanner.putStatementResult(
        StatementResult.query(
            SPANNER_TO_AVRO_SCHEMA_FOO_STATEMENT,
            RandomResultSetGenerator.generateRandomResultSetInformationSchemaResultSet()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SPANNER_TO_AVRO_SCHEMA_BAR_STATEMENT,
            RandomResultSetGenerator.generateRandomResultSetInformationSchemaResultSet()));
  }
}
