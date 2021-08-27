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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.publisher.SpannerToAvroFactory.SpannerToAvro;
import com.google.cloud.spanner.publisher.SpannerToAvroFactory.SpannerToAvro.SchemaSet;
import com.google.cloud.spanner.watcher.SpannerUtils;
import com.google.cloud.spanner.watcher.TableId;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerToAvroTest {
  private static final Type SCHEMA_ROW_TYPE =
      Type.struct(
          StructField.of("COLUMN_NAME", Type.string()),
          StructField.of("SPANNER_TYPE", Type.string()),
          StructField.of("IS_NULLABLE", Type.string()));

  private static ResultSet createSchemaResultSet() {
    int index = 1;
    return ResultSets.forRows(
        SCHEMA_ROW_TYPE,
        Arrays.asList(
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("INT64")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("INT64")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("BOOL")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("BOOL")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("BYTES(24)")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("BYTES(MAX)")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("STRING(100)")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("STRING(MAX)")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("FLOAT64")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("FLOAT64")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("DATE")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("DATE")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("TIMESTAMP")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("TIMESTAMP")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("NUMERIC")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("NUMERIC")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("JSON")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("JSON")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),

            // ARRAY types.
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<INT64>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<INT64>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<BOOL>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<BOOL>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<BYTES(24)>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<BYTES(MAX)>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<STRING(100)>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<STRING(MAX)>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<FLOAT64>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<FLOAT64>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<DATE>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<DATE>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<TIMESTAMP>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<TIMESTAMP>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<NUMERIC>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<NUMERIC>")
                .set("IS_NULLABLE")
                .to("YES")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<JSON>")
                .set("IS_NULLABLE")
                .to("NO")
                .build(),
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to(String.format("C%d", index++))
                .set("SPANNER_TYPE")
                .to("ARRAY<JSON>")
                .set("IS_NULLABLE")
                .to("YES")
                .build()));
  }

  @Test
  public void testConvertTableToSchemaSet() {
    SchemaSet set =
        SpannerToAvroFactory.SpannerToAvro.convertTableToSchemaSet(
            TableId.of(DatabaseId.of("p", "i", "d"), "FOO"),
            "NAMESPACE",
            createSchemaResultSet(),
            "commitTimestamp");
    int index = 1;
    Schema schema = set.avroSchema();
    assertThat(schema.getField(String.format("C%d", index)).schema()).isEqualTo(SchemaBuilder.builder().longType());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().longType().endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    assertThat(schema.getField(String.format("C%d", index)).schema()).isEqualTo(SchemaBuilder.builder().booleanType());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().booleanType().endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    assertThat(schema.getField(String.format("C%d", index)).schema()).isEqualTo(SchemaBuilder.builder().bytesType());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().bytesType().endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    assertThat(schema.getField(String.format("C%d", index)).schema()).isEqualTo(SchemaBuilder.builder().stringType());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    assertThat(schema.getField(String.format("C%d", index)).schema()).isEqualTo(SchemaBuilder.builder().doubleType());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().doubleType().endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    // DATE, TIMESTAMP, NUMERIC and JSON are all handled as STRING.
    // DATE
    assertThat(schema.getField(String.format("C%d", index)).schema()).isEqualTo(SchemaBuilder.builder().stringType());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    // TIMESTAMP
    assertThat(schema.getField(String.format("C%d", index)).schema()).isEqualTo(SchemaBuilder.builder().stringType());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    // NUMERIC
    assertThat(schema.getField(String.format("C%d", index)).schema()).isEqualTo(SchemaBuilder.builder().stringType());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    // JSON
    assertThat(schema.getField(String.format("C%d", index)).schema()).isEqualTo(SchemaBuilder.builder().stringType());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    // ARRAY types.
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .longType()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .longType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .booleanType()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .booleanType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .bytesType()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .bytesType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .doubleType()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .doubleType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    // DATE, TIMESTAMP, NUMERIC and JSON are all handled as STRING.
    // DATE
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    // TIMESTAMP
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    // NUMERIC
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();

    // JSON
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isFalse();
    assertThat(schema.getField(String.format("C%d", index)).schema())
        .isEqualTo(
            SchemaBuilder.builder()
                .unionOf()
                .nullType()
                .and()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion()
                .endUnion());
    assertThat(schema.getField(String.format("C%d", index++)).schema().isNullable()).isTrue();
  }

  @Test
  public void testConvert() throws IOException {
    DatabaseClient client = mock(DatabaseClient.class);
    ReadContext context = mock(ReadContext.class);
    when(client.singleUse()).thenReturn(context);
    when(context.executeQueryAsync(
            Statement.newBuilder(SpannerUtils.FIND_COMMIT_TIMESTAMP_COLUMN_QUERY)
                .bind("catalog")
                .to("")
                .bind("schema")
                .to("")
                .bind("table")
                .to("FOO")
                .build()))
        .thenReturn(ResultSets.toAsyncResultSet(createTimestampColumnResultSet()));
    when(context.executeQuery(
            Statement.newBuilder(SpannerToAvroFactory.SCHEMA_QUERY)
                .bind("catalog")
                .to("")
                .bind("schema")
                .to("")
                .bind("table")
                .to("FOO")
                .build()))
        .thenReturn(createSchemaResultSet());
    SpannerToAvro converter =
        SpannerToAvroFactory.INSTANCE.create(
            client, TableId.of(DatabaseId.of("p", "i", "i"), "FOO"));

    int index = 1;
    Struct row =
        Struct.newBuilder()
            .set(String.format("C%d", index++))
            .to(1L)
            .set(String.format("C%d", index++))
            .to((Long) null)
            .set(String.format("C%d", index++))
            .to(true)
            .set(String.format("C%d", index++))
            .to((Boolean) null)
            .set(String.format("C%d", index++))
            .to(ByteArray.copyFrom("TEST"))
            .set(String.format("C%d", index++))
            .to((ByteArray) null)
            .set(String.format("C%d", index++))
            .to("TEST")
            .set(String.format("C%d", index++))
            .to((String) null)
            .set(String.format("C%d", index++))
            .to(3.14D)
            .set(String.format("C%d", index++))
            .to((Double) null)
            .set(String.format("C%d", index++))
            .to(Date.fromYearMonthDay(2020, 3, 31))
            .set(String.format("C%d", index++))
            .to((Date) null)
            .set(String.format("C%d", index++))
            .to(Timestamp.parseTimestamp("2020-03-31T21:21:15.120Z"))
            .set(String.format("C%d", index++))
            .to((Timestamp) null)
            .set(String.format("C%d", index++))
            .to(new BigDecimal("3.14"))
            .set(String.format("C%d", index++))
            .to((BigDecimal) null)
            .set(String.format("C%d", index++))
            .to(Value.json("{\"key\": \"value\"}"))
            .set(String.format("C%d", index++))
            .to(Value.json(null))
            // ARRAY types
            .set(String.format("C%d", index++))
            .toInt64Array(Arrays.asList(1L, null, 3L, null, 5L))
            .set(String.format("C%d", index++))
            .toInt64Array((long[]) null)
            .set(String.format("C%d", index++))
            .toBoolArray(Arrays.asList(true, null, false, null))
            .set(String.format("C%d", index++))
            .toBoolArray((boolean[]) null)
            .set(String.format("C%d", index++))
            .toBytesArray(
                Arrays.asList(ByteArray.copyFrom("TEST"), null, ByteArray.copyFrom("FOO"), null))
            .set(String.format("C%d", index++))
            .toBytesArray(null)
            .set(String.format("C%d", index++))
            .toStringArray(Arrays.asList("TEST", null, "FOO", null))
            .set(String.format("C%d", index++))
            .toStringArray(null)
            .set(String.format("C%d", index++))
            .toFloat64Array(Arrays.asList(3.14D, null, 6.626D, null))
            .set(String.format("C%d", index++))
            .toFloat64Array((double[]) null)
            .set(String.format("C%d", index++))
            .toDateArray(
                Arrays.asList(
                    Date.fromYearMonthDay(2020, 3, 31),
                    null,
                    Date.fromYearMonthDay(1970, 1, 1),
                    null))
            .set(String.format("C%d", index++))
            .toDateArray(null)
            .set(String.format("C%d", index++))
            .toTimestampArray(
                Arrays.asList(
                    Timestamp.parseTimestamp("2020-03-31T21:21:15.120Z"),
                    null,
                    Timestamp.ofTimeSecondsAndNanos(0, 0),
                    null))
            .set(String.format("C%d", index++))
            .toTimestampArray(null)
            .set(String.format("C%d", index++))
            .toNumericArray(
                Arrays.asList(new BigDecimal("3.14"), null, new BigDecimal("6.6260"), null))
            .set(String.format("C%d", index++))
            .toNumericArray(null)
            .set(String.format("C%d", index++))
            .toJsonArray(Arrays.asList("{\"key1\": \"val1\"}", null, "{\"key2\": \"val2\"}", null))
            .set(String.format("C%d", index++))
            .toJsonArray(null)
            .build();

    ByteString data = converter.convert(row);
    assertThat(data).isNotNull();

    // Read the data back in.
    SchemaSet set =
        SpannerToAvro.convertTableToSchemaSet(
            TableId.of(DatabaseId.of("p", "i", "i"), "FOO"),
            "NAMESPACE",
            createSchemaResultSet(),
            "LastModifiedAt");
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data.newInput(), null);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(set.avroSchema());
    GenericRecord record = reader.read(null, decoder);
    assertThat(record).isNotNull();
    index = 1;
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(1L);
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(true);
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(ByteBuffer.wrap("TEST".getBytes()));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(new Utf8("TEST"));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(3.14D);
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(new Utf8("2020-03-31"));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(new Utf8("2020-03-31T21:21:15.120000000Z"));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(new Utf8("3.14"));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(new Utf8("{\"key\": \"value\"}"));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    // ARRAY types
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(Arrays.asList(1L, null, 3L, null, 5L));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(Arrays.asList(true, null, false, null));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++)))
        .isEqualTo(
            Arrays.asList(
                ByteBuffer.wrap("TEST".getBytes()), null, ByteBuffer.wrap("FOO".getBytes()), null));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++)))
        .isEqualTo(Arrays.asList(new Utf8("TEST"), null, new Utf8("FOO"), null));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++))).isEqualTo(Arrays.asList(3.14D, null, 6.626D, null));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++)))
        .isEqualTo(Arrays.asList(new Utf8("2020-03-31"), null, new Utf8("1970-01-01"), null));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++)))
        .isEqualTo(
            Arrays.asList(
                new Utf8("2020-03-31T21:21:15.120000000Z"),
                null,
                new Utf8("1970-01-01T00:00:00Z"),
                null));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++)))
        .isEqualTo(Arrays.asList(new Utf8("3.14"), null, new Utf8("6.6260"), null));
    assertThat(record.get(String.format("C%d", index++))).isNull();
    assertThat(record.get(String.format("C%d", index++)))
        .isEqualTo(Arrays.asList(new Utf8("{\"key1\": \"val1\"}"), null, new Utf8("{\"key2\": \"val2\"}"), null));
    assertThat(record.get(String.format("C%d", index++))).isNull();
  }

  private ResultSet createTimestampColumnResultSet() {
    return ResultSets.forRows(
        Type.struct(
            StructField.of("COLUMN_NAME", Type.string()),
            StructField.of("OPTION_NAME", Type.string()),
            StructField.of("OPTION_VALUE", Type.string())),
        Collections.singleton(
            Struct.newBuilder()
                .set("COLUMN_NAME")
                .to("LastModifiedAt")
                .set("OPTION_NAME")
                .to("allow_commit_timestamp")
                .set("OPTION_VALUE")
                .to("TRUE")
                .build()));
  }
}
