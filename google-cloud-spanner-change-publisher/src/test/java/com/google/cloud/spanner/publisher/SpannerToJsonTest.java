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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.publisher.SpannerToJsonFactory.SpannerToJson;
import com.google.cloud.spanner.watcher.TableId;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerToJsonTest {
  @Test
  public void testConvert() throws IOException {
    DatabaseClient client = mock(DatabaseClient.class);
    SpannerToJson converter =
        SpannerToJsonFactory.INSTANCE.create(
            client, TableId.of(DatabaseId.of("p", "i", "i"), "FOO"));

    Struct row =
        Struct.newBuilder()
            .set("C1")
            .to(1L)
            .set("C2")
            .to((Long) null)
            .set("C3")
            .to(true)
            .set("C4")
            .to((Boolean) null)
            .set("C5")
            .to(ByteArray.copyFrom("TEST"))
            .set("C6")
            .to((ByteArray) null)
            .set("C7")
            .to("TEST")
            .set("C8")
            .to((String) null)
            .set("C9")
            .to(3.14D)
            .set("C10")
            .to((Double) null)
            .set("C11")
            .to(Date.fromYearMonthDay(2020, 3, 31))
            .set("C12")
            .to((Date) null)
            .set("C13")
            .to(Timestamp.parseTimestamp("2020-03-31T21:21:15.120Z"))
            .set("C14")
            .to((Timestamp) null)
            // ARRAY types
            .set("C15")
            .toInt64Array(Arrays.asList(1L, null, 3L, null, 5L))
            .set("C16")
            .toInt64Array((long[]) null)
            .set("C17")
            .toBoolArray(Arrays.asList(true, null, false, null))
            .set("C18")
            .toBoolArray((boolean[]) null)
            .set("C19")
            .toBytesArray(
                Arrays.asList(ByteArray.copyFrom("TEST"), null, ByteArray.copyFrom("FOO"), null))
            .set("C20")
            .toBytesArray(null)
            .set("C21")
            .toStringArray(Arrays.asList("TEST", null, "FOO", null))
            .set("C22")
            .toStringArray(null)
            .set("C23")
            .toFloat64Array(Arrays.asList(3.14D, null, 6.626D, null))
            .set("C24")
            .toFloat64Array((double[]) null)
            .set("C25")
            .toDateArray(
                Arrays.asList(
                    Date.fromYearMonthDay(2020, 3, 31),
                    null,
                    Date.fromYearMonthDay(1970, 1, 1),
                    null))
            .set("C26")
            .toDateArray(null)
            .set("C27")
            .toTimestampArray(
                Arrays.asList(
                    Timestamp.parseTimestamp("2020-03-31T21:21:15.120Z"),
                    null,
                    Timestamp.ofTimeSecondsAndNanos(0, 0),
                    null))
            .set("C28")
            .toTimestampArray(null)
            .build();

    ByteString data = converter.convert(row);
    assertThat(data).isNotNull();

    // Read the data back in.
    JsonObject record = JsonParser.parseString(data.toStringUtf8()).getAsJsonObject();
    assertThat(record).isNotNull();
    assertThat(record.get("C1")).isEqualTo(new JsonPrimitive(1L));
    assertThat(record.get("C2")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C3")).isEqualTo(new JsonPrimitive(true));
    assertThat(record.get("C4")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C5"))
        .isEqualTo(new JsonPrimitive(ByteArray.copyFrom("TEST").toBase64()));
    assertThat(record.get("C6")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C7")).isEqualTo(new JsonPrimitive("TEST"));
    assertThat(record.get("C8")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C9")).isEqualTo(new JsonPrimitive(3.14D));
    assertThat(record.get("C10")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C11")).isEqualTo(new JsonPrimitive("2020-03-31"));
    assertThat(record.get("C12")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C13")).isEqualTo(new JsonPrimitive("2020-03-31T21:21:15.120000000Z"));
    assertThat(record.get("C14")).isEqualTo(JsonNull.INSTANCE);
    // ARRAY types
    assertThat(record.get("C15")).isEqualTo(toJsonArray(1L, null, 3L, null, 5L));
    assertThat(record.get("C16")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C17")).isEqualTo(toJsonArray(true, null, false, null));
    assertThat(record.get("C18")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C19"))
        .isEqualTo(
            toJsonArray(
                ByteArray.copyFrom("TEST").toBase64(),
                null,
                ByteArray.copyFrom("FOO").toBase64(),
                null));
    assertThat(record.get("C20")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C21")).isEqualTo(toJsonArray("TEST", null, "FOO", null));
    assertThat(record.get("C22")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C23")).isEqualTo(toJsonArray(3.14D, null, 6.626D, null));
    assertThat(record.get("C24")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C25")).isEqualTo(toJsonArray("2020-03-31", null, "1970-01-01", null));
    assertThat(record.get("C26")).isEqualTo(JsonNull.INSTANCE);
    assertThat(record.get("C27"))
        .isEqualTo(
            toJsonArray("2020-03-31T21:21:15.120000000Z", null, "1970-01-01T00:00:00Z", null));
    assertThat(record.get("C28")).isEqualTo(JsonNull.INSTANCE);
  }

  private static JsonArray toJsonArray(Long... values) {
    JsonArray res = new JsonArray();
    for (Long value : values) {
      res.add(value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
    }
    return res;
  }

  private static JsonArray toJsonArray(Boolean... values) {
    JsonArray res = new JsonArray();
    for (Boolean value : values) {
      res.add(value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
    }
    return res;
  }

  private static JsonArray toJsonArray(String... values) {
    JsonArray res = new JsonArray();
    for (String value : values) {
      res.add(value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
    }
    return res;
  }

  private static JsonArray toJsonArray(Double... values) {
    JsonArray res = new JsonArray();
    for (Double value : values) {
      res.add(value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
    }
    return res;
  }
}
