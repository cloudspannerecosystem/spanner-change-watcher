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

package com.google.cloud.spanner.watcher;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerUtilsTest {

  @Test
  public void TestBuildKey() {
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(
                Arrays.asList(
                    StructField.of("bool", Type.bool()),
                    StructField.of("bytes", Type.bytes()),
                    StructField.of("date", Type.date()),
                    StructField.of("float64", Type.float64()),
                    StructField.of("int64", Type.int64()),
                    StructField.of("numeric", Type.numeric()),
                    StructField.of("string", Type.string()),
                    StructField.of("timestamp", Type.timestamp()))),
            Arrays.asList(
                Struct.newBuilder()
                    .set("bool")
                    .to(true)
                    .set("bytes")
                    .to(ByteArray.copyFrom("test"))
                    .set("date")
                    .to(Date.fromYearMonthDay(2000, 1, 1))
                    .set("float64")
                    .to(3.14)
                    .set("int64")
                    .to(1L)
                    .set("numeric")
                    .to(BigDecimal.ONE)
                    .set("string")
                    .to("string")
                    .set("timestamp")
                    .to(Timestamp.ofTimeMicroseconds(1000L))
                    .build()));
    resultSet.next();

    Key key =
        SpannerUtils.buildKey(
            Arrays.asList(
                "bool", "bytes", "date", "float64", "int64", "numeric", "string", "timestamp"),
            resultSet);
    Iterator<Object> parts = key.getParts().iterator();
    assertEquals(true, parts.next());
    assertEquals(ByteArray.copyFrom("test"), parts.next());
    assertEquals(Date.fromYearMonthDay(2000, 1, 1), parts.next());
    assertEquals(3.14D, parts.next());
    assertEquals(1L, parts.next());
    assertEquals(BigDecimal.ONE, parts.next());
    assertEquals("string", parts.next());
    assertEquals(Timestamp.ofTimeMicroseconds(1000L), parts.next());
  }
}
