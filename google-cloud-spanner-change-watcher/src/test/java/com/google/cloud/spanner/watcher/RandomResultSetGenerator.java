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

import com.google.api.client.util.Base64;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.common.base.Preconditions;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.util.Random;

public class RandomResultSetGenerator {
  public enum ValueMode {
    FORCE_NULL_VALUE,
    ALLOW_NULL_VALUE,
    NO_NULL_VALUE;
  }

  private static final Type TYPES[] =
      new Type[] {
        Type.newBuilder().setCode(TypeCode.BOOL).build(),
        Type.newBuilder().setCode(TypeCode.INT64).build(),
        Type.newBuilder().setCode(TypeCode.FLOAT64).build(),
        Type.newBuilder().setCode(TypeCode.STRING).build(),
        Type.newBuilder().setCode(TypeCode.BYTES).build(),
        Type.newBuilder().setCode(TypeCode.DATE).build(),
        Type.newBuilder().setCode(TypeCode.TIMESTAMP).build(),
        Type.newBuilder().setCode(TypeCode.NUMERIC).build(),
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.BOOL))
            .build(),
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.INT64))
            .build(),
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.FLOAT64))
            .build(),
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.STRING))
            .build(),
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.BYTES))
            .build(),
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.DATE))
            .build(),
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.TIMESTAMP))
            .build(),
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.NUMERIC))
            .build(),
      };

  private static final ResultSetMetadata generateMetadata() {
    StructType.Builder rowTypeBuilder = StructType.newBuilder();
    for (int col = 0; col < TYPES.length; col++) {
      rowTypeBuilder.addFields(Field.newBuilder().setName("COL" + col).setType(TYPES[col])).build();
    }
    rowTypeBuilder
        .addFields(
            Field.newBuilder()
                .setName("LastModified")
                .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build()))
        .build();
    ResultSetMetadata.Builder builder = ResultSetMetadata.newBuilder();
    builder.setRowType(rowTypeBuilder.build());
    return builder.build();
  }

  public static final ResultSet generateRandomResultSetInformationSchemaResultSet() {
    ResultSet.Builder builder = ResultSet.newBuilder();
    for (int col = 0; col < TYPES.length; col++) {
      String typeName;
      if (TYPES[col].getCode() == TypeCode.ARRAY) {
        typeName = "ARRAY<" + getSpannerTypeName(TYPES[col].getArrayElementType().getCode()) + ">";
      } else {
        typeName = getSpannerTypeName(TYPES[col].getCode());
      }
      builder.addRows(
          ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue("COL" + col).build())
              .addValues(Value.newBuilder().setStringValue(typeName).build())
              .addValues(Value.newBuilder().setStringValue("YES").build())
              .build());
    }
    builder.addRows(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("LastModified").build())
            .addValues(Value.newBuilder().setStringValue("TIMESTAMP").build())
            .addValues(Value.newBuilder().setStringValue("YES").build()));
    builder.setMetadata(
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
                    .addFields(
                        Field.newBuilder()
                            .setName("IS_NULLABLE")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build()));
    return builder.build();
  }

  private static String getSpannerTypeName(TypeCode code) {
    String typeName = code.name();
    if (code == TypeCode.STRING || code == TypeCode.BYTES) {
      typeName = typeName + "(MAX)";
    }
    return typeName;
  }

  public static final ResultSetMetadata METADATA = generateMetadata();

  private final int rowCount;
  private final Random random = new Random();

  public static Timestamp getMaxCommitTimestamp(ResultSet resultSet) {
    Timestamp res = Timestamp.MIN_VALUE;
    for (ListValue row : resultSet.getRowsList()) {
      Value commitTimestampString = row.getValuesList().get(TYPES.length);
      Timestamp ts = Timestamp.parseTimestamp(commitTimestampString.getStringValue());
      if (ts.compareTo(res) > 0) {
        res = ts;
      }
    }
    return res;
  }

  public RandomResultSetGenerator(int rowCount) {
    this.rowCount = rowCount;
  }

  public ResultSet generate() {
    return internalGenerate(null);
  }

  public ResultSet generateWithFixedCommitTimestamp(Timestamp timestamp) {
    return internalGenerate(Preconditions.checkNotNull(timestamp));
  }

  private ResultSet internalGenerate(Timestamp commitTimestamp) {
    ResultSet.Builder builder = ResultSet.newBuilder();
    for (int row = 0; row < rowCount; row++) {
      ListValue.Builder rowBuilder = ListValue.newBuilder();
      for (int col = 0; col < TYPES.length; col++) {
        Value.Builder valueBuilder = Value.newBuilder();
        setRandomValue(valueBuilder, TYPES[col], ValueMode.ALLOW_NULL_VALUE);
        rowBuilder.addValues(valueBuilder.build());
      }
      Value.Builder valueBuilder = Value.newBuilder();
      if (commitTimestamp == null) {
        setRandomValue(
            valueBuilder,
            Type.newBuilder().setCode(TypeCode.TIMESTAMP).build(),
            ValueMode.NO_NULL_VALUE);
      } else {
        valueBuilder.setStringValue(commitTimestamp.toString());
      }
      rowBuilder.addValues(valueBuilder.build());
      builder.addRows(rowBuilder.build());
    }
    builder.setMetadata(METADATA);
    return builder.build();
  }

  private void setRandomValue(Value.Builder builder, Type type, ValueMode mode) {
    if (mode != ValueMode.NO_NULL_VALUE && (mode == ValueMode.FORCE_NULL_VALUE || randomNull())) {
      builder.setNullValue(NullValue.NULL_VALUE);
    } else {
      switch (type.getCode()) {
        case ARRAY:
          int length = random.nextInt(20) + 1;
          ListValue.Builder arrayBuilder = ListValue.newBuilder();
          for (int i = 0; i < length; i++) {
            Value.Builder valueBuilder = Value.newBuilder();
            setRandomValue(valueBuilder, type.getArrayElementType(), mode);
            arrayBuilder.addValues(valueBuilder.build());
          }
          builder.setListValue(arrayBuilder.build());
          break;
        case BOOL:
          builder.setBoolValue(random.nextBoolean());
          break;
        case STRING:
        case BYTES:
          byte[] bytes = new byte[random.nextInt(200)];
          random.nextBytes(bytes);
          builder.setStringValue(Base64.encodeBase64String(bytes));
          break;
        case DATE:
          Date date =
              Date.fromYearMonthDay(
                  random.nextInt(2019) + 1, random.nextInt(11) + 1, random.nextInt(28) + 1);
          builder.setStringValue(date.toString());
          break;
        case FLOAT64:
          builder.setNumberValue(random.nextDouble());
          break;
        case INT64:
          builder.setStringValue(String.valueOf(random.nextLong()));
          break;
        case TIMESTAMP:
          com.google.protobuf.Timestamp ts =
              Timestamps.add(
                  Timestamps.EPOCH,
                  com.google.protobuf.Duration.newBuilder()
                      .setSeconds(random.nextInt(100_000_000))
                      .setNanos(random.nextInt(1000_000_000))
                      .build());
          builder.setStringValue(Timestamp.fromProto(ts).toString());
          break;
        case NUMERIC:
          builder.setStringValue(random.nextInt() + "." + random.nextInt(1000000000));
          break;
        case STRUCT:
        case TYPE_CODE_UNSPECIFIED:
        case UNRECOGNIZED:
        default:
          throw new IllegalArgumentException("Unknown or unsupported type: " + type.getCode());
      }
    }
  }

  private boolean randomNull() {
    return random.nextInt(10) == 0;
  }
}
