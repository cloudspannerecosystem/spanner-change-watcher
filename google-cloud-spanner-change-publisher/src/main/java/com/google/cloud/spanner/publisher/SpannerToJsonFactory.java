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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.watcher.TableId;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;

/** Creates {@link SpannerToJson} converters. */
public class SpannerToJsonFactory implements ConverterFactory {
  @Override
  public SpannerToJson create(DatabaseClient client, TableId table) {
    return new SpannerToJson();
  }

  /** Converts a Spanner {@link StructReader} to JSON. */
  public static class SpannerToJson implements Converter {
    @Override
    public ByteString convert(StructReader row) {
      Type type = row.getType();
      int numColumns = type.getStructFields().size();
      JsonObject obj = new JsonObject();
      for (int i = 0; i < numColumns; i++) {
        String columnName = type.getStructFields().get(i).getName();
        if (row.isNull(i)) {
          obj.add(columnName, JsonNull.INSTANCE);
        } else {
          switch (type.getStructFields().get(i).getType().getCode()) {
            case ARRAY:
              JsonArray arrayObject = new JsonArray();
              switch (type.getStructFields().get(i).getType().getArrayElementType().getCode()) {
                case BOOL:
                  row.getBooleanList(i).stream().forEach(b -> arrayObject.add(b));
                  break;
                case BYTES:
                  row.getBytesList(i).stream()
                      .forEach(b -> arrayObject.add(b == null ? null : b.toBase64()));
                  break;
                case DATE:
                  row.getDateList(i).stream()
                      .forEach(d -> arrayObject.add(d == null ? null : d.toString()));
                  break;
                case FLOAT64:
                  row.getDoubleList(i).stream().forEach(d -> arrayObject.add(d));
                  break;
                case INT64:
                  row.getLongList(i).stream().forEach(l -> arrayObject.add(l));
                  break;
                case STRING:
                  row.getStringList(i).stream().forEach(s -> arrayObject.add(s));
                  break;
                case TIMESTAMP:
                  row.getTimestampList(i).stream()
                      .forEach(t -> arrayObject.add(t == null ? null : t.toString()));
                  break;
                case STRUCT:
                case ARRAY:
                default:
                  throw new IllegalArgumentException(
                      "unknown or unsupported array element type: "
                          + type.getStructFields()
                              .get(i)
                              .getType()
                              .getArrayElementType()
                              .getCode());
              }
              obj.add(columnName, arrayObject);
              break;
            case BOOL:
              obj.addProperty(columnName, row.getBoolean(i));
              break;
            case BYTES:
              obj.addProperty(columnName, row.getBytes(i).toBase64());
              break;
            case DATE:
              obj.addProperty(columnName, row.getDate(i).toString());
              break;
            case FLOAT64:
              obj.addProperty(columnName, row.getDouble(i));
              break;
            case INT64:
              obj.addProperty(columnName, row.getLong(i));
              break;
            case STRING:
              obj.addProperty(columnName, row.getString(i));
              break;
            case TIMESTAMP:
              obj.addProperty(columnName, row.getTimestamp(i).toString());
              break;
            case STRUCT:
            default:
              throw new IllegalArgumentException(
                  "unknown or unsupported type: "
                      + type.getStructFields().get(i).getType().getCode());
          }
        }
      }
      return ByteString.copyFromUtf8(obj.toString());
    }
  }
}
