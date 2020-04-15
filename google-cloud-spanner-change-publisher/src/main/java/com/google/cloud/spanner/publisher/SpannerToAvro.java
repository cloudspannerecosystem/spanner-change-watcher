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

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.watcher.SpannerUtils;
import com.google.cloud.spanner.watcher.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

/** Converts schema and data from Spanner to Avro. */
class SpannerToAvro {
  private static final Logger logger = Logger.getLogger(SpannerToAvro.class.getName());
  private static final ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
  static final String SCHEMA_QUERY =
      "SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE\n"
          + "FROM INFORMATION_SCHEMA.COLUMNS\n"
          + "WHERE TABLE_CATALOG = @catalog\n"
          + "AND TABLE_SCHEMA = @schema\n"
          + "AND TABLE_NAME = @table\n"
          + "ORDER BY ORDINAL_POSITION";

  private final DatabaseClient client;
  private final TableId table;
  private final Statement statement;
  private final SchemaSet schemaSet;

  SpannerToAvro(DatabaseClient client, TableId table) {
    this.client = client;
    this.table = table;
    this.statement =
        Statement.newBuilder(SCHEMA_QUERY)
            .bind("catalog")
            .to(table.getCatalog())
            .bind("schema")
            .to(table.getSchema())
            .bind("table")
            .to(table.getTable())
            .build();
    this.schemaSet = createSchemaSet();
  }

  private SchemaSet createSchemaSet() {
    String tsColName = SpannerUtils.getTimestampColumn(client, table);
    try (ResultSet resultSet = client.singleUse().executeQuery(statement)) {
      return convertTableToSchemaSet(table, "avroNamespace", resultSet, tsColName);
    }
  }

  @VisibleForTesting
  static SchemaSet convertTableToSchemaSet(
      TableId table, String avroNamespace, ResultSet resultSet, String tsColName) {
    final LinkedHashMap<String, String> spannerSchema = Maps.newLinkedHashMap();
    final SchemaBuilder.FieldAssembler<Schema> avroSchemaBuilder =
        SchemaBuilder.record(table.getTable()).namespace(avroNamespace).fields();
    while (resultSet.next()) {
      final Struct currentRow = resultSet.getCurrentRowAsStruct();
      final String name = currentRow.getString(0);
      final String type = currentRow.getString(1);
      final boolean nullable = currentRow.getString(2).equals("NO") ? false : true;
      final String baseType;
      String elementType;
      if (type.indexOf('<') > -1 && type.indexOf('>') > -1) {
        baseType = type.substring(0, type.indexOf('<'));
        elementType = type.substring(type.indexOf('<') + 1, type.length() - 1);
        if (elementType.indexOf('(') > -1 && elementType.indexOf(')') > -1) {
          elementType = elementType.substring(0, elementType.indexOf('('));
        }
      } else if (type.indexOf('(') > -1 && type.indexOf(')') > -1) {
        baseType = type.substring(0, type.indexOf('('));
        elementType = "";
      } else {
        baseType = type;
        elementType = "";
      }

      spannerSchema.put(name, baseType);
      logger.log(Level.FINE, "Binding Avro Schema");
      switch (baseType) {
        case "ARRAY":
          logger.log(Level.FINE, "Made ARRAY");
          switch (elementType) {
            case "BOOL":
              logger.log(Level.FINE, "Made ARRAY<BOOL>");
              if (nullable) {
                avroSchemaBuilder
                    .name(name)
                    .type()
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
                    .endUnion()
                    .noDefault();
              } else {
                avroSchemaBuilder
                    .name(name)
                    .type()
                    .array()
                    .items()
                    .unionOf()
                    .nullType()
                    .and()
                    .booleanType()
                    .endUnion()
                    .noDefault();
              }
              break;
            case "BYTES":
              logger.log(Level.FINE, "Made ARRAY<BYTES>");
              if (nullable) {
                avroSchemaBuilder
                    .name(name)
                    .type()
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
                    .endUnion()
                    .noDefault();
              } else {
                avroSchemaBuilder
                    .name(name)
                    .type()
                    .array()
                    .items()
                    .unionOf()
                    .nullType()
                    .and()
                    .bytesType()
                    .endUnion()
                    .noDefault();
              }
              break;
            case "DATE":
              // Date handled as String type
              logger.log(Level.FINE, "Made ARRAY<DATE>");
              if (nullable) {
                avroSchemaBuilder
                    .name(name)
                    .type()
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
                    .endUnion()
                    .noDefault();
              } else {
                avroSchemaBuilder
                    .name(name)
                    .type()
                    .array()
                    .items()
                    .unionOf()
                    .nullType()
                    .and()
                    .stringType()
                    .endUnion()
                    .noDefault();
              }
              break;
            case "FLOAT64":
              logger.log(Level.FINE, "Made ARRAY<FLOAT64>");
              if (nullable) {
                avroSchemaBuilder
                    .name(name)
                    .type()
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
                    .endUnion()
                    .noDefault();
              } else {
                avroSchemaBuilder
                    .name(name)
                    .type()
                    .array()
                    .items()
                    .unionOf()
                    .nullType()
                    .and()
                    .doubleType()
                    .endUnion()
                    .noDefault();
              }
              break;
            case "INT64":
              logger.log(Level.FINE, "Made ARRAY<INT64>");
              if (nullable) {
                avroSchemaBuilder
                    .name(name)
                    .type()
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
                    .endUnion()
                    .noDefault();
              } else {
                avroSchemaBuilder
                    .name(name)
                    .type()
                    .array()
                    .items()
                    .unionOf()
                    .nullType()
                    .and()
                    .longType()
                    .endUnion()
                    .noDefault();
              }
              break;
            case "STRING":
              logger.log(Level.FINE, "Made ARRAY<STRING>");
              if (nullable) {
                avroSchemaBuilder
                    .name(name)
                    .type()
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
                    .endUnion()
                    .noDefault();
              } else {
                avroSchemaBuilder
                    .name(name)
                    .type()
                    .array()
                    .items()
                    .unionOf()
                    .nullType()
                    .and()
                    .stringType()
                    .endUnion()
                    .noDefault();
              }
              break;
            case "TIMESTAMP":
              // Timestamp handled as String type
              logger.log(Level.FINE, "Made ARRAY<TIMESTAMP>");
              if (nullable) {
                avroSchemaBuilder
                    .name(name)
                    .type()
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
                    .endUnion()
                    .noDefault();
              } else {
                avroSchemaBuilder
                    .name(name)
                    .type()
                    .array()
                    .items()
                    .unionOf()
                    .nullType()
                    .and()
                    .stringType()
                    .endUnion()
                    .noDefault();
              }
              break;
          }
          break;
        case "BOOL":
          logger.log(Level.FINE, "Made BOOL");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().booleanType();
          } else {
            avroSchemaBuilder.name(name).type().booleanType().noDefault();
          }
          break;
        case "BYTES":
          logger.log(Level.FINE, "Made BYTES");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().bytesType();
          } else {
            avroSchemaBuilder.name(name).type().bytesType().noDefault();
          }
          break;
        case "DATE":
          // Date handled as String type
          logger.log(Level.FINE, "Made DATE");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().stringType();
          } else {
            avroSchemaBuilder.name(name).type().stringType().noDefault();
          }
          break;
        case "FLOAT64":
          logger.log(Level.FINE, "Made FLOAT64");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().doubleType();
          } else {
            avroSchemaBuilder.name(name).type().doubleType().noDefault();
          }
          break;
        case "INT64":
          logger.log(Level.FINE, "Made INT64");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().longType();
          } else {
            avroSchemaBuilder.name(name).type().longType().noDefault();
          }
          break;
        case "STRING":
          logger.log(Level.FINE, "Made STRING");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().stringType();
          } else {
            avroSchemaBuilder.name(name).type().stringType().noDefault();
          }
          break;
        case "TIMESTAMP":
          logger.log(Level.FINE, "Made TIMESTAMP");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().stringType();
          } else {
            avroSchemaBuilder.name(name).type().stringType().noDefault();
          }
          break;
        default:
          throw new IllegalArgumentException("Unknown SPANNER_TYPE: " + type);
      }
    }

    logger.log(Level.FINE, "Ending Avro Record");
    final Schema avroSchema = avroSchemaBuilder.endRecord();

    logger.log(Level.FINE, "Made Avro Schema");

    if (logger.isLoggable(Level.FINE)) {
      final Set<String> keySet = spannerSchema.keySet();
      for (String k : keySet) {
        logger.info("-------------------------- ColName: " + k + " Type: " + spannerSchema.get(k));
      }

      logger.info("--------------------------- " + avroSchema.toString());
    }

    return SchemaSet.create(avroSchema, ImmutableMap.copyOf(spannerSchema), tsColName);
  }

  ByteString makeRecord(StructReader row) {
    //    final ByteBuf bb = Unpooled.directBuffer();
    final ByteBuf bb = alloc.directBuffer(1024); // fix this
    final Set<String> keySet = schemaSet.spannerSchema().keySet();
    final GenericRecord record = new GenericData.Record(schemaSet.avroSchema());

    logger.log(Level.FINE, "KeySet: " + keySet);
    logger.log(Level.FINE, "Record: " + record);

    for (String x : keySet) {
      logger.log(Level.FINE, "Column Name: " + x);
      logger.log(Level.FINE, "Data Type: " + schemaSet.spannerSchema().get(x));

      if (row.isNull(x)) {
        record.put(x, null);
      } else {
        switch (schemaSet.spannerSchema().get(x)) {
          case "ARRAY":
            logger.log(Level.FINE, "Put ARRAY");

            final Type columnType = row.getColumnType(x);
            final String arrayTypeString = columnType.getArrayElementType().getCode().toString();

            logger.log(Level.FINE, "Type: " + columnType);
            logger.log(Level.FINE, "ArrayString: " + arrayTypeString);

            switch (arrayTypeString) {
              case "BOOL":
                logger.log(Level.FINE, "Put ARRAY<BOOL>");
                record.put(x, row.getBooleanList(x));
                break;
              case "BYTES":
                logger.log(Level.FINE, "Put ARRAY<BYTES>");
                record.put(x, toByteBufferCollection(row.getBytesList(x)));
                break;
              case "DATE":
                logger.log(Level.FINE, "Put ARRAY<DATE>");
                record.put(x, toStringCollection(row.getDateList(x)));
                break;
              case "FLOAT64":
                logger.log(Level.FINE, "Put ARRAY<FLOAT64>");
                record.put(x, row.getDoubleList(x));
                break;
              case "INT64":
                logger.log(Level.FINE, "Put ARRAY<INT64>");
                record.put(x, row.getLongList(x));
                break;
              case "STRING":
                logger.log(Level.FINE, "Put ARRAY<STRING>");
                record.put(x, row.getStringList(x));
                break;
              case "TIMESTAMP":
                logger.log(Level.FINE, "Put ARRAY<TIMESTAMP>");
                record.put(x, toStringCollection(row.getTimestampList(x)));
                break;
              default:
                logger.warning(
                    "Unknown Data type when generating Array Schema: " + arrayTypeString);
                break;
            }
            break;
          case "BOOL":
            logger.log(Level.FINE, "Put BOOL");
            record.put(x, row.getBoolean(x));
            break;
          case "BYTES":
            logger.log(Level.FINE, "Put BYTES");
            record.put(x, row.getBytes(x).asReadOnlyByteBuffer());
            break;
          case "DATE":
            logger.log(Level.FINE, "Put DATE");
            record.put(x, row.getDate(x).toString());
            break;
          case "FLOAT64":
            logger.log(Level.FINE, "Put FLOAT64");
            record.put(x, row.getDouble(x));
            break;
          case "INT64":
            logger.log(Level.FINE, "Put INT64");
            record.put(x, row.getLong(x));
            break;
          case "STRING":
            logger.log(Level.FINE, "Put STRING");
            record.put(x, row.getString(x));
            break;
          case "TIMESTAMP":
            logger.log(Level.FINE, "Put TIMESTAMP");
            record.put(x, row.getTimestamp(x).toString());
            break;
          default:
            logger.warning(
                "Unknown Data type when generating Avro Record: "
                    + schemaSet.spannerSchema().get(x));
            break;
        }
      }
    }

    logger.log(Level.FINE, "Made Record");
    logger.log(Level.FINE, record.toString());

    try (final ByteBufOutputStream outputStream = new ByteBufOutputStream(bb)) {
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      final DatumWriter<Object> writer = new GenericDatumWriter<>(schemaSet.avroSchema());

      logger.log(Level.FINE, "Serializing Record");
      writer.write(record, encoder);
      encoder.flush();
      outputStream.flush();
      logger.log(Level.FINE, "Adding serialized record to list");
      logger.log(
          Level.FINE, "--------------------------------- readableBytes " + bb.readableBytes());
      logger.log(Level.FINE, "--------------------------------- readerIndex " + bb.readerIndex());
      logger.log(Level.FINE, "--------------------------------- writerIndex " + bb.writerIndex());

      final ByteString message = ByteString.copyFrom(bb.nioBuffer());

      return message;

    } catch (IOException e) {
      logger.log(
          Level.WARNING,
          "IOException while Serializing Spanner Stuct to Avro Record: " + record.toString(),
          e);
    } finally {
      bb.release();
    }

    return null;
  }

  private static <T> Collection<String> toStringCollection(Collection<T> objects) {
    return Collections2.transform(
        objects,
        new Function<T, String>() {
          @Override
          public String apply(T input) {
            return input == null ? null : input.toString();
          }
        });
  }

  private static Collection<ByteBuffer> toByteBufferCollection(Collection<ByteArray> byteArrays) {
    return Collections2.transform(
        byteArrays,
        new Function<ByteArray, ByteBuffer>() {
          @Override
          public ByteBuffer apply(ByteArray input) {
            return input == null ? null : input.asReadOnlyByteBuffer();
          }
        });
  }

  static class SchemaSet {
    private final Schema avroSchema;
    private final ImmutableMap<String, String> spannerSchema;
    private final String tsColName;

    static SchemaSet create(
        Schema avroSchema, ImmutableMap<String, String> spannerSchema, String tsColName) {
      return new SchemaSet(avroSchema, spannerSchema, tsColName);
    }

    private SchemaSet(
        Schema avroSchema, ImmutableMap<String, String> spannerSchema, String tsColName) {
      this.avroSchema = avroSchema;
      this.spannerSchema = spannerSchema;
      this.tsColName = tsColName;
    }

    Schema avroSchema() {
      return avroSchema;
    }

    ImmutableMap<String, String> spannerSchema() {
      return spannerSchema;
    }

    String tsColName() {
      return tsColName;
    }
  }
}
