package com.google.cloud.spanner.publisher;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher;
import com.google.cloud.spanner.watcher.TableId;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

/**
 * ConverterFactory creates instances of converters that can convert a changed row in Spanner from
 * {@link StructReader} representation to a {@link ByteString} representation. The {@link
 * ByteString} will be used as the data of the {@link PubsubMessage} that is published for the
 * change.
 */
public interface ConverterFactory {

  /**
   * Converter is responsible for converting a changed row that is received in a {@link
   * SpannerTableChangeWatcher.RowChangeCallback} from a Spanner {@link ResultSet} to a {@link
   * ByteString} that can be published to Pubsub. The default converter that is used by a Publisher
   * will convert a changed row to an Avro record.
   */
  interface Converter {
    /**
     * Converts a {@link Spanner} {@link StructReader} to a {@link ByteString} that can be published
     * to Pubsub.
     */
    ByteString convert(StructReader row);
  }

  /**
   * Creates a new {@link Converter}. This method will be called automatically by a {@link
   * SpannerTableChangeEventPublisher} or {@link SpannerDatabaseChangeEventPublisher} when it is
   * started for each table that is being monitored.
   */
  Converter create(DatabaseClient client, TableId table);
}
