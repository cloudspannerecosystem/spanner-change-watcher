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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Publishes change events from a Spanner table to a PubSub topic.
 *
 * <p>The changes to the table are emitted from a {@link SpannerTableChangeWatcher} and then sent to
 * Pub/Sub by this event publisher.
 */
public class SpannerTableChangeEventPublisher {
  private static final Logger logger =
      Logger.getLogger(SpannerTableChangeEventPublisher.class.getName());

  /** Interface for getting a callback when a message is published to PubSub. */
  public static interface PublishListener {
    /** Called when a change is successfully published to PubSub. */
    void onPublished(TableId table, Timestamp commitTimestamp, String messageId);
  }

  private static final class NoOpListener implements PublishListener {
    @Override
    public void onPublished(TableId table, Timestamp commitTimestamp, String messageId) {}
  }

  public static class Builder {
    private final SpannerTableChangeWatcher capturer;
    private final DatabaseClient client;
    private Publisher publisher;
    private PublishListener listener = new NoOpListener();
    private String topicName;
    private Credentials credentials;
    private String endpoint = PublisherStubSettings.getDefaultEndpoint();
    private boolean usePlainText;

    private Builder(SpannerTableChangeWatcher capturer, DatabaseClient client) {
      this.capturer = capturer;
      this.client = client;
    }

    public Builder setListener(PublishListener publishListener) {
      this.listener = Preconditions.checkNotNull(publishListener);
      return this;
    }

    /** Sets the name of the topic where the events should be published. */
    public Builder setTopicName(String topicName) {
      Preconditions.checkState(
          publisher == null,
          "Set either the Publisher or the TopicName and Credentials, but not both.");
      this.topicName = Preconditions.checkNotNull(topicName);
      return this;
    }

    /**
     * Sets the credentials to use to publish to Pub/Sub. If no credentials are set, the credentials
     * returned by {@link GoogleCredentials#getApplicationDefault()} will be used.
     */
    public Builder setCredentials(Credentials credentials) {
      Preconditions.checkState(
          publisher == null,
          "Set either the Publisher or the TopicName and Credentials, but not both.");
      this.credentials = Preconditions.checkNotNull(credentials);
      return this;
    }

    @VisibleForTesting
    Builder setEndpoint(String endpoint) {
      this.endpoint = Preconditions.checkNotNull(endpoint);
      return this;
    }

    @VisibleForTesting
    Builder usePlainText() {
      this.usePlainText = true;
      return this;
    }

    /**
     * Sets the {@link Publisher} to use for this event publisher. Use this method if you want to
     * use custom batching or retry settings for Pub/Sub.
     */
    public Builder setPublisher(Publisher publisher) {
      Preconditions.checkState(
          topicName == null && credentials == null,
          "Set either the Publisher or the TopicName and Credentials, but not both.");
      this.publisher = Preconditions.checkNotNull(publisher);
      return this;
    }

    public SpannerTableChangeEventPublisher build() throws IOException {
      Preconditions.checkState(publisher != null || topicName != null);
      return new SpannerTableChangeEventPublisher(this);
    }
  }

  /**
   * Creates a new {@link Builder} for a {@link SpannerTableChangeEventPublisher} with the given
   * {@link SpannerTableChangeWatcher} as its source.
   */
  public static Builder newBuilder(SpannerTableChangeWatcher capturer, DatabaseClient client) {
    return new Builder(capturer, client);
  }

  private boolean started = false;
  private boolean stopped = false;
  private ApiFuture<Void> capturerStopFuture;
  private final Publisher publisher;
  private final PublishListener listener;
  private final SpannerTableChangeWatcher capturer;
  private final SpannerToAvro converter;

  private SpannerTableChangeEventPublisher(Builder builder) throws IOException {
    if (builder.publisher != null) {
      this.publisher = builder.publisher;
    } else {
      Credentials credentials =
          builder.credentials == null
              ? GoogleCredentials.getApplicationDefault()
              : builder.credentials;
      if (credentials == null) {
        throw new IllegalArgumentException(
            "There is no credentials set on the builder, and the environment has no default credentials set.");
      }
      Publisher.Builder publisherBuilder =
          Publisher.newBuilder(builder.topicName)
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .setEndpoint(builder.endpoint);
      if (builder.usePlainText) {
        ManagedChannel channel =
            ManagedChannelBuilder.forTarget(builder.endpoint).usePlaintext().build();
        TransportChannelProvider channelProvider =
            FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
        publisherBuilder.setChannelProvider(channelProvider);
        publisherBuilder.setCredentialsProvider(credentialsProvider);
      }
      this.publisher = publisherBuilder.build();
    }
    this.listener = builder.listener;
    this.capturer = builder.capturer;
    this.converter = new SpannerToAvro(builder.client, capturer.getTable());
  }

  /** Starts this event publisher and starts publishing changes to Pub/Sub. */
  public void start() {
    Preconditions.checkState(!started, "This event publisher has already been started.");
    started = true;
    capturer.start(
        new RowChangeCallback() {
          @Override
          public void rowChange(final TableId table, Row row, final Timestamp commitTimestamp) {
            // Only use resources to convert the row to a string if we are actually going to log it.
            final String rowString =
                logger.isLoggable(Level.FINE) ? row.asStruct().toString() : null;
            logger.log(Level.FINE, "Publishing change to row {0}", rowString);
            ApiFuture<String> result =
                publisher.publish(
                    PubsubMessage.newBuilder()
                        .setData(converter.makeRecord(row))
                        .putAttributes("Database", table.getDatabaseId().getName())
                        .putAttributes("Catalog", table.getCatalog())
                        .putAttributes("Schema", table.getSchema())
                        .putAttributes("Table", table.getTable())
                        .putAttributes("Timestamp", commitTimestamp.toString())
                        .build());
            ApiFutures.addCallback(
                result,
                new ApiFutureCallback<String>() {
                  @Override
                  public void onSuccess(String messageId) {
                    logger.log(Level.FINE, "Successfully published change to row {0}", rowString);
                    listener.onPublished(table, commitTimestamp, messageId);
                  }

                  @Override
                  public void onFailure(Throwable t) {
                    logger.log(Level.WARNING, "Failed to publish change", t);
                    logger.log(
                        Level.FINE,
                        String.format("Failed to publish change to row {0}", rowString));
                  }
                },
                MoreExecutors.directExecutor());
          }
        });
  }

  /**
   * Stops this event publisher. No more changes will be published to Pub/Sub when this method has
   * returned.
   */
  public void stop() {
    Preconditions.checkState(started, "This event publisher has not been started");
    Preconditions.checkState(!stopped, "This event publisher has already been stopped");
    stopped = true;
    capturerStopFuture = capturer.stopAsync();
    publisher.shutdown();
  }

  public boolean awaitTermination(long duration, TimeUnit unit) throws InterruptedException {
    Preconditions.checkState(stopped, "This event publisher has not been stopped");
    Stopwatch watch = Stopwatch.createStarted();
    try {
      capturerStopFuture.get(duration, unit);
    } catch (ExecutionException | TimeoutException e) {
      return false;
    }
    long remaining = duration - watch.elapsed(unit) + 1L;
    return publisher.awaitTermination(remaining, unit);
  }
}
