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

import com.google.api.core.AbstractApiService;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.ApiService;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Publishes change events from a single Spanner table to a single Google Cloud Pubsub topic.
 *
 * <p>The changes to the table are emitted from a {@link SpannerTableChangeWatcher} and then sent to
 * Google Cloud Pubsub by this event publisher.
 */
public class SpannerTableChangeEventPublisher extends AbstractApiService implements ApiService {
  private static final Logger logger =
      Logger.getLogger(SpannerTableChangeEventPublisher.class.getName());

  /**
   * Interface for getting a callback when a message is published to PubSub. This can be used for
   * monitoring and logging purposes, but is not required for publishing changes to Pubsub.
   */
  public static interface PublishListener {
    /** Called when a change is successfully published to Pubsub. */
    default void onPublished(TableId table, Timestamp commitTimestamp, String messageId) {}

    /** Called when a change could not be published to Pubsub. */
    default void onFailure(TableId table, Timestamp commitTimestamp, Throwable t) {}
  }

  /** Builder for a {@link SpannerTableChangeEventPublisher}. */
  public static class Builder {
    private final SpannerTableChangeWatcher watcher;
    private final DatabaseClient client;
    private Publisher publisher;
    private String topicName;
    private ExecutorService startStopExecutor;
    private List<PublishListener> listeners = new ArrayList<>();
    private Credentials credentials;
    private String endpoint = PublisherStubSettings.getDefaultEndpoint();
    private boolean usePlainText;

    private Builder(SpannerTableChangeWatcher watcher, DatabaseClient client) {
      this.watcher = watcher;
      this.client = client;
    }

    /**
     * Adds a {@link PublishListener} for the {@link SpannerTableChangeEventPublisher}. This
     * listener will receive a callback for each message that is published to Pubsub. This can be
     * used for monitoring and logging purposes, but is not required for publishing changes to
     * Pubsub.
     */
    public Builder addListener(PublishListener publishListener) {
      this.listeners.add(Preconditions.checkNotNull(publishListener));
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
     * The {@link SpannerTableChangeEventPublisher} needs to perform a number of administrative
     * tasks during startup and shutdown, and will use this executor for those tasks. The default
     * will use a cached thread pool executor.
     */
    public Builder setStartStopExecutor(ExecutorService executor) {
      this.startStopExecutor = executor;
      return this;
    }

    /**
     * Sets the credentials to use to publish to Pubsub. If no credentials are set, the credentials
     * returned by {@link GoogleCredentials#getApplicationDefault()} will be used.
     */
    public Builder setCredentials(Credentials credentials) {
      Preconditions.checkState(
          publisher == null,
          "Set either the Publisher or the TopicName and Credentials, but not both.");
      this.credentials = Preconditions.checkNotNull(credentials);
      return this;
    }

    /**
     * Set a custom endpoint for Pubsub. Can be used for testing against a local mock server or
     * emulator.
     */
    @VisibleForTesting
    Builder setEndpoint(String endpoint) {
      this.endpoint = Preconditions.checkNotNull(endpoint);
      return this;
    }

    /**
     * Use a plain text connection in combination with a custom endpoint. Can be used for testing
     * against a locak mock server or emulator.
     */
    @VisibleForTesting
    Builder usePlainText() {
      this.usePlainText = true;
      return this;
    }

    /**
     * Sets the {@link Publisher} to use for this event publisher. Use this method if you want to
     * use custom batching or retry settings for Pubsub. If not set, the {@link
     * SpannerTableChangeEventPublisher} will create a {@link Publisher} using default settings.
     */
    public Builder setPublisher(Publisher publisher) {
      Preconditions.checkState(
          topicName == null && credentials == null,
          "Set either the Publisher or the TopicName and Credentials, but not both.");
      this.publisher = Preconditions.checkNotNull(publisher);
      return this;
    }

    /** Creates the {@link SpannerTableChangeEventPublisher}. */
    public SpannerTableChangeEventPublisher build() throws IOException {
      Preconditions.checkState(publisher != null || topicName != null);
      return new SpannerTableChangeEventPublisher(this);
    }
  }

  /**
   * Creates a {@link Builder} for a {@link SpannerTableChangeEventPublisher}.
   *
   * @param watcher A {@link SpannerTableChangeWatcher} for the table that this publisher should
   *     publish the change events for. The {@link SpannerTableChangeWatcher} will be managed by
   *     this publisher, and should not be started or stopped manually. It is ok to add additional
   *     {@link RowChangeCallback} to the watcher.
   * @param client A {@link DatabaseClient} for the database that is being watched for changes. The
   *     publisher uses this client to query the database for information on the data types and
   *     primary key of the table that are being watched.
   */
  public static Builder newBuilder(SpannerTableChangeWatcher watcher, DatabaseClient client) {
    Preconditions.checkNotNull(watcher);
    Preconditions.checkNotNull(client);
    Preconditions.checkArgument(watcher.state() == State.NEW);
    return new Builder(watcher, client);
  }

  abstract static class PublishRowChangeCallback implements RowChangeCallback {
    private final ImmutableList<PublishListener> listeners;

    PublishRowChangeCallback(ImmutableList<PublishListener> listeners) {
      this.listeners = listeners;
    }

    abstract SpannerToAvro getConverter(TableId table);

    abstract Publisher getPublisher(TableId table);

    abstract void onFailure(TableId table, Timestamp commitTimestamp, Throwable t);

    @Override
    public void rowChange(final TableId table, Row row, final Timestamp commitTimestamp) {
      // Only use resources to convert the row to a string if we are actually going to log it.
      final String rowString = logger.isLoggable(Level.FINE) ? row.asStruct().toString() : null;
      logger.log(Level.FINE, "Publishing change to row {0}", rowString);
      ApiFuture<String> result =
          getPublisher(table)
              .publish(
                  PubsubMessage.newBuilder()
                      .setData(getConverter(table).makeRecord(row))
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
              for (PublishListener listener : listeners) {
                listener.onPublished(table, commitTimestamp, messageId);
              }
            }

            @Override
            public void onFailure(Throwable t) {
              logger.log(Level.WARNING, "Failed to publish change", t);
              logger.log(
                  Level.FINE, String.format("Failed to publish change to row {0}", rowString));
              PublishRowChangeCallback.this.onFailure(table, commitTimestamp, t);
              for (PublishListener listener : listeners) {
                listener.onFailure(table, commitTimestamp, t);
              }
            }
          },
          MoreExecutors.directExecutor());
    }
  }

  private final Builder builder;
  private final DatabaseClient client;
  private Publisher publisher;
  private final ImmutableList<PublishListener> listeners;
  private final ExecutorService startStopExecutor;
  private final boolean isOwnedExecutor;
  private final SpannerTableChangeWatcher watcher;

  private SpannerTableChangeEventPublisher(Builder builder) throws IOException {
    this.builder = builder;
    this.client = builder.client;
    this.startStopExecutor =
        builder.startStopExecutor == null
            ? Executors.newCachedThreadPool()
            : builder.startStopExecutor;
    this.isOwnedExecutor = builder.startStopExecutor == null;
    if (builder.publisher != null) {
      this.publisher = builder.publisher;
    }
    this.listeners = ImmutableList.copyOf(builder.listeners);
    this.watcher = builder.watcher;
  }

  @Override
  protected void doStart() {
    startStopExecutor.execute(
        new Runnable() {
          @Override
          public void run() {
            try {
              SpannerToAvro converter = new SpannerToAvro(client, watcher.getTable());
              if (publisher == null) {
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
                  publisherBuilder.setChannelProvider(
                      FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));
                  publisherBuilder.setCredentialsProvider(NoCredentialsProvider.create());
                }
                publisher = publisherBuilder.build();
              }
              watcher.addListener(
                  new Listener() {
                    @Override
                    public void running() {
                      notifyStarted();
                    }

                    @Override
                    public void failed(State from, Throwable failure) {
                      notifyFailed(failure);
                    }
                  },
                  MoreExecutors.directExecutor());
              watcher.addCallback(
                  new PublishRowChangeCallback(listeners) {
                    @Override
                    Publisher getPublisher(TableId table) {
                      return publisher;
                    }

                    @Override
                    SpannerToAvro getConverter(TableId table) {
                      return converter;
                    }

                    @Override
                    void onFailure(TableId table, Timestamp commitTimestamp, Throwable t) {
                      stopDependencies(false);
                      notifyFailed(t);
                    }
                  });
              watcher.startAsync();
            } catch (Throwable t) {
              notifyFailed(t);
            }
          }
        });
  }

  @Override
  public void doStop() {
    logger.log(Level.FINE, "Stopping event publisher");
    stopDependencies(true);
  }

  private void stopDependencies(boolean notify) {
    startStopExecutor.execute(
        new Runnable() {
          @Override
          public void run() {
            try {
              watcher.stopAsync().awaitTerminated();
              publisher.shutdown();
              if (notify) {
                notifyStopped();
              }
            } catch (Throwable t) {
              if (notify) {
                notifyFailed(t);
              }
            }
          }
        });
    if (isOwnedExecutor) {
      startStopExecutor.shutdown();
    }
  }
}
