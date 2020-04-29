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
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher.PublishListener;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher.PublishRowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerUtils.LogRecordBuilder;
import com.google.cloud.spanner.watcher.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Publishes change events from a Spanner database to one or more Pubsub topics.
 *
 * <p>The data changes to the tables are emitted from a {@link SpannerDatabaseChangeWatcher} and
 * then sent to Pubsub by this event publisher. The publisher can be configured to publish change
 * events from each table to a separate topic, or to publish all changes to a single topic. Each
 * change event contains the {@link TableId}, the new row data and the commit timestamp.
 */
public class SpannerDatabaseChangeEventPublisher extends AbstractApiService implements ApiService {
  private static final Logger logger =
      Logger.getLogger(SpannerDatabaseChangeEventPublisher.class.getName());

  public static class Builder {
    private final SpannerDatabaseChangeWatcher watcher;
    private final DatabaseClient client;
    private String topicNameFormat;
    private ExecutorService startStopExecutor;
    private List<PublishListener> listeners = new ArrayList<>();
    private Credentials credentials;
    private String endpoint = PublisherStubSettings.getDefaultEndpoint();
    private boolean usePlainText;

    private Builder(SpannerDatabaseChangeWatcher watcher, DatabaseClient client) {
      this.watcher = watcher;
      this.client = client;
    }

    /**
     * Adds a {@link PublishListener} for the {@link SpannerDatabaseChangeEventPublisher}. This
     * listener will receive a callback for each message that is published to Pubsub. This can be
     * used for monitoring and logging purposes, but is not required for publishing changes to
     * Pubsub.
     */
    public Builder addListener(PublishListener publishListener) {
      this.listeners.add(Preconditions.checkNotNull(publishListener));
      return this;
    }

    /**
     * Sets the format of the names of the topics where the events should be published. The name
     * format should be in the form 'projects/<project-id>/topics/<topic-id>', where <topic-id> may
     * contain a number of place holders for the name of the database and table that caused the
     * event. These place holders will be replaced by the actual database name and/or table name.
     *
     * <p>Possible place holders are:
     *
     * <ul>
     *   <li>%project%}: The project id of the Cloud Spanner database.
     *   <li>%instance%: The instance id of the Cloud Spanner database.
     *   <li>%database%: The database id of the Cloud Spanner database.
     *   <li>%catalog%: The catalog name of the Cloud Spanner table.
     *   <li>%schema%: The schema name of the Cloud Spanner table.
     *   <li>%table%: The table name of the Cloud Spanner table.
     * </ul>
     */
    public Builder setTopicNameFormat(String topicNameFormat) {
      this.topicNameFormat = Preconditions.checkNotNull(topicNameFormat);
      return this;
    }

    /**
     * The {@link SpannerDatabaseChangeEventPublisher} needs to perform a number of administrative
     * tasks during startup and shutdown, and will use this executor for those tasks. The default
     * will use a cached thread pool executor.
     */
    public Builder setStartStopExecutor(ExecutorService executor) {
      this.startStopExecutor = executor;
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
     * Sets the credentials to use to publish to Pub/Sub. If no credentials are set, the credentials
     * returned by {@link GoogleCredentials#getApplicationDefault()} will be used.
     */
    public Builder setCredentials(Credentials credentials) {
      this.credentials = Preconditions.checkNotNull(credentials);
      return this;
    }

    /** Creates the {@link SpannerDatabaseChangeEventPublisher}. */
    public SpannerDatabaseChangeEventPublisher build() throws IOException {
      return new SpannerDatabaseChangeEventPublisher(this);
    }
  }

  /**
   * Creates a {@link Builder} for a {@link SpannerDatabaseChangeEventPublisher}.
   *
   * @param watcher A {@link SpannerDatabaseChangeWatcher} for the tables that this publisher should
   *     publish the change events for. The {@link SpannerDatabaseChangeWatcher} will be managed by
   *     this publisher, and should not be started or stopped manually. It is ok to add additional
   *     {@link RowChangeCallback} to the watcher.
   * @param client A {@link DatabaseClient} for the database that is being watched for changes. The
   *     publisher uses this client to query the database for information on the data types and
   *     primary key of the tables that are being watched.
   */
  public static Builder newBuilder(SpannerDatabaseChangeWatcher watcher, DatabaseClient client) {
    Preconditions.checkNotNull(watcher);
    Preconditions.checkNotNull(client);
    Preconditions.checkArgument(watcher.state() == State.NEW);
    return new Builder(watcher, client);
  }

  private final DatabaseClient client;
  private final SpannerDatabaseChangeWatcher watcher;
  private final String topicNameFormat;
  private final ExecutorService startStopExecutor;
  private final ImmutableList<PublishListener> listeners;
  private final boolean isOwnedExecutor;
  private final Credentials credentials;
  private final String endpoint;
  private final boolean usePlainText;
  private Map<TableId, Publisher> publishers;
  private Map<TableId, SpannerToAvro> converters;

  private SpannerDatabaseChangeEventPublisher(Builder builder) throws IOException {
    this.client = builder.client;
    this.watcher = builder.watcher;
    this.topicNameFormat = builder.topicNameFormat;
    this.startStopExecutor =
        builder.startStopExecutor == null
            ? Executors.newCachedThreadPool()
            : builder.startStopExecutor;
    this.listeners = ImmutableList.copyOf(builder.listeners);
    this.isOwnedExecutor = builder.startStopExecutor == null;
    this.credentials = builder.credentials;
    this.endpoint = builder.endpoint;
    this.usePlainText = builder.usePlainText;
    this.addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            logger.log(
                LogRecordBuilder.of(
                    Level.WARNING,
                    "Publisher for database {0} failed",
                    watcher.getDatabaseId(),
                    failure));
            stopDependencies(false);
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  protected void doStart() {
    logger.log(Level.INFO, "Starting event publisher for database {0}", watcher.getDatabaseId());
    startStopExecutor.execute(
        new Runnable() {
          @Override
          public void run() {
            try {
              publishers = new HashMap<>(watcher.getTables().size());
              converters = new HashMap<>(watcher.getTables().size());
              for (TableId table : watcher.getTables()) {
                // TODO: Re-use code from SpannerTableChangeEventPublisher.
                converters.put(table, new SpannerToAvro(client, table));
                Credentials useCredentials =
                    credentials == null ? GoogleCredentials.getApplicationDefault() : credentials;
                if (useCredentials == null) {
                  throw new IllegalArgumentException(
                      "There is no credentials set on the builder, and the environment has no default credentials set.");
                }
                Publisher.Builder publisherBuilder =
                    Publisher.newBuilder(
                            topicNameFormat
                                .replace(
                                    "%project%", table.getDatabaseId().getInstanceId().getProject())
                                .replace(
                                    "%instance%",
                                    table.getDatabaseId().getInstanceId().getInstance())
                                .replace("%database%", table.getDatabaseId().getDatabase())
                                .replace("%catalog%", table.getCatalog())
                                .replace("%schema%", table.getSchema())
                                .replace("%table%", table.getTable()))
                        .setCredentialsProvider(FixedCredentialsProvider.create(useCredentials))
                        .setEndpoint(endpoint);
                if (usePlainText) {
                  ManagedChannel channel =
                      ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
                  publisherBuilder.setChannelProvider(
                      FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));
                  publisherBuilder.setCredentialsProvider(NoCredentialsProvider.create());
                }
                publishers.put(table, publisherBuilder.build());
              }
              watcher.addListener(
                  new Listener() {
                    @Override
                    public void failed(State from, Throwable failure) {
                      logger.log(
                          LogRecordBuilder.of(
                              Level.WARNING,
                              "Watcher for database {0} failed to start",
                              watcher.getDatabaseId(),
                              failure));
                      notifyFailed(failure);
                    }

                    @Override
                    public void running() {
                      logger.log(
                          Level.INFO,
                          "Watcher for database {0} started successfully",
                          watcher.getDatabaseId());
                      notifyStarted();
                    }
                  },
                  MoreExecutors.directExecutor());
              watcher.addCallback(
                  new PublishRowChangeCallback(listeners) {
                    @Override
                    Publisher getPublisher(TableId table) {
                      return publishers.get(table);
                    }

                    @Override
                    SpannerToAvro getConverter(TableId table) {
                      return converters.get(table);
                    }

                    @Override
                    void onFailure(TableId table, Timestamp commitTimestamp, Throwable t) {
                      logger.log(
                          LogRecordBuilder.of(
                              Level.WARNING,
                              "Publish failed, stopping the watcher for database {0} and failing the publisher",
                              watcher.getDatabaseId(),
                              t));
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
  protected void doStop() {
    logger.log(Level.INFO, "Stopping event publisher for database {0}", watcher.getDatabaseId());
    stopDependencies(true);
  }

  private void stopDependencies(boolean notify) {
    watcher.addListener(
        new Listener() {
          @Override
          public void terminated(State from) {
            try {
              for (Publisher publisher : publishers.values()) {
                publisher.shutdown();
              }
              for (Publisher publisher : publishers.values()) {
                try {
                  publisher.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
              if (notify) {
                notifyStopped();
              }
            } catch (Throwable t) {
              if (notify) {
                notifyFailed(t);
              }
            } finally {
              if (isOwnedExecutor) {
                startStopExecutor.shutdown();
              }
            }
          }
        },
        startStopExecutor);
    watcher.stopAsync();
  }
}
