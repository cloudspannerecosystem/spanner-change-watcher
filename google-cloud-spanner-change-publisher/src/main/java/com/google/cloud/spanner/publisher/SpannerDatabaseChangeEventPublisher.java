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
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher.NoOpListener;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher.PublishRowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Publishes change events from a Spanner database to PubSub topics.
 *
 * <p>The changes to the tables are emitted from a {@link SpannerDatabaseChangeWatcher} and then
 * sent to Pub/Sub by this event publisher.
 */
public class SpannerDatabaseChangeEventPublisher extends AbstractApiService implements ApiService {
  private static final Logger logger =
      Logger.getLogger(SpannerDatabaseChangeEventPublisher.class.getName());

  public static class Builder {
    private final SpannerDatabaseChangeWatcher watcher;
    private final DatabaseClient client;
    private String topicNameFormat;
    private ExecutorService startStopExecutor;
    private Credentials credentials;
    private String endpoint = PublisherStubSettings.getDefaultEndpoint();
    private boolean usePlainText;

    private Builder(SpannerDatabaseChangeWatcher watcher, DatabaseClient client) {
      this.watcher = watcher;
      this.client = client;
    }

    /**
     * Sets the format of the names of the topics where the events should be published. The name
     * format should be in the form 'projects/<project-id>/topics/<topic-id>', where <topic-id> may
     * contain the string %table%, which will be replaced with the actual table name.
     */
    public Builder setTopicNameFormat(String topicNameFormat) {
      this.topicNameFormat = Preconditions.checkNotNull(topicNameFormat);
      return this;
    }

    public Builder setStartStopExecutor(ExecutorService executor) {
      this.startStopExecutor = executor;
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
     * Sets the credentials to use to publish to Pub/Sub. If no credentials are set, the credentials
     * returned by {@link GoogleCredentials#getApplicationDefault()} will be used.
     */
    public Builder setCredentials(Credentials credentials) {
      this.credentials = Preconditions.checkNotNull(credentials);
      return this;
    }

    public SpannerDatabaseChangeEventPublisher build() throws IOException {
      return new SpannerDatabaseChangeEventPublisher(this);
    }
  }

  public static Builder newBuilder(SpannerDatabaseChangeWatcher watcher, DatabaseClient client) {
    Preconditions.checkNotNull(watcher);
    Preconditions.checkNotNull(client);
    Preconditions.checkArgument(watcher.state() == State.NEW);
    return new Builder(watcher, client);
  }

  private final Builder builder;
  private final DatabaseClient client;
  private final SpannerDatabaseChangeWatcher watcher;
  private final ExecutorService startStopExecutor;
  private final boolean isOwnedExecutor;
  private Map<TableId, Publisher> publishers;
  private Map<TableId, SpannerToAvro> converters;

  private SpannerDatabaseChangeEventPublisher(Builder builder) throws IOException {
    this.builder = builder;
    this.client = builder.client;
    this.watcher = builder.watcher;
    this.startStopExecutor =
        builder.startStopExecutor == null
            ? Executors.newCachedThreadPool()
            : builder.startStopExecutor;
    this.isOwnedExecutor = builder.startStopExecutor == null;
  }

  @Override
  protected void doStart() {
    logger.log(Level.FINE, "Starting event publisher");
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
                Credentials credentials =
                    builder.credentials == null
                        ? GoogleCredentials.getApplicationDefault()
                        : builder.credentials;
                if (credentials == null) {
                  throw new IllegalArgumentException(
                      "There is no credentials set on the builder, and the environment has no default credentials set.");
                }
                Publisher.Builder publisherBuilder =
                    Publisher.newBuilder(
                            builder
                                .topicNameFormat
                                .replace(
                                    "%project%", table.getDatabaseId().getInstanceId().getProject())
                                .replace(
                                    "%instance%",
                                    table.getDatabaseId().getInstanceId().getInstance())
                                .replace("%database%", table.getDatabaseId().getDatabase())
                                .replace("%catalog%", table.getCatalog())
                                .replace("%schema%", table.getSchema())
                                .replace("%table%", table.getTable()))
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                        .setEndpoint(builder.endpoint);
                if (builder.usePlainText) {
                  ManagedChannel channel =
                      ManagedChannelBuilder.forTarget(builder.endpoint).usePlaintext().build();
                  publisherBuilder.setChannelProvider(
                      FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));
                  publisherBuilder.setCredentialsProvider(NoCredentialsProvider.create());
                }
                publishers.put(table, publisherBuilder.build());
              }
              watcher.addCallback(
                  new PublishRowChangeCallback(new NoOpListener()) {
                    @Override
                    Publisher getPublisher(TableId table) {
                      return publishers.get(table);
                    }

                    @Override
                    SpannerToAvro getConverter(TableId table) {
                      return converters.get(table);
                    }
                  });
              watcher.addListener(
                  new Listener() {
                    @Override
                    public void failed(State from, Throwable failure) {
                      notifyFailed(failure);
                    }

                    @Override
                    public void running() {
                      notifyStarted();
                    }
                  },
                  MoreExecutors.directExecutor());
              watcher.startAsync();
            } catch (Throwable t) {
              notifyFailed(t);
            }
          }
        });
  }

  @Override
  protected void doStop() {
    logger.log(Level.FINE, "Stopping event publisher");
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
              notifyStopped();
            } catch (Throwable t) {
              notifyFailed(t);
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
