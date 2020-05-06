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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

class PubsubHelper implements AutoCloseable {
  private final TopicAdminClient client;
  private final GrpcTransportChannel channel;

  PubsubHelper(Credentials credentials, String endpoint, boolean usePlainText) throws IOException {
    if (usePlainText) {
      channel =
          GrpcTransportChannel.create(
              ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build());
      client =
          TopicAdminClient.create(
              TopicAdminSettings.newBuilder()
                  .setCredentialsProvider(NoCredentialsProvider.create())
                  .setTransportChannelProvider(FixedTransportChannelProvider.create(channel))
                  .build());
    } else {
      channel = null;
      client =
          TopicAdminClient.create(
              TopicAdminSettings.newBuilder()
                  .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                  .setEndpoint(endpoint)
                  .build());
    }
  }

  @Override
  public void close() throws InterruptedException {
    client.shutdown();
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(15L, TimeUnit.SECONDS);
    }
    client.awaitTermination(15L, TimeUnit.SECONDS);
  }

  void checkExists(String topicName) throws Exception {
    try {
      client.getTopic(topicName);
    } catch (ApiException e) {
      // Ignore PERMISSION_DENIED as it could be that the user only has permission to publish
      // messages, but not see topics.
      if (e.getStatusCode().getCode() != Code.PERMISSION_DENIED) {
        throw e;
      }
    }
  }
}
