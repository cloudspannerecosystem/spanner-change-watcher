/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.publisher;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.PushConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.logging.Logger;

/** Util class for automatically generating test topics on a Pubsub Emulator instance. */
class PubsubEmulatorUtil {

  static void maybeCreateSubscriptions(String project, String topic, String subscription) {
    try {
      ManagedChannel channel =
          ManagedChannelBuilder.forTarget(Configuration.getPubsubEmulatorEndpoint())
              .usePlaintext()
              .build();
      TransportChannelProvider channelProvider =
          FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
      SubscriptionAdminClient client =
          SubscriptionAdminClient.create(
              SubscriptionAdminSettings.newBuilder()
                  .setTransportChannelProvider(channelProvider)
                  .setCredentialsProvider(NoCredentialsProvider.create())
                  .build());
      client.createSubscription(
          String.format("projects/%s/subscriptions/%s", project, subscription),
          String.format("projects/%s/topics/%s", project, topic),
          PushConfig.getDefaultInstance(),
          10);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (AlreadyExistsException e) {
      // Ignore
    }
  }

  static Subscriber createDemoSubscriber(Configuration config, String table, Logger logger) {
    ManagedChannel channel =
        ManagedChannelBuilder.forTarget(Configuration.getPubsubEmulatorEndpoint())
            .usePlaintext()
            .build();
    TransportChannelProvider channelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    return Subscriber.newBuilder(
            String.format(
                "projects/%s/subscriptions/spanner-update-%s-%s",
                config.getPubsubProject(), config.getDatabaseId().getDatabase(), table),
            (MessageReceiver)
                (message, consumer) -> {
                  logger.info("Received message: " + message.toString());
                  consumer.ack();
                })
        .setCredentialsProvider(NoCredentialsProvider.create())
        .setChannelProvider(channelProvider)
        .build();
  }
}
