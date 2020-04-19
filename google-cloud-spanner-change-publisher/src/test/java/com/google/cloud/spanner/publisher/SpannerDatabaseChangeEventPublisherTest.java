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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.publisher.MockPubSubServer.MockPublisherServiceImpl;
import com.google.cloud.spanner.publisher.MockPubSubServer.MockSubscriberServiceImpl;
import com.google.cloud.spanner.watcher.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class SpannerDatabaseChangeEventPublisherTest extends AbstractMockServerTest {
  private static MockPubSubServer mockPubSub;
  private static Server pubSubServer;
  private static InetSocketAddress pubSubAddress;
  private static MockPublisherServiceImpl publisherService;
  private static MockSubscriberServiceImpl subscriberService;

  @BeforeClass
  public static void setupPubSub() throws IOException {
    mockPubSub = new MockPubSubServer();
    publisherService = mockPubSub.new MockPublisherServiceImpl();
    subscriberService = mockPubSub.new MockSubscriberServiceImpl();
    pubSubAddress = new InetSocketAddress("localhost", 0);
    pubSubServer =
        NettyServerBuilder.forAddress(pubSubAddress)
            .addService(publisherService)
            .addService(subscriberService)
            .build()
            .start();
  }

  @AfterClass
  public static void teardownPubSub() throws InterruptedException {
    pubSubServer.shutdown();
    pubSubServer.awaitTermination();
  }

  @Before
  public void setup() {
    publisherService.reset();
    subscriberService.reset();
  }

  @Test
  public void testPublishChanges() throws Exception {
    final AtomicInteger receivedMessages = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);

    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("localhost:" + pubSubServer.getPort())
            .usePlaintext()
            .build();
    TransportChannelProvider channelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
    Subscriber subscriber =
        Subscriber.newBuilder(
                "projects/p/subscriptions/s",
                new MessageReceiver() {
                  @Override
                  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                    latch.countDown();
                    receivedMessages.incrementAndGet();
                    consumer.ack();
                  }
                })
            .setChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();
    subscriber.startAsync().awaitRunning();

    Spanner spanner = getSpanner();
    DatabaseId db = DatabaseId.of("p", "i", "d");
    SpannerDatabaseTailer tailer =
        SpannerDatabaseTailer.newBuilder(spanner, db)
            .allTables()
            .setPollInterval(Duration.ofMillis(10L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, db)
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            .build();
    SpannerDatabaseChangeEventPublisher publisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(tailer, spanner.getDatabaseClient(db))
            .usePlainText()
            .setEndpoint("localhost:" + pubSubServer.getPort())
            .setTopicNameFormat("projects/p/topics/%table%-updates")
            .build();
    publisher.startAsync().awaitRunning();
    latch.await(10L, TimeUnit.SECONDS);
    assertThat(receivedMessages.get()).isEqualTo(SELECT_FOO_ROW_COUNT + SELECT_BAR_ROW_COUNT);
    publisher.stopAsync().awaitTerminated();
    subscriber.stopAsync().awaitTerminated();
  }
}
