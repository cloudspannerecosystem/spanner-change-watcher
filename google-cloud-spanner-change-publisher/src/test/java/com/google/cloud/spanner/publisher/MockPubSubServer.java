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

import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc.PublisherImplBase;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberImplBase;
import com.google.pubsub.v1.Topic;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;

public class MockPubSubServer {
  private Deque<PubsubMessage> messages = new ConcurrentLinkedDeque<>();

  public class MockPublisherServiceImpl extends PublisherImplBase implements MockGrpcService {
    @Override
    public void publish(PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
      PublishResponse.Builder builder = PublishResponse.newBuilder();
      for (PubsubMessage message : request.getMessagesList()) {
        messages.add(message);
        builder.addMessageIds(UUID.randomUUID().toString());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getTopic(GetTopicRequest request, StreamObserver<Topic> responseObserver) {
      responseObserver.onNext(Topic.newBuilder().setName(request.getTopic()).build());
      responseObserver.onCompleted();
    }

    @Override
    public List<AbstractMessage> getRequests() {
      return null;
    }

    @Override
    public void addResponse(AbstractMessage response) {}

    @Override
    public void addException(Exception exception) {}

    @Override
    public ServerServiceDefinition getServiceDefinition() {
      return bindService();
    }

    @Override
    public void reset() {
      messages.clear();
    }
  }

  public class MockSubscriberServiceImpl extends SubscriberImplBase implements MockGrpcService {
    @Override
    public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
      PullResponse.Builder builder = PullResponse.newBuilder();
      while (builder.getReceivedMessagesCount() < request.getMaxMessages()) {
        PubsubMessage msg = messages.poll();
        if (msg == null) {
          break;
        }
        builder.addReceivedMessages(
            ReceivedMessage.newBuilder()
                .setAckId(UUID.randomUUID().toString())
                .setDeliveryAttempt(1)
                .setMessage(msg)
                .build());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }

    @Override
    public void modifyAckDeadline(
        ModifyAckDeadlineRequest request, StreamObserver<Empty> responseObserver) {
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void acknowledge(AcknowledgeRequest request, StreamObserver<Empty> responseObserver) {
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<StreamingPullRequest> streamingPull(
        final StreamObserver<StreamingPullResponse> responseObserver) {
      return new StreamObserver<StreamingPullRequest>() {
        @Override
        public void onNext(StreamingPullRequest request) {
          PubsubMessage msg;
          while ((msg = messages.poll()) != null) {
            responseObserver.onNext(
                StreamingPullResponse.newBuilder()
                    .addReceivedMessages(
                        ReceivedMessage.newBuilder()
                            .setAckId(UUID.randomUUID().toString())
                            .setDeliveryAttempt(1)
                            .setMessage(msg)
                            .build())
                    .build());
          }
          responseObserver.onCompleted();
        }

        @Override
        public void onError(Throwable t) {
          responseObserver.onError(t);
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }

    @Override
    public List<AbstractMessage> getRequests() {
      return null;
    }

    @Override
    public void addResponse(AbstractMessage response) {}

    @Override
    public void addException(Exception exception) {}

    @Override
    public ServerServiceDefinition getServiceDefinition() {
      return bindService();
    }

    @Override
    public void reset() {}
  }
}
