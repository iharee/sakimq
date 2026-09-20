package com.arth.sakimq.broker.server;

import io.grpc.stub.StreamObserver;
import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.core.MonoBroker;
import com.arth.sakimq.protocol.AckRequest;
import com.arth.sakimq.protocol.AckResponse;
import com.arth.sakimq.protocol.ConsumeRequest;
import com.arth.sakimq.protocol.ConsumeResponse;
import com.arth.sakimq.protocol.CreateQueueRequest;
import com.arth.sakimq.protocol.CreateQueueResponse;
import com.arth.sakimq.protocol.GetQueueStatsRequest;
import com.arth.sakimq.protocol.GetQueueStatsResponse;
import com.arth.sakimq.protocol.MQServiceGrpc;
import com.arth.sakimq.protocol.PublishRequest;
import com.arth.sakimq.protocol.PublishResponse;
import com.arth.sakimq.model.QueueStats;
import com.google.protobuf.ByteString;

import java.time.Duration;
import java.util.Optional;

public class MQServiceImpl extends MQServiceGrpc.MQServiceImplBase {

    private final Broker broker;

    public MQServiceImpl() {
        this(new MonoBroker());
    }

    public MQServiceImpl(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void createQueue(CreateQueueRequest request,
                            StreamObserver<CreateQueueResponse> responseObserver) {
        boolean created = broker.createQueue(request.getQueue());
        responseObserver.onNext(CreateQueueResponse.newBuilder().setCreated(created).build());
        responseObserver.onCompleted();
    }

    @Override
    public void publish(PublishRequest request,
                        StreamObserver<PublishResponse> responseObserver) {
        String messageId = broker.publish(request.getQueue(), request.getBody().toByteArray());
        responseObserver.onNext(PublishResponse.newBuilder().setMessageId(messageId).build());
        responseObserver.onCompleted();
    }

    @Override
    public void consume(ConsumeRequest request,
                        StreamObserver<ConsumeResponse> responseObserver) {
        Duration visibilityTimeout = Duration.ofMillis(Math.max(0, request.getVisibilityTimeoutMs()));
        Duration waitTimeout = Duration.ofMillis(Math.max(0, request.getWaitTimeoutMs()));
        Optional<com.arth.sakimq.model.Delivery> delivery = broker.consume(
                request.getQueue(), visibilityTimeout, waitTimeout);

        ConsumeResponse.Builder response = ConsumeResponse.newBuilder();
        delivery.ifPresent(value -> response.setDelivery(com.arth.sakimq.protocol.Delivery.newBuilder()
                .setMessage(com.arth.sakimq.protocol.Message.newBuilder()
                        .setMessageId(value.messageId())
                        .setQueue(value.queue())
                        .setBody(value.body() == null ? ByteString.EMPTY : ByteString.copyFrom(value.body()))
                        .setCreatedAt(value.createdAt())
                        .build())
                .setReceiptHandle(value.receiptHandle())
                .setDeliveryCount(value.deliveryCount())
                .build()));
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void ack(AckRequest request,
                    StreamObserver<AckResponse> responseObserver) {
        boolean acknowledged = broker.ack(request.getQueue(), request.getReceiptHandle());
        responseObserver.onNext(AckResponse.newBuilder().setAcknowledged(acknowledged).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getQueueStats(GetQueueStatsRequest request,
                              StreamObserver<GetQueueStatsResponse> responseObserver) {
        QueueStats stats = broker.stats(request.getQueue());
        responseObserver.onNext(GetQueueStatsResponse.newBuilder()
                .setStats(com.arth.sakimq.protocol.QueueStats.newBuilder()
                        .setQueue(stats.queue())
                        .setReadyCount(stats.readyCount())
                        .setInflightCount(stats.inflightCount())
                        .setTotalCount(stats.totalCount())
                        .build())
                .build());
        responseObserver.onCompleted();
    }
}
