package com.arth.sakimq.producer;

import com.arth.sakimq.protocol.CreateQueueRequest;
import com.arth.sakimq.protocol.CreateQueueResponse;
import com.arth.sakimq.protocol.MQServiceGrpc;
import com.arth.sakimq.protocol.PublishRequest;
import com.arth.sakimq.protocol.PublishResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public final class GrpcProducer implements Producer {

    private static final String DEFAULT_TARGET = "localhost:50051";

    private final ManagedChannel channel;
    private final MQServiceGrpc.MQServiceBlockingStub stub;

    public GrpcProducer() {
        this(DEFAULT_TARGET);
    }

    public GrpcProducer(String target) {
        this(ManagedChannelBuilder.forTarget(target).usePlaintext().build());
    }

    public GrpcProducer(ManagedChannel channel) {
        this.channel = channel;
        this.stub = MQServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public boolean createQueue(String queue) {
        checkQueue(queue);
        CreateQueueResponse response = stub.createQueue(CreateQueueRequest.newBuilder().setQueue(queue).build());
        return response.getCreated();
    }

    @Override
    public String publish(String queue, byte[] body) {
        checkQueue(queue);
        if (body == null) throw new IllegalArgumentException("body must not be null");

        PublishResponse response = stub.publish(PublishRequest.newBuilder()
                .setQueue(queue)
                .setBody(ByteString.copyFrom(body))
                .build());
        return response.getMessageId();
    }

    @Override
    public void close() {
        channel.shutdown();
    }

    private static void checkQueue(String queue) {
        if (queue == null || queue.isBlank()) {
            throw new IllegalArgumentException("queue must not be blank");
        }
    }
}
