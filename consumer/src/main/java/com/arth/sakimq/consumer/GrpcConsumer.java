package com.arth.sakimq.consumer;

import com.arth.sakimq.model.Delivery;
import com.arth.sakimq.protocol.AckRequest;
import com.arth.sakimq.protocol.ConsumeRequest;
import com.arth.sakimq.protocol.ConsumeResponse;
import com.arth.sakimq.protocol.MQServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class GrpcConsumer implements Consumer {

    private static final String DEFAULT_TARGET = "localhost:50051";

    private final ManagedChannel channel;
    private final MQServiceGrpc.MQServiceBlockingStub stub;
    private final Deduplicator deduplicator;

    public GrpcConsumer() {
        this(DEFAULT_TARGET, null);
    }

    public GrpcConsumer(String target) {
        this(target, null);
    }

    public GrpcConsumer(Deduplicator deduplicator) {
        this(DEFAULT_TARGET, deduplicator);
    }

    public GrpcConsumer(String target, Deduplicator deduplicator) {
        this(ManagedChannelBuilder.forTarget(target).usePlaintext().build(), deduplicator);
    }

    public GrpcConsumer(ManagedChannel channel, Deduplicator deduplicator) {
        this.channel = channel;
        this.stub = MQServiceGrpc.newBlockingStub(channel);
        this.deduplicator = deduplicator;
    }

    @Override
    public Optional<Delivery> consume(String queue, Duration visibilityTimeout, Duration waitTimeout) {
        checkQueue(queue);
        if (visibilityTimeout == null || visibilityTimeout.isNegative() || visibilityTimeout.isZero())
            throw new IllegalArgumentException("invalid visibilityTimeout");
        if (waitTimeout == null || waitTimeout.isNegative())
            throw new IllegalArgumentException("invalid waitTimeout");

        long deadline = System.nanoTime() + waitTimeout.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return Optional.empty();

            ConsumeResponse response = stub.consume(ConsumeRequest.newBuilder()
                    .setQueue(queue)
                    .setVisibilityTimeoutMs(Math.max(1, visibilityTimeout.toMillis()))
                    .setWaitTimeoutMs(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)))
                    .build());

            if (!response.hasDelivery()) return Optional.empty();

            Delivery delivery = toDelivery(response.getDelivery());
            if (deduplicator != null && deduplicator.isDuplicate(delivery)) {
                // 已处理过的重复消息：直接 ack，避免 broker 反复重投
                ack(queue, delivery.receiptHandle());
                continue;
            }
            if (deduplicator != null) {
                deduplicator.markProcessed(delivery);
            }
            return Optional.of(delivery);
        }
    }

    @Override
    public boolean ack(String queue, String receiptHandle) {
        checkQueue(queue);
        if (receiptHandle == null || receiptHandle.isBlank()) {
            throw new IllegalArgumentException("receiptHandle must not be blank");
        }
        return stub.ack(AckRequest.newBuilder()
                .setQueue(queue)
                .setReceiptHandle(receiptHandle)
                .build())
                .getAcknowledged();
    }

    @Override
    public void close() {
        channel.shutdown();
    }

    protected Deduplicator deduplicator() {
        return deduplicator;
    }

    private static Delivery toDelivery(com.arth.sakimq.protocol.Delivery delivery) {
        com.arth.sakimq.protocol.Message message = delivery.getMessage();
        return new Delivery(
                message.getMessageId(),
                message.getQueue(),
                message.getBody().toByteArray(),
                message.getCreatedAt(),
                delivery.getReceiptHandle(),
                delivery.getDeliveryCount());
    }

    private static void checkQueue(String queue) {
        if (queue == null || queue.isBlank()) {
            throw new IllegalArgumentException("queue must not be blank");
        }
    }
}
