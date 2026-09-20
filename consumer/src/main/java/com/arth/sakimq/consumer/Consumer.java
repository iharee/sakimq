package com.arth.sakimq.consumer;

import com.arth.sakimq.model.Delivery;

import java.time.Duration;
import java.util.Optional;

/**
 * 消息消费者客户端，通过 gRPC 从 Broker 拉取并确认消息
 */
public interface Consumer extends AutoCloseable {

    Optional<Delivery> consume(
            String queue,
            Duration visibilityTimeout,
            Duration waitTimeout
    );

    boolean ack(String queue, String receiptHandle);

    @Override
    void close();
}
