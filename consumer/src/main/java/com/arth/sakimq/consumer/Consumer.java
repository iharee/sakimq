package com.arth.sakimq.consumer;

import com.arth.sakimq.model.Delivery;

import java.time.Duration;
import java.util.Optional;


public interface Consumer extends AutoCloseable {

    Optional<Delivery> consume(
            String queue,
            Duration visibilityTimeout,
            Duration waitTimeout
    );

    boolean ack(Delivery delivery);

    @Override
    void close();
}
