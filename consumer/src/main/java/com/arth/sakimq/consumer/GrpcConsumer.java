package com.arth.sakimq.consumer;

import com.arth.sakimq.model.Delivery;

import java.time.Duration;
import java.util.Optional;

public class GrpcConsumer implements Consumer {

    private final Deduplicator deduplicator;

    public GrpcConsumer() {
        this(null);
    }

    public GrpcConsumer(Deduplicator deduplicator) {
        this.deduplicator = deduplicator;
    }

    @Override
    public Optional<Delivery> consume(String queue, Duration visibilityTimeout, Duration waitTimeout) {
        return Optional.empty();
    }

    @Override
    public boolean ack(String queue, String receiptHandle) {
        return false;
    }

    protected Deduplicator deduplicator() {
        return deduplicator;
    }
}
