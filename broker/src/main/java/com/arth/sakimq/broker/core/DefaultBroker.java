package com.arth.sakimq.broker.core;

import com.arth.sakimq.model.Delivery;
import com.arth.sakimq.model.QueueStats;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultBroker implements Broker {

    private final ConcurrentHashMap<String, MessageQueue> queues = new ConcurrentHashMap<>();

    @Override
    public void createQueue(String queue) {
    }

    @Override
    public String publish(String queue, byte[] body) {
        return "";
    }

    @Override
    public Optional<Delivery> consume(String queue, Duration visibilityTimeout, Duration waitTimeout) {
        return Optional.empty();
    }

    @Override
    public boolean ack(String queue, String receiptHandle) {
        return false;
    }

    @Override
    public QueueStats stats(String queue) {
        return QueueStats.empty(queue);
    }
}
