package com.arth.sakimq.broker.core;

import com.arth.sakimq.broker.config.MQConfig;
import com.arth.sakimq.model.Delivery;
import com.arth.sakimq.model.QueueStats;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class MonoBroker implements Broker {

    private final ConcurrentHashMap<String, MessageQueue> queues = new ConcurrentHashMap<>();
    private final int maxDeliveryCount;

    public MonoBroker() {
        this(MQConfig.defaults());
    }

    public MonoBroker(MQConfig config) {
        this.maxDeliveryCount = config.maxDeliveryCount();
    }

    @Override
    public boolean createQueue(String queue) {
        return queues.putIfAbsent(queue, new MessageQueue(maxDeliveryCount)) == null;
    }

    @Override
    public String publish(String queue, byte[] body) {
        MessageQueue mq = queues.get(queue);
        if (mq == null) throw new IllegalArgumentException("Queue does not exist: " + queue);
        String id = UUID.randomUUID().toString();
        mq.publish(Message.of(id, queue, body));
        return id;
    }

    @Override
    public Optional<Delivery> consume(String queue, Duration visibilityTimeout, Duration waitTimeout) {
        MessageQueue mq = queues.get(queue);
        if (mq == null) throw new IllegalArgumentException("Queue does not exist: " + queue);
        return mq.consume(visibilityTimeout, waitTimeout);
    }

    @Override
    public boolean ack(String queue, String receiptHandle) {
        MessageQueue mq = queues.get(queue);
        if (mq == null) throw new IllegalArgumentException("Queue does not exist: " + queue);
        return mq.ack(receiptHandle);
    }

    @Override
    public QueueStats stats(String queue) {
        MessageQueue mq = queues.get(queue);
        if (mq == null) return QueueStats.empty(queue);
        return mq.stats(queue);
    }
}
