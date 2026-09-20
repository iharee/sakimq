package com.arth.sakimq.broker.core;

import com.arth.sakimq.broker.config.MQConfig;
import com.arth.sakimq.broker.storage.MessageLog;
import com.arth.sakimq.model.Delivery;
import com.arth.sakimq.model.QueueStats;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class MonoBroker implements Broker {

    private final ConcurrentHashMap<String, MessageQueue> queues = new ConcurrentHashMap<>();
    private final int maxDeliveryCount;
    private final MessageLog wal;

    public MonoBroker() {
        this(MQConfig.defaults(), null);
    }

    public MonoBroker(MQConfig config) {
        this(config, null);
    }

    public MonoBroker(MQConfig config, MessageLog wal) {
        this.maxDeliveryCount = config.maxDeliveryCount();
        this.wal = wal;
    }

    @Override
    public boolean createQueue(String queue) {
        if (queues.containsKey(queue)) return false;
        // 先写 wal，再入内存，保证空队列在重启后仍然存在
        if (wal != null) wal.appendCreateQueue(queue);
        return queues.putIfAbsent(queue, new MessageQueue(maxDeliveryCount)) == null;
    }

    @Override
    public String publish(String queue, byte[] body) {
        MessageQueue mq = queues.get(queue);
        if (mq == null) throw new IllegalArgumentException("Queue does not exist: " + queue);
        String id = UUID.randomUUID().toString();
        Message message = Message.of(id, queue, body);
        // 先写 wal，再入内存
        if (wal != null) wal.appendPublish(message);
        mq.publish(message);
        return id;
    }

    @Override
    public Optional<Delivery> consume(String queue, Duration visibilityTimeout, Duration waitTimeout) {
        MessageQueue mq = queues.get(queue);
        if (mq == null) throw new IllegalArgumentException("Queue does not exist: " + queue);
        Optional<Delivery> delivery = mq.consume(visibilityTimeout, waitTimeout);
        // 投递计数先落盘，避免重启后 deliveryCount 归零导致毒消息被无限重投
        if (wal != null) {
            delivery.ifPresent(d -> wal.appendDelivery(d.queue(), d.messageId(), d.deliveryCount()));
        }
        return delivery;
    }

    @Override
    public boolean ack(String queue, String receiptHandle) {
        MessageQueue mq = queues.get(queue);
        if (mq == null) throw new IllegalArgumentException("Queue does not exist: " + queue);
        Optional<String> messageId = mq.ack(receiptHandle);
        if (messageId.isEmpty()) return false;
        if (wal != null) wal.appendAck(queue, messageId.get());  // 最后再写 wal
        return true;
    }

    @Override
    public QueueStats stats(String queue) {
        MessageQueue mq = queues.get(queue);
        if (mq == null) return QueueStats.empty(queue);
        return mq.stats(queue);
    }

    @Override
    public void restore(Message message, int deliveryCount) {
        MessageQueue mq = queues.computeIfAbsent(message.queue(), k -> new MessageQueue(maxDeliveryCount));
        mq.restore(message, deliveryCount);
    }

    @Override
    public void restoreQueue(String queue) {
        queues.computeIfAbsent(queue, k -> new MessageQueue(maxDeliveryCount));
    }
}
