package com.arth.sakimq.broker.core;

import com.arth.sakimq.broker.config.MQConfig;
import com.arth.sakimq.broker.storage.MessageLog;
import com.arth.sakimq.exception.InvalidArgumentException;
import com.arth.sakimq.exception.QueueNotFoundException;
import com.arth.sakimq.model.Delivery;
import com.arth.sakimq.model.QueueStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class MonoBroker implements Broker {

    private static final Logger log = LoggerFactory.getLogger(MonoBroker.class);

    private final ConcurrentHashMap<String, MessageQueue> queues = new ConcurrentHashMap<>();
    private final int maxDeliveryCount;
    private final MessageLog wal;
    private final Lock createQueueLock = new ReentrantLock();

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

    private static void requireQueue(String queue) {
        if (queue == null || queue.isBlank()) {
            throw new InvalidArgumentException("queue must not be blank");
        }
    }

    @Override
    public boolean createQueue(String queue) {
        requireQueue(queue);
        createQueueLock.lock();
        try {
            if (queues.containsKey(queue)) {
                log.debug("Queue already exists: {}", queue);
                return false;
            }
            // 先写 wal，再入内存，保证空队列在重启后仍然存在
            if (wal != null) wal.appendCreateQueue(queue);
            queues.put(queue, new MessageQueue(maxDeliveryCount));
            log.info("Queue created: {}", queue);
            return true;
        } finally {
            createQueueLock.unlock();
        }
    }

    @Override
    public String publish(String queue, byte[] body) {
        requireQueue(queue);
        MessageQueue mq = queues.get(queue);
        if (mq == null) {
            log.debug("Publish rejected: queue not found: {}", queue);
            throw new QueueNotFoundException(queue);
        }
        String id = UUID.randomUUID().toString();
        Message message = Message.of(id, queue, body);
        // 先写 wal，再入内存
        if (wal != null) wal.appendPublish(message);
        mq.publish(message);
        log.debug("Published message: id={}, queue={}", id, queue);
        return id;
    }

    @Override
    public Optional<Delivery> consume(String queue, Duration visibilityTimeout, Duration waitTimeout) {
        requireQueue(queue);
        MessageQueue mq = queues.get(queue);
        if (mq == null) {
            log.debug("Consume rejected: queue not found: {}", queue);
            throw new QueueNotFoundException(queue);
        }
        Optional<Delivery> delivery = mq.consume(visibilityTimeout, waitTimeout);
        // 投递计数先落盘，避免重启后 deliveryCount 归零导致毒消息被无限重投
        if (wal != null) {
            delivery.ifPresent(d -> wal.appendDelivery(d.queue(), d.messageId(), d.deliveryCount()));
        }
        delivery.ifPresent(d -> log.debug("Delivered message: id={}, queue={}, deliveryCount={}",
                d.messageId(), d.queue(), d.deliveryCount()));
        return delivery;
    }

    @Override
    public boolean ack(String queue, String receiptHandle) {
        requireQueue(queue);
        MessageQueue mq = queues.get(queue);
        if (mq == null) {
            log.debug("Ack rejected: queue not found: {}", queue);
            throw new QueueNotFoundException(queue);
        }
        // 落盘与内存移除在同一临界区内完成，且 WAL 先行；写失败时消息仍 inflight，超时后重投
        Optional<String> messageId = mq.ack(receiptHandle, id -> {
            if (wal != null) wal.appendAck(queue, id);
        });
        if (messageId.isEmpty()) {
            log.debug("Ack rejected: unknown or expired receiptHandle: queue={}, receiptHandlePrefix={}",
                    queue, abbreviate(receiptHandle));
            return false;
        }
        log.debug("Ack recorded: queue={}, messageId={}", queue, messageId.get());
        return true;
    }

    /** receiptHandle 是 ack 的 capability token，日志中只保留前缀 */
    private static String abbreviate(String s) {
        return s == null ? "null" : s.length() <= 8 ? s : s.substring(0, 8) + "...";
    }

    @Override
    public QueueStats stats(String queue) {
        requireQueue(queue);
        MessageQueue mq = queues.get(queue);
        if (mq == null) return QueueStats.empty(queue);
        return mq.stats(queue);
    }

    @Override
    public void restore(Message message, int deliveryCount) {
        MessageQueue mq = queues.computeIfAbsent(message.queue(), k -> new MessageQueue(maxDeliveryCount));
        if (mq.restore(message, deliveryCount)) {
            log.debug("Restored message: id={}, queue={}, deliveryCount={}",
                    message.messageId(), message.queue(), deliveryCount);
        }
    }

    @Override
    public void restoreQueue(String queue) {
        queues.computeIfAbsent(queue, k -> new MessageQueue(maxDeliveryCount));
        log.debug("Restored queue: {}", queue);
    }
}
