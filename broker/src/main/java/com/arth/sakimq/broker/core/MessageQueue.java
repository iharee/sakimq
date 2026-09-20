package com.arth.sakimq.broker.core;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.arth.sakimq.model.Delivery;
import com.arth.sakimq.model.QueueStats;

public class MessageQueue {

    // 就绪消息的队列
    private final Deque<String> ready = new ArrayDeque<>();
    // 已投递但未 ack 消息的集合
    private final Map<String, InflightMessage> inflight = new HashMap<>();
    // 消息实体集合
    private final Map<String, Message> messages = new HashMap<>();
    // 每条消息已投递的次数
    private final Map<String, Integer> deliveryCounts = new HashMap<>();
    
    // 最大投递次数，超过后丢弃（死信队列 TODO）
    private final int maxDeliveryCount;

    public MessageQueue(int maxDeliveryCount) {
        this.maxDeliveryCount = maxDeliveryCount;
    }

    public synchronized String publish(Message message) {
        String id = message.messageId();
        messages.put(id, message);
        ready.add(id);
        notifyAll();
        return id;
    }

    /**
     * 
     * @param visibilityTimeout 消息被投递后在多少时间内对其他消费者不可见，避免竞争者重复消费
     * @param waitTimeout 等待超时时间，若队列为空，则在超过该时间后返回 Optional.empty()
     * @return 返回 Optional<Delivery> 或 Optional.empty()，在无有效消息时返回 Optional.empty()
     */
    public Optional<Delivery> consume(Duration visibilityTimeout, Duration waitTimeout) {
        if (visibilityTimeout == null || visibilityTimeout.isNegative() || visibilityTimeout.isZero())
            throw new IllegalArgumentException("invalid visibilityTimeout");
        if (waitTimeout == null || waitTimeout.isNegative())
            throw new IllegalArgumentException("invalid waitTimeout");

        long deadline = System.nanoTime() + waitTimeout.toNanos();

        synchronized (this) {
            while (true) {
                // 1. 每次消费前，尝试重试超时但未 ack 的消息
                requeueExpiredMessages();

                // 2. 尝试取一条可见消息
                String id = ready.poll();
                if (id != null) {
                    Message msg = messages.get(id);
                    int deliveryCount = deliveryCounts.merge(id, 1, Integer::sum);
                    if (deliveryCount > maxDeliveryCount) {
                        // TODO: 死信队列
                        messages.remove(id);
                        deliveryCounts.remove(id);
                        continue;
                    }
                    String receiptHandle = UUID.randomUUID().toString();  // receiptHandle 标识投递
                    long visibleAt = System.currentTimeMillis() + visibilityTimeout.toMillis();
                    inflight.put(receiptHandle, new InflightMessage(id, receiptHandle, visibleAt));
                    return Optional.of(new Delivery(
                            msg.messageId(),
                            msg.queue(),
                            msg.body(),
                            msg.createdAt(),
                            receiptHandle,
                            deliveryCount));
                }

                // 3. 空队列 case
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return Optional.empty();  // 已超时，返回超时

                // 等待时间不超过最早到期的 inflight 消息，这样 visibility 超时能及时唤醒本线程去复活
                long waitNanos = remaining;
                long earliestDeadline = Long.MAX_VALUE;
                for (InflightMessage im : inflight.values()) {
                    earliestDeadline = Math.min(earliestDeadline, im.deadline());
                }
                if (earliestDeadline != Long.MAX_VALUE) {
                    long untilEarliest = earliestDeadline - System.currentTimeMillis();
                    waitNanos = Math.min(waitNanos, Math.max(0, untilEarliest * 1_000_000L));
                }
                if (waitNanos <= 0) continue;  // 已有消息到期，回答循环开头处重新投递到期消息

                // 4. 释放锁，直至被 publish 唤醒、inflight 到期或超时
                try {
                    long millis = TimeUnit.NANOSECONDS.toMillis(waitNanos);
                    int nanos = (int) (waitNanos % 1_000_000);
                    this.wait(millis, nanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            }
        }
    }

    public synchronized boolean ack(String receiptHandle) {
        InflightMessage inflightMsg = inflight.remove(receiptHandle);
        if (inflightMsg == null) return false;
        messages.remove(inflightMsg.messageId());
        deliveryCounts.remove(inflightMsg.messageId());
        return true;
    }

    // TODO: 可考虑再使用一个额外的后台线程定期调用本方法
    public synchronized void requeueExpiredMessages() {
        long now = System.currentTimeMillis();
        boolean requeued = false;
        var it = inflight.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, InflightMessage> entry = it.next();
            if (entry.getValue().deadline() <= now) {
                it.remove();
                ready.add(entry.getValue().messageId());
                requeued = true;
            }
        }
        if (requeued) notifyAll();
    }

    public synchronized QueueStats stats(String queue) {
        return new QueueStats(queue, ready.size(), inflight.size(), messages.size());
    }
}
