package com.arth.sakimq.broker.core;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.arth.sakimq.model.Delivery;
import com.arth.sakimq.model.QueueStats;

public class MessageQueue {

    // 就绪消息队列
    private final Deque<QueueEntry> ready = new ArrayDeque<>();
    // 已投递但未 ack 的投递记录
    private final Map<String, InflightMessage> inflight = new HashMap<>();
    // 按到期时间排序的投递记录的优先队列
    private final PriorityQueue<InflightMessage> expiry =
        new PriorityQueue<>(Comparator.comparingLong(InflightMessage::deadlineNanos));
    // 当前队列中所有消息的 messageId
    private final Set<String> messageIds = new HashSet<>();
    // 最大投递次数
    private final int maxDeliveryCount;

    public MessageQueue(int maxDeliveryCount) {
        this.maxDeliveryCount = maxDeliveryCount;
    }

    public synchronized String publish(Message message) {
        String id = message.messageId();
        if (!messageIds.add(id)) throw new RuntimeException("duplicate message id: " + id);
        ready.add(new QueueEntry(message));
        notifyAll();
        return id;
    }

    /**
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
                // 1. 每次消费前，尝试重试已超时而未 ack 的消息
                requeueExpiredMessages();

                // 2. 尝试取一条可见消息
                QueueEntry entry = ready.poll();
                if (entry != null) {
                    entry.deliveryCount++;
                    if (entry.deliveryCount > maxDeliveryCount) {
                        // TODO: 死信队列
                        messageIds.remove(entry.message.messageId());
                        continue;
                    }
                    String receiptHandle = UUID.randomUUID().toString();  // receiptHandle 标识本次投递
                    long deadlineNanos = System.nanoTime() + visibilityTimeout.toNanos();
                    InflightMessage im = new InflightMessage(entry, receiptHandle, deadlineNanos);
                    inflight.put(receiptHandle, im);
                    expiry.add(im);
                    return Optional.of(new Delivery(
                            entry.message.messageId(),
                            entry.message.queue(),
                            entry.message.body(),
                            entry.message.createdAt(),
                            receiptHandle,
                            entry.deliveryCount));
                }

                // 3. 空队列
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return Optional.empty();  // 已超时，返回超时

                // 准备等待消息
                long waitNanos = remaining;
                InflightMessage next = peekValidExpiryLocked();
                if (next != null) {
                    long untilEarliest = next.deadlineNanos() - System.nanoTime();
                    waitNanos = Math.min(waitNanos, Math.max(0, untilEarliest));
                }
                if (waitNanos <= 0) continue;

                // 4. 释放锁并等待消息，直至被 publish 唤醒、inflight 到期或超时
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

    public synchronized Optional<String> peekAck(String receiptHandle) {
        InflightMessage im = inflight.get(receiptHandle);
        if (im == null) return Optional.empty();
        return Optional.of(im.entry().message.messageId());
    }

    public synchronized boolean ack(String receiptHandle) {
        InflightMessage im = inflight.remove(receiptHandle);
        if (im == null) return false;
        messageIds.remove(im.entry().message.messageId());
        return true;
    }

    // TODO: 可考虑再使用一个额外的后台线程定期调用本方法
    public synchronized void requeueExpiredMessages() {
        long now = System.nanoTime();
        boolean requeued = false;
        while (true) {
            InflightMessage top = expiry.peek();
            if (top == null) break;

            // 堆顶是有效且未过期，没有需要被重新投递的信息
            if (top.deadlineNanos() > now && inflight.get(top.receiptHandle()) == top) break;
            expiry.poll();
            // 已被 ack 的残留记录，丢弃
            if (inflight.get(top.receiptHandle()) != top) continue;

            inflight.remove(top.receiptHandle());
            ready.add(top.entry());
            requeued = true;
        }
        if (requeued) notifyAll();
    }

    /**
     * 只能在持有锁时调用
     * 
     * @return 返回 expiry 堆顶的有效投递记录
     */
    private InflightMessage peekValidExpiryLocked() {
        while (true) {
            InflightMessage top = expiry.peek();
            if (top == null) return null;
            if (inflight.get(top.receiptHandle()) == top) return top;
            expiry.poll();
        }
    }

    public synchronized QueueStats stats(String queue) {
        return new QueueStats(queue, ready.size(), inflight.size(), messageIds.size());
    }
}
