package com.arth.sakimq.broker.core;

/**
 * 队列中的一条消息及其投递状态。deliveryCount 在每次投递时递增；消息被 ack 或丢弃时，整个 entry 随之失效。
 */
final class QueueEntry {

    final Message message;
    int deliveryCount;

    QueueEntry(Message message) {
        this(message, 0);
    }

    QueueEntry(Message message, int deliveryCount) {
        this.message = message;
        this.deliveryCount = deliveryCount;
    }
}
