package com.arth.sakimq.broker.core;

/**
 * 一次投递的记录：引用队列中的消息 entry，携带本次投递的 receiptHandle 和到期时间（nanoTime）
 */
public record InflightMessage(QueueEntry entry, String receiptHandle, long deadlineNanos) {
}
