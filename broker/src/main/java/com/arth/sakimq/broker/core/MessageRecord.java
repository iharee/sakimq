package com.arth.sakimq.broker.core;

public record MessageRecord(
        String messageId,
        String queue,
        byte[] body,
        long createdAt
) {
}
