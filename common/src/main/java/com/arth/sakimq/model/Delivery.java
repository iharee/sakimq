package com.arth.sakimq.model;

public record Delivery(
        String messageId,
        String queue,
        byte[] body,
        long createdAt,
        String receiptHandle,
        int deliveryCount
) {
}
