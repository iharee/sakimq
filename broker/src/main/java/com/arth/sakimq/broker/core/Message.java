package com.arth.sakimq.broker.core;

import java.util.UUID;

public record Message(
                String messageId,
                String queue,
                byte[] body,
                long createdAt) {

        public static Message of(String queue, byte[] body) {
                return new Message(
                                UUID.randomUUID().toString(),
                                queue,
                                body,
                                System.currentTimeMillis());
        }

        public static Message of(String messageId, String queue, byte[] body) {
                return new Message(messageId, queue, body, System.currentTimeMillis());
        }
}
