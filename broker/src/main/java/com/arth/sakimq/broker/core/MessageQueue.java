package com.arth.sakimq.broker.core;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import com.arth.sakimq.model.Delivery;
import com.arth.sakimq.model.QueueStats;

public class MessageQueue {

    private final Deque<String> ready = new ArrayDeque<>();
    private final Map<String, InflightMessage> inflight = new HashMap<>();
    private final Map<String, MessageRecord> messages = new HashMap<>();

    public String publish(MessageRecord message) {
        return "";
    }

    public Optional<Delivery> consume(Duration visibilityTimeout, Duration waitTimeout) {
        return Optional.empty();
    }

    public boolean ack(String receiptHandle) {
        return false;
    }

    public void requeueExpiredMessages() {
    }

    public QueueStats stats(String queue) {
        return QueueStats.empty(queue);
    }
}
