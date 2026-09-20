package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.core.Message;
import com.arth.sakimq.protocol.WalRecord;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Recovery {

    private Recovery() {
    }

    public static void recover(MessageLog wal, Broker broker) {
        Map<String, Message> pending = new LinkedHashMap<>();
        Map<String, Integer> deliveryCounts = new HashMap<>();
        for (WalRecord record : wal.recover()) {
            switch (record.getType()) {
                case CREATE_QUEUE -> broker.restoreQueue(record.getQueue());
                case PUBLISH -> {
                    pending.put(record.getMessageId(), new Message(
                            record.getMessageId(),
                            record.getQueue(),
                            record.getBody().toByteArray(),
                            record.getCreatedAt()));
                    deliveryCounts.put(record.getMessageId(), record.getDeliveryCount());
                }
                case DELIVERY -> deliveryCounts.put(record.getMessageId(), record.getDeliveryCount());
                case ACK -> {
                    pending.remove(record.getMessageId());
                    deliveryCounts.remove(record.getMessageId());
                }
                default -> {
                }
            }
        }
        for (Map.Entry<String, Message> entry : pending.entrySet()) {
            broker.restore(entry.getValue(), deliveryCounts.getOrDefault(entry.getKey(), 0));
        }
    }
}
