package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.core.Message;
import com.arth.sakimq.protocol.WalRecord;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Recovery {

    private Recovery() {
    }

    public static void recover(MessageLog wal, Broker broker) {
        Map<String, Message> pending = new LinkedHashMap<>();
        for (WalRecord record : wal.recover()) {
            switch (record.getType()) {
                case PUBLISH -> pending.put(record.getMessageId(), new Message(
                        record.getMessageId(),
                        record.getQueue(),
                        record.getBody().toByteArray(),
                        record.getCreatedAt()));
                case ACK -> pending.remove(record.getMessageId());
                default -> {
                }
            }
        }
        for (Message message : pending.values()) {
            broker.restore(message);
        }
    }
}
