package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.Message;
import com.arth.sakimq.protocol.WalRecord;

import java.util.List;

public interface MessageLog extends AutoCloseable {

    void appendPublish(Message message);

    void appendAck(String queue, String messageId);

    void appendDelivery(String queue, String messageId, int deliveryCount);

    void appendCreateQueue(String queue);

    List<WalRecord> recover();

    @Override
    void close();
}
