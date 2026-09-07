package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.MessageRecord;
import com.arth.sakimq.protocol.WalRecord;

import java.util.List;

public interface MessageLog extends AutoCloseable {

    void appendPublish(MessageRecord message);

    void appendAck(String queue, String receiptHandle);

    List<WalRecord> recover();

    @Override
    void close();
}
